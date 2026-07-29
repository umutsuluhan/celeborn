package rdma_comms;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import io.netty.buffer.ByteBuf;
import org.apache.celeborn.common.network.buffer.ManagedBuffer;
import org.apache.celeborn.common.network.buffer.NettyManagedBuffer;
import org.apache.celeborn.common.network.client.ChunkReceivedCallback;
import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommsWrapper implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(CommsWrapper.class);

  static {
    try {
      java.io.File tempDir = createTempDir();
      java.io.File coreLib = extractResource(tempDir, "/natives/libml_transport.so");
      java.io.File jniLib = extractResource(tempDir, "/natives/libml_transport_jni.so");
      System.load(coreLib.getAbsolutePath());
      System.load(jniLib.getAbsolutePath());
    } catch (Exception ex) {
      throw new RuntimeException("Failed to load native libraries from JAR resources", ex);
    }
  }

  public static volatile boolean RDMA_TRACKER_ENABLED = true;
  private static final Map<Long, CommsWrapper> activeWrappers = new ConcurrentHashMap<>();

  private final long nativePtr;
  private ByteBuffer localBuffer;
  private Map<String, ByteBuffer> serverClientBuffers = new ConcurrentHashMap<>();
  public ServerJniHandler serverHandler;

  public CommsWrapper() {
    this.nativePtr = nativeCreate();
    activeWrappers.put(this.nativePtr, this);
  }

  public synchronized void init(Map<String, String> params) {
    if (params.containsKey("AP_RDMA_TRACKER_ENABLED")) {
      RDMA_TRACKER_ENABLED = Boolean.parseBoolean(params.get("AP_RDMA_TRACKER_ENABLED"));
    }
    nativeInit(nativePtr, params);
  }

  public synchronized byte[] getEndpointInfo() {
    return nativeGetEndpointInfo(nativePtr);
  }

  private final Map<String, Integer> peerIdMap = new ConcurrentHashMap<>();

  public int getPeerId(String peerName) {
    Integer id = peerIdMap.get(peerName);
    if (id != null) return id;
    id = nativeGetPeerId(nativePtr, peerName);
    if (id >= 0) {
      peerIdMap.put(peerName, id);
      return id;
    }
    throw new IllegalArgumentException("Unknown peerName: " + peerName + ". Ensure addRemoteEndpoint was called first.");
  }

  public int addRemoteEndpoint(String peerName, byte[] opaqueHandleBytes, boolean block) {
    int peerId = nativeAddRemoteEndpoint(nativePtr, peerName, opaqueHandleBytes, block);
    peerIdMap.put(peerName, peerId);
    return peerId;
  }

  public void connect(int peerId) {
    nativeConnect(nativePtr, peerId);
  }

  public void connect(String peerName) {
    connect(getPeerId(peerName));
  }

  public NativeBuffer allocateAndRegMem(long size, MemoryType memType) {
    long[] tokenPtrOut = new long[1];
    ByteBuffer buf = nativeAllocateAndRegMem(nativePtr, size, memType.ordinal(), tokenPtrOut);
    return new NativeBuffer(buf, new MemToken(tokenPtrOut[0]));
  }
  
  public synchronized void deregAndFreeMem(ByteBuffer buffer, MemToken token) {
    nativeDeregAndFreeMem(nativePtr, buffer, token.getNativePtr());
    token.close();
  }

  public synchronized MemToken getMemToken(byte[] serTok) {
    long tokenPtr = nativeGetMemToken(nativePtr, serTok);
    return new MemToken(tokenPtr);
  }

  // --- Client API ---
  public void initClientPool(int pushSlots, int pushSlotSize, int fetchSlots, int fetchSlotSize, ByteBuffer localBuffer, MemToken localToken, MemToken remoteToken, int serverPeerId) {
    this.localBuffer = localBuffer;
    nativeInitClientPool(nativePtr, serverPeerId, pushSlots, pushSlotSize, fetchSlots, fetchSlotSize, localToken.getNativePtr(), remoteToken.getNativePtr());
  }

  public void initClientPool(int pushSlots, int pushSlotSize, int fetchSlots, int fetchSlotSize, ByteBuffer localBuffer, MemToken localToken, MemToken remoteToken, String serverPeerName) {
    initClientPool(pushSlots, pushSlotSize, fetchSlots, fetchSlotSize, localBuffer, localToken, remoteToken, getPeerId(serverPeerName));
  }

  public void fetchChunk(int peerId, long streamId, int chunkIndex, ChunkReceivedCallback callback) {
    nativeFetchChunk(nativePtr, peerId, streamId, chunkIndex, callback);
  }

  public void fetchChunk(String remotePeer, long streamId, int chunkIndex, ChunkReceivedCallback callback) {
    fetchChunk(getPeerId(remotePeer), streamId, chunkIndex, callback);
  }

  public void fetchChunksBatched(int peerId, long[] streamIds, int[] chunkIndices, ChunkReceivedCallback[] callbacks) {
    nativeFetchChunksBatched(nativePtr, peerId, streamIds, chunkIndices, callbacks);
  }

  public void fetchChunksBatched(String remotePeer, long[] streamIds, int[] chunkIndices, ChunkReceivedCallback[] callbacks) {
    fetchChunksBatched(getPeerId(remotePeer), streamIds, chunkIndices, callbacks);
  }

  private byte[] encodePushDataPayload(String shuffleKey, String partitionUniqueId) {
    byte[] keyBytes = shuffleKey.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    byte[] pidBytes = partitionUniqueId.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    ByteBuffer buf = ByteBuffer.allocate(8 + keyBytes.length + pidBytes.length);
    buf.putInt(keyBytes.length);
    buf.put(keyBytes);
    buf.putInt(pidBytes.length);
    buf.put(pidBytes);
    return buf.array();
  }

  private byte[] encodePushMergedPayload(String shuffleKey, String[] partitionUniqueIds, int[] offsets) {
    byte[] keyBytes = shuffleKey.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    byte[][] pidBytes = new byte[partitionUniqueIds.length][];
    int len = 8 + keyBytes.length + 8;
    for (int i = 0; i < partitionUniqueIds.length; i++) {
        pidBytes[i] = partitionUniqueIds[i].getBytes(java.nio.charset.StandardCharsets.UTF_8);
        len += 4 + pidBytes[i].length + 4;
    }
    ByteBuffer buf = ByteBuffer.allocate(len);
    buf.putInt(keyBytes.length);
    buf.put(keyBytes);
    buf.putInt(partitionUniqueIds.length);
    for (byte[] pBytes : pidBytes) {
        buf.putInt(pBytes.length);
        buf.put(pBytes);
    }
    buf.putInt(offsets.length);
    for (int offset : offsets) buf.putInt(offset);
    return buf.array();
  }

  public int acquirePushSlot() {
    return nativeAcquirePushSlot(nativePtr);
  }

  public void pushData(int peerId, int slotOffset, int length, String shuffleKey, String partitionUniqueId, RpcResponseCallback callback) {
    byte[] payload = encodePushDataPayload(shuffleKey, partitionUniqueId);
    nativePushData(nativePtr, peerId, slotOffset, length, payload, callback);
  }

  public void pushData(String remotePeer, int slotOffset, int length, String shuffleKey, String partitionUniqueId, RpcResponseCallback callback) {
    pushData(getPeerId(remotePeer), slotOffset, length, shuffleKey, partitionUniqueId, callback);
  }

  public void pushData(int peerId, byte[] body, String shuffleKey, String partitionUniqueId, RpcResponseCallback callback) {
    int slotOffset = nativeAcquirePushSlot(nativePtr);
    ByteBuffer slice = getLocalBufferSlice(slotOffset, body.length);
    slice.put(body);
    pushData(peerId, slotOffset, body.length, shuffleKey, partitionUniqueId, callback);
  }

  public void pushData(String remotePeer, byte[] body, String shuffleKey, String partitionUniqueId, RpcResponseCallback callback) {
    pushData(getPeerId(remotePeer), body, shuffleKey, partitionUniqueId, callback);
  }

  public void pushDataBatched(int peerId, int[] slotOffsets, int[] lengths, String[] shuffleKeys, String[] partitionUniqueIds, RpcResponseCallback[] callbacks) {
    byte[][] payloads = new byte[slotOffsets.length][];
    for(int i = 0; i < slotOffsets.length; i++) {
        payloads[i] = encodePushDataPayload(shuffleKeys[i], partitionUniqueIds[i]);
    }
    nativePushDataBatched(nativePtr, peerId, slotOffsets, lengths, payloads, callbacks);
  }

  public void pushDataBatched(String remotePeer, int[] slotOffsets, int[] lengths, String[] shuffleKeys, String[] partitionUniqueIds, RpcResponseCallback[] callbacks) {
    pushDataBatched(getPeerId(remotePeer), slotOffsets, lengths, shuffleKeys, partitionUniqueIds, callbacks);
  }

  public void pushMergedData(int peerId, int slotOffset, int length, String shuffleKey, String[] partitionUniqueIds, int[] offsets, RpcResponseCallback callback) {
    byte[] payload = encodePushMergedPayload(shuffleKey, partitionUniqueIds, offsets);
    nativePushMergedData(nativePtr, peerId, slotOffset, length, payload, callback);
  }

  public void pushMergedData(String remotePeer, int slotOffset, int length, String shuffleKey, String[] partitionUniqueIds, int[] offsets, RpcResponseCallback callback) {
    pushMergedData(getPeerId(remotePeer), slotOffset, length, shuffleKey, partitionUniqueIds, offsets, callback);
  }

  public void pushMergedData(int peerId, byte[] body, String shuffleKey, String[] partitionUniqueIds, int[] offsets, RpcResponseCallback callback) {
    int slotOffset = nativeAcquirePushSlot(nativePtr);
    ByteBuffer slice = getLocalBufferSlice(slotOffset, body.length);
    slice.put(body);
    pushMergedData(peerId, slotOffset, body.length, shuffleKey, partitionUniqueIds, offsets, callback);
  }

  public void pushMergedData(String remotePeer, byte[] body, String shuffleKey, String[] partitionUniqueIds, int[] offsets, RpcResponseCallback callback) {
    pushMergedData(getPeerId(remotePeer), body, shuffleKey, partitionUniqueIds, offsets, callback);
  }

  public void pushMergedDataBatched(int peerId, int[] slotOffsets, int[] lengths, String[] shuffleKeys, String[][] partitionUniqueIds, int[][] offsets, RpcResponseCallback[] callbacks) {
    byte[][] payloads = new byte[slotOffsets.length][];
    for (int i = 0; i < slotOffsets.length; i++) {
        payloads[i] = encodePushMergedPayload(shuffleKeys[i], partitionUniqueIds[i], offsets[i]);
    }
    nativePushMergedDataBatched(nativePtr, peerId, slotOffsets, lengths, payloads, callbacks);
  }

  public void pushMergedDataBatched(String remotePeer, int[] slotOffsets, int[] lengths, String[] shuffleKeys, String[][] partitionUniqueIds, int[][] offsets, RpcResponseCallback[] callbacks) {
    pushMergedDataBatched(getPeerId(remotePeer), slotOffsets, lengths, shuffleKeys, partitionUniqueIds, offsets, callbacks);
  }

  // --- Server API ---
  public interface ServerJniHandler {
    void onFetchRequest(String clientPeer, long streamId, int chunkIndex, int slotOffset);
    void onPushData(String clientPeer, int slotOffset, int length, byte[] payload);
    void onPushMergedData(String clientPeer, int slotOffset, int length, byte[] payload);
  }

  public void setServerHandler(ServerJniHandler handler) {
    this.serverHandler = handler;
  }

  public void initServerClientPool(String clientPeer, ByteBuffer clientBuffer) {
    this.serverClientBuffers.put(clientPeer, clientBuffer); // Note: server maintains pool per client locally in ServerContext if needed
    nativeInitServerClientPool(nativePtr, clientPeer);
  }

  public void serverChunkReady(int clientPeerId, long streamId, int chunkIndex, int length, int slotOffset) {
    nativeServerChunkReady(nativePtr, clientPeerId, streamId, chunkIndex, length, slotOffset);
  }

  public void serverChunkReady(String clientPeer, long streamId, int chunkIndex, int length, int slotOffset) {
    serverChunkReady(getPeerId(clientPeer), streamId, chunkIndex, length, slotOffset);
  }

  public void serverPushComplete(int clientPeerId, int slotOffset, byte statusCode) {
    nativeServerPushComplete(nativePtr, clientPeerId, slotOffset, statusCode);
  }

  public void serverPushComplete(String clientPeer, int slotOffset, byte statusCode) {
    serverPushComplete(getPeerId(clientPeer), slotOffset, statusCode);
  }

  public void serverPushFailed(int clientPeerId, int slotOffset, String errorMsg) {
    nativeServerPushFailed(nativePtr, clientPeerId, slotOffset, errorMsg);
  }

  public void serverPushFailed(String clientPeer, int slotOffset, String errorMsg) {
    serverPushFailed(getPeerId(clientPeer), slotOffset, errorMsg);
  }

  // --- Support ---
  public ByteBuffer getLocalBufferSlice(int slotOffset, int length) {
    return getLocalBufferSlice(null, slotOffset, length);
  }

  public ByteBuffer getLocalBufferSlice(String clientPeer, int slotOffset, int length) {
    ByteBuffer slice;
    ByteBuffer buf = (clientPeer != null && serverClientBuffers.containsKey(clientPeer)) 
                     ? serverClientBuffers.get(clientPeer) 
                     : this.localBuffer;
    synchronized (buf) {
      ByteBuffer dup = buf.duplicate();
      dup.position(slotOffset);
      dup.limit(slotOffset + length);
      slice = dup.slice();
    }
    return slice;
  }

  public void releaseClientSlot(int slotOffset) {
    nativeReleaseSlot(nativePtr, slotOffset);
  }

  // --- Static Upcall Dispatchers (called from C++) ---
  private static CommsWrapper getWrapper(long ptr) { return activeWrappers.get(ptr); }

  public static void dispatchClientFetchSuccess(long nativePtr, int chunkIndex, ChunkReceivedCallback callback, int length, int slotOffset) {
    CommsWrapper wrapper = getWrapper(nativePtr);
    if (wrapper != null) {
      ByteBuffer sliced = wrapper.getLocalBufferSlice(slotOffset, length);
      ByteBuf customBuf = new CustomRDMAByteBuf(
          io.netty.buffer.UnpooledByteBufAllocator.DEFAULT,
          sliced,
          length,
          () -> wrapper.releaseClientSlot(slotOffset)
      );
      ManagedBuffer managedBuffer = new NettyManagedBuffer(customBuf);
      callback.onSuccess(chunkIndex, managedBuffer);
      managedBuffer.release();
    }
  }

  public static void dispatchClientFetchFailed(long nativePtr, int chunkIndex, ChunkReceivedCallback callback, String errorMsg, int slotOffset) {
    CommsWrapper wrapper = getWrapper(nativePtr);
    if (wrapper != null) {
      callback.onFailure(chunkIndex, new java.io.IOException("RDMA fetch failed: " + errorMsg));
      wrapper.releaseClientSlot(slotOffset);
    }
  }

  public static void dispatchClientPushComplete(long nativePtr, RpcResponseCallback callback, byte statusCode, int slotOffset) {
    CommsWrapper wrapper = getWrapper(nativePtr);
    if (wrapper != null) {
      callback.onSuccess(ByteBuffer.wrap(new byte[]{statusCode}));
      wrapper.releaseClientSlot(slotOffset);
    }
  }

  public static void dispatchClientPushFailed(long nativePtr, RpcResponseCallback callback, String errorMsg, int slotOffset) {
    CommsWrapper wrapper = getWrapper(nativePtr);
    if (wrapper != null) {
      callback.onFailure(new java.io.IOException("RDMA push failed: " + errorMsg));
      wrapper.releaseClientSlot(slotOffset);
    }
  }

  public static void dispatchServerFetchRequest(long nativePtr, String clientPeer, long streamId, int chunkIndex, int slotOffset) {
    CommsWrapper wrapper = getWrapper(nativePtr);
    if (wrapper != null && wrapper.serverHandler != null) {
      wrapper.serverHandler.onFetchRequest(clientPeer, streamId, chunkIndex, slotOffset);
    }
  }

  public static void dispatchServerPushData(long nativePtr, String clientPeer, int slotOffset, int length, byte[] payload) {
    CommsWrapper wrapper = getWrapper(nativePtr);
    if (wrapper != null && wrapper.serverHandler != null) {
      wrapper.serverHandler.onPushData(clientPeer, slotOffset, length, payload);
    }
  }

  public static void dispatchServerPushMergedData(long nativePtr, String clientPeer, int slotOffset, int length, byte[] payload) {
    CommsWrapper wrapper = getWrapper(nativePtr);
    if (wrapper != null && wrapper.serverHandler != null) {
      wrapper.serverHandler.onPushMergedData(clientPeer, slotOffset, length, payload);
    }
  }

  @Override
  public synchronized void close() {
    activeWrappers.remove(nativePtr);
    nativeDestroy(nativePtr);
  }

  public static native long getDirectBufferAddress(ByteBuffer buf);

  // Nested Types
  public enum MemoryType { Dram, Vram }

  public static class NativeBuffer {
      public final ByteBuffer buffer;
      public final MemToken token;
      public NativeBuffer(ByteBuffer buffer, MemToken token) { this.buffer = buffer; this.token = token; }
  }

  public static class MemToken implements AutoCloseable {
    private long nativePtr;
    MemToken(long nativePtr) { this.nativePtr = nativePtr; }
    long getNativePtr() { return nativePtr; }
    public byte[] serialize() { return nativeMemTokenSerialize(nativePtr); }
    public long getAddress() { return nativeMemTokenGetAddress(nativePtr); }
    public long getSize() { return nativeMemTokenGetSize(nativePtr); }
    @Override public void close() {
      if (nativePtr != 0) { nativeMemTokenDelete(nativePtr); nativePtr = 0; }
    }
  }

  // --- Native Declarations ---
  private static native long nativeCreate();
  private static native void nativeDestroy(long nativePtr);
  private static native void nativeInit(long nativePtr, Map<String, String> params);
  private static native byte[] nativeGetEndpointInfo(long nativePtr);
  private static native int nativeAddRemoteEndpoint(long nativePtr, String peerName, byte[] opaqueHandleBytes, boolean block);
  private static native int nativeGetPeerId(long nativePtr, String peerName);
  private static native void nativeConnect(long nativePtr, int peerId);
  private static native ByteBuffer nativeAllocateAndRegMem(long nativePtr, long size, int memoryType, long[] tokenPtrOut);
  private static native void nativeDeregAndFreeMem(long nativePtr, ByteBuffer buffer, long memTokenPtr);
  private static native long nativeGetMemToken(long nativePtr, byte[] serTok);

  private static native void nativeInitClientPool(long nativePtr, int peerId, int pushSlots, int pushSlotSize, int fetchSlots, int fetchSlotSize, long localTokenPtr, long remoteTokenPtr);
  private static native void nativeFetchChunk(long nativePtr, int peerId, long streamId, int chunkIndex, ChunkReceivedCallback callback);
  private static native void nativeFetchChunksBatched(long nativePtr, int peerId, long[] streamIds, int[] chunkIndices, ChunkReceivedCallback[] callbacks);
  private static native int nativeAcquirePushSlot(long nativePtr);
  private static native void nativePushData(long nativePtr, int peerId, int slotOffset, int length, byte[] payload, RpcResponseCallback callback);
  private static native void nativePushDataBatched(long nativePtr, int peerId, int[] slotOffsets, int[] lengths, byte[][] payloads, RpcResponseCallback[] callbacks);
  private static native void nativePushMergedData(long nativePtr, int peerId, int slotOffset, int length, byte[] payload, RpcResponseCallback callback);
  private static native void nativePushMergedDataBatched(long nativePtr, int peerId, int[] slotOffsets, int[] lengths, byte[][] payloads, RpcResponseCallback[] callbacks);
  private static native void nativeReleaseSlot(long nativePtr, int slotOffset);
  
  private static native void nativeInitServerClientPool(long nativePtr, String clientPeer);
  private static native void nativeServerChunkReady(long nativePtr, int clientPeerId, long streamId, int chunkIndex, int length, int slotOffset);
  private static native void nativeServerPushComplete(long nativePtr, int clientPeerId, int slotOffset, byte statusCode);
  private static native void nativeServerPushFailed(long nativePtr, int clientPeerId, int slotOffset, String errorMsg);

  private static native byte[] nativeMemTokenSerialize(long tokenPtr);
  private static native long nativeMemTokenGetAddress(long tokenPtr);
  private static native long nativeMemTokenGetSize(long tokenPtr);
  private static native void nativeMemTokenDelete(long tokenPtr);

  public void startOobServerAsync(int oobPort, long poolSize, String serverPeerName) {
    nativeStartOobServerAsync(nativePtr, oobPort, poolSize, serverPeerName);
  }

  public void connectAndRegisterOOBAsync(String serverIp, int oobPort, String localPeerName, String serverPeerName, int pushSlotsCount, int pushSlotSize, int fetchSlotsCount, int fetchSlotSize, RpcResponseCallback callback) {
    nativeConnectAndRegisterOOBAsync(nativePtr, serverIp, oobPort, localPeerName, serverPeerName, pushSlotsCount, pushSlotSize, fetchSlotsCount, fetchSlotSize, callback);
  }

  public static void dispatchClientSetupComplete(long nativePtr, RpcResponseCallback callback, byte statusCode, ByteBuffer directBuffer) {
    CommsWrapper wrapper = getWrapper(nativePtr);
    if (wrapper != null) {
      wrapper.localBuffer = directBuffer;
      callback.onSuccess(directBuffer);
    }
  }

  public static void dispatchClientSetupFailed(long nativePtr, RpcResponseCallback callback, String errorMsg) {
    CommsWrapper wrapper = getWrapper(nativePtr);
    if (wrapper != null) {
      callback.onFailure(new java.io.IOException("RDMA setup failed: " + errorMsg));
    }
  }

  public static void dispatchServerClientConnected(long nativePtr, String clientPeer, ByteBuffer directBuffer) {
    CommsWrapper wrapper = getWrapper(nativePtr);
    if (wrapper != null) {
      wrapper.initServerClientPool(clientPeer, directBuffer);
    }
  }

  private static java.io.File createTempDir() throws java.io.IOException {
    java.io.File tempDir = java.io.File.createTempFile("ml_transport_natives-", "");
    if (!tempDir.delete() || !tempDir.mkdir()) throw new java.io.IOException("Failed to create temp directory");
    tempDir.deleteOnExit();
    return tempDir;
  }

  private static java.io.File extractResource(java.io.File destDir, String resourcePath) throws java.io.IOException {
    java.io.InputStream in = CommsWrapper.class.getResourceAsStream(resourcePath);
    if (in == null) throw new java.io.FileNotFoundException("Resource not found in JAR: " + resourcePath);
    String filename = resourcePath.substring(resourcePath.lastIndexOf('/') + 1);
    java.io.File destFile = new java.io.File(destDir, filename);
    destFile.deleteOnExit();
    try (java.io.FileOutputStream out = new java.io.FileOutputStream(destFile)) {
      byte[] buffer = new byte[8192];
      int read;
      while ((read = in.read(buffer)) != -1) out.write(buffer, 0, read);
    }
    return destFile;
  }

  private static native void nativeStartOobServerAsync(long nativePtr, int oobPort, long poolSize, String serverPeerName);
  private static native void nativeConnectAndRegisterOOBAsync(long nativePtr, String serverIp, int oobPort, String localPeerName, String serverPeerName, int pushSlots, int pushSlotSize, int fetchSlots, int fetchSlotSize, RpcResponseCallback callback);
}
