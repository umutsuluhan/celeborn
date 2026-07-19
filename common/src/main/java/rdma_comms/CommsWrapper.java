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

  public void addRemoteEndpoint(String peerName, byte[] opaqueHandleBytes, boolean block) {
    nativeAddRemoteEndpoint(nativePtr, peerName, opaqueHandleBytes, block);
  }

  public void connect(String peerName) {
    nativeConnect(nativePtr, peerName);
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
  public void initClientPool(int pushSlots, int pushSlotSize, int fetchSlots, int fetchSlotSize, ByteBuffer localBuffer, MemToken localToken, MemToken remoteToken, String serverPeerName) {
    this.localBuffer = localBuffer;
    nativeInitClientPool(nativePtr, serverPeerName, pushSlots, pushSlotSize, fetchSlots, fetchSlotSize, localToken.getNativePtr(), remoteToken.getNativePtr());
  }

  public void fetchChunk(String remotePeer, long streamId, int chunkIndex, ChunkReceivedCallback callback) {
    nativeFetchChunk(nativePtr, remotePeer, streamId, chunkIndex, callback);
  }

  public void pushData(String remotePeer, byte[] body, String shuffleKey, String partitionUniqueId, RpcResponseCallback callback) {
    int slotOffset = nativeAcquirePushSlot(nativePtr);
    ByteBuffer slice = localBuffer.slice();
    slice.position(slotOffset);
    slice.put(body);
    nativePushData(nativePtr, remotePeer, slotOffset, body.length, shuffleKey, partitionUniqueId, callback);
  }

  public void pushMergedData(String remotePeer, byte[] body, String shuffleKey, String[] partitionUniqueIds, int[] offsets, RpcResponseCallback callback) {
    String pIds = String.join(",", partitionUniqueIds);
    String offs = java.util.Arrays.stream(offsets).mapToObj(String::valueOf).collect(java.util.stream.Collectors.joining(","));
    
    int slotOffset = nativeAcquirePushSlot(nativePtr);
    ByteBuffer slice = localBuffer.slice();
    slice.position(slotOffset);
    slice.put(body);
    
    nativePushMergedData(nativePtr, remotePeer, slotOffset, body.length, shuffleKey, pIds, offs, callback);
  }

  // --- Server API ---
  public interface ServerJniHandler {
    void onFetchRequest(String clientPeer, long streamId, int chunkIndex, int slotOffset);
    void onPushData(String clientPeer, int slotOffset, int length, String shuffleKey, String partitionUniqueId);
    void onPushMergedData(String clientPeer, int slotOffset, int length, String shuffleKey, String payload);
  }

  public void setServerHandler(ServerJniHandler handler) {
    this.serverHandler = handler;
  }

  public void initServerClientPool(String clientPeer, ByteBuffer clientBuffer) {
    this.serverClientBuffers.put(clientPeer, clientBuffer); // Note: server maintains pool per client locally in ServerContext if needed
    nativeInitServerClientPool(nativePtr, clientPeer);
  }

  public void serverChunkReady(String clientPeer, long streamId, int chunkIndex, int length, int slotOffset) {
    nativeServerChunkReady(nativePtr, clientPeer, streamId, chunkIndex, length, slotOffset);
  }

  public void serverPushComplete(String clientPeer, int slotOffset, byte statusCode) {
    nativeServerPushComplete(nativePtr, clientPeer, slotOffset, statusCode);
  }

  public void serverPushFailed(String clientPeer, int slotOffset, String errorMsg) {
    nativeServerPushFailed(nativePtr, clientPeer, slotOffset, errorMsg);
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

  public static void dispatchServerPushData(long nativePtr, String clientPeer, int slotOffset, int length, String shuffleKey, String partitionUniqueId) {
    CommsWrapper wrapper = getWrapper(nativePtr);
    if (wrapper != null && wrapper.serverHandler != null) {
      wrapper.serverHandler.onPushData(clientPeer, slotOffset, length, shuffleKey, partitionUniqueId);
    }
  }

  public static void dispatchServerPushMergedData(long nativePtr, String clientPeer, int slotOffset, int length, String shuffleKey, String payload) {
    CommsWrapper wrapper = getWrapper(nativePtr);
    if (wrapper != null && wrapper.serverHandler != null) {
      wrapper.serverHandler.onPushMergedData(clientPeer, slotOffset, length, shuffleKey, payload);
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
  private static native void nativeAddRemoteEndpoint(long nativePtr, String peerName, byte[] opaqueHandleBytes, boolean block);
  private static native void nativeConnect(long nativePtr, String peerName);
  private static native ByteBuffer nativeAllocateAndRegMem(long nativePtr, long size, int memoryType, long[] tokenPtrOut);
  private static native void nativeDeregAndFreeMem(long nativePtr, ByteBuffer buffer, long memTokenPtr);
  private static native long nativeGetMemToken(long nativePtr, byte[] serTok);

  private static native void nativeInitClientPool(long nativePtr, String peerName, int pushSlots, int pushSlotSize, int fetchSlots, int fetchSlotSize, long localTokenPtr, long remoteTokenPtr);
  private static native void nativeFetchChunk(long nativePtr, String remotePeer, long streamId, int chunkIndex, ChunkReceivedCallback callback);
  private static native int nativeAcquirePushSlot(long nativePtr);
  private static native void nativePushData(long nativePtr, String remotePeer, int slotOffset, int length, String shuffleKey, String partitionUniqueId, RpcResponseCallback callback);
  private static native void nativePushMergedData(long nativePtr, String remotePeer, int slotOffset, int length, String shuffleKey, String partitionIds, String offsets, RpcResponseCallback callback);
  private static native void nativeReleaseSlot(long nativePtr, int slotOffset);
  
  private static native void nativeInitServerClientPool(long nativePtr, String clientPeer);
  private static native void nativeServerChunkReady(long nativePtr, String clientPeer, long streamId, int chunkIndex, int length, int slotOffset);
  private static native void nativeServerPushComplete(long nativePtr, String clientPeer, int slotOffset, byte statusCode);
  private static native void nativeServerPushFailed(long nativePtr, String clientPeer, int slotOffset, String errorMsg);

  private static native byte[] nativeMemTokenSerialize(long tokenPtr);
  private static native long nativeMemTokenGetAddress(long tokenPtr);
  private static native long nativeMemTokenGetSize(long tokenPtr);
  private static native void nativeMemTokenDelete(long tokenPtr);

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
}
