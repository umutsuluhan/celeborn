package rdma_comms;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import io.netty.buffer.ByteBuf;
import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommsServer implements CommsWrapper.ServerJniHandler {
  private static final Logger logger = LoggerFactory.getLogger(CommsServer.class);

  private final String transportType;
  private final String localPeerName;
  private final String localIp;
  private final int bootstrapPort;
  private final int oobPort;
  
  private final int slotSize;
  private final long poolSize;

  private CommsWrapper comms;
  private ServerSocket listenSock;
  private final Map<String, String> clientPeerNames = new ConcurrentHashMap<>();
  private final Map<String, Integer> clientPeerIds = new ConcurrentHashMap<>();
  private final AtomicBoolean running = new AtomicBoolean(false);
  public boolean rdmaTrackerEnabled = true;

  public interface ChunkFetchHandler {
    default org.apache.celeborn.common.network.buffer.ManagedBuffer getChunkBuffer(long streamId, int chunkIndex) throws IOException {
      return null;
    }
    default int fetchChunk(long streamId, int chunkIndex, ByteBuffer target) throws IOException {
      org.apache.celeborn.common.network.buffer.ManagedBuffer buf = getChunkBuffer(streamId, chunkIndex);
      if (buf != null) {
        return copyBufferToTarget(buf, target, true);
      }
      return 0;
    }
  }

  public static int copyBufferToTarget(org.apache.celeborn.common.network.buffer.ManagedBuffer buffer, ByteBuffer target, boolean trackerEnabled) throws IOException {
    long t0 = trackerEnabled ? System.nanoTime() : 0;
    try {
      ByteBuffer nioBuf = buffer.nioByteBuffer();
      int len = nioBuf.remaining();
      target.put(nioBuf);
      return len;
    } finally {
      if (trackerEnabled) {
        rdma_comms.RDMATracker.record(rdma_comms.RDMATracker.CallType.SERVER_FETCH_CHUNK_COPY, System.nanoTime() - t0);
      }
    }
  }
  public interface ChunkPushHandler {
    void pushData(String shuffleKey, String partitionUniqueId, ByteBuf body, RpcResponseCallback callback) throws IOException;
    void pushMergedData(String shuffleKey, String[] partitionUniqueIds, int[] offsets, ByteBuf body, RpcResponseCallback callback) throws IOException;
  }

  private ChunkFetchHandler chunkFetchHandler;
  private ChunkPushHandler chunkPushHandler;
  private java.util.concurrent.ExecutorService fetchExecutor;
  private java.util.concurrent.ExecutorService registerExecutor;

  public CommsServer(String transportType, String localPeerName, String localIp, 
                     int bootstrapPort, int bufferSize, int oobPort,
                     int pushSlotsCount, int fetchSlotsCount, int pushSlotSize, int fetchSlotSize) {
    this.transportType = transportType;
    this.localPeerName = localPeerName;
    this.localIp = (localIp == null || localIp.isEmpty()) ? org.apache.celeborn.common.util.JavaUtils.getLocalHost() : localIp;
    this.bootstrapPort = bootstrapPort;
    this.oobPort = oobPort;
    this.slotSize = bufferSize + 4 * 1024 * 1024;
    int pSize = pushSlotSize;
    int fSize = fetchSlotSize == 0 ? this.slotSize : fetchSlotSize;
    this.poolSize = ((long) pushSlotsCount * pSize) + ((long) fetchSlotsCount * fSize);
  }

  public void registerChunkFetchHandler(ChunkFetchHandler handler) { this.chunkFetchHandler = handler; }
  public void registerChunkPushHandler(ChunkPushHandler handler) { this.chunkPushHandler = handler; }

  public void setup() {
    long t0 = rdmaTrackerEnabled ? System.nanoTime() : 0;
    try {
      String transport = "RDMA".equalsIgnoreCase(transportType) ? "1" : "0";
      try {
        this.comms = new CommsWrapper();
        Map<String, String> params = new HashMap<>();
        params.put("AP_TRANSPORT", transport);
        params.put("AP_LOCAL_PEER_NAME", localPeerName);
        params.put("AP_BOOTSTRAP_PORT", String.valueOf(bootstrapPort));
        params.put("AP_BOOTSTRAP_IP", localIp);

        comms.init(params);
        comms.setServerHandler(this); // Register for JNI upcalls!
        this.running.set(true);

        this.fetchExecutor = java.util.concurrent.Executors.newFixedThreadPool(64, r -> {
          Thread t = new Thread(r, "RDMA-Server-Fetch");
          t.setDaemon(true); return t;
        });
        this.registerExecutor = java.util.concurrent.Executors.newFixedThreadPool(Math.max(1, Runtime.getRuntime().availableProcessors() / 4), r -> {
          Thread t = new Thread(r, "RDMA-Server-Register");
          t.setDaemon(true); return t;
        });

        comms.startOobServerAsync(oobPort, this.poolSize, localPeerName);

      } catch (Exception e) {
        logger.error("Comms initialization failed", e);
        shutdown();
      }
    } finally {
      if (rdmaTrackerEnabled) rdma_comms.RDMATracker.record(rdma_comms.RDMATracker.CallType.SERVER_SETUP, System.nanoTime() - t0);
    }
  }

  // --- Handlers Invoked from C++ ---
  @Override
  public void onFetchRequest(String clientPeer, long streamId, int chunkIndex, int slotOffset) {
    if (chunkFetchHandler == null) return;
    long t0 = rdmaTrackerEnabled ? System.nanoTime() : 0;
    try {
      fetchExecutor.submit(() -> {
        try {
          ByteBuffer target = comms.getLocalBufferSlice(clientPeer, slotOffset, this.slotSize);
          int length = chunkFetchHandler.fetchChunk(streamId, chunkIndex, target);
          // Signal C++ to send CHUNK_READY
          comms.serverChunkReady(clientPeer, streamId, chunkIndex, length, slotOffset);
        } catch (Exception e) {
          logger.error("Fetch failed", e);
          // Actually we don't have a specific failed API to C++ for this, but ideally we'd send CHUNK_FAILED. 
          // C++ doesn't parse server chunk failed yet, so we just log.
        }
      });
    } finally {
      if (rdmaTrackerEnabled) rdma_comms.RDMATracker.record(rdma_comms.RDMATracker.CallType.SERVER_FETCH_CHUNK_REQ, System.nanoTime() - t0);
    }
  }

  @Override
  public void onPushData(String clientPeer, int slotOffset, int length, byte[] payload) {
    if (chunkPushHandler == null) return;
    long t0 = rdmaTrackerEnabled ? System.nanoTime() : 0;
    try {
      fetchExecutor.submit(() -> {
        try {
          ByteBuffer buf = ByteBuffer.wrap(payload);
          int keyLen = buf.getInt();
          byte[] keyBytes = new byte[keyLen];
          buf.get(keyBytes);
          String shuffleKey = new String(keyBytes, java.nio.charset.StandardCharsets.UTF_8);
          
          int pidLen = buf.getInt();
          byte[] pidBytes = new byte[pidLen];
          buf.get(pidBytes);
          String partitionUniqueId = new String(pidBytes, java.nio.charset.StandardCharsets.UTF_8);

          ByteBuffer slice = comms.getLocalBufferSlice(clientPeer, slotOffset, length);
          ByteBuf body = io.netty.buffer.PooledByteBufAllocator.DEFAULT.directBuffer(length);
          body.writeBytes(slice);
          
          chunkPushHandler.pushData(shuffleKey, partitionUniqueId, body, new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer response) {
              byte statusCode = (response != null && response.remaining() > 0) ? response.get(response.position()) : 0;
              comms.serverPushComplete(comms.getPeerId(clientPeer), slotOffset, statusCode);
            }
            @Override
            public void onFailure(Throwable e) {
              comms.serverPushFailed(comms.getPeerId(clientPeer), slotOffset, e.getMessage());
            }
          });
          body.release();
        } catch (Throwable t) {
          comms.serverPushFailed(comms.getPeerId(clientPeer), slotOffset, t.getMessage());
        }
      });
    } finally {
      if (rdmaTrackerEnabled) rdma_comms.RDMATracker.record(rdma_comms.RDMATracker.CallType.SERVER_PUSH_DATA_REQ, System.nanoTime() - t0);
    }
  }

  @Override
  public void onPushMergedData(String clientPeer, int slotOffset, int length, byte[] payload) {
    if (chunkPushHandler == null) return;
    long t0 = rdmaTrackerEnabled ? System.nanoTime() : 0;
    try {
      fetchExecutor.submit(() -> {
        try {
          ByteBuffer buf = ByteBuffer.wrap(payload);
          int keyLen = buf.getInt();
          byte[] keyBytes = new byte[keyLen];
          buf.get(keyBytes);
          String shuffleKey = new String(keyBytes, java.nio.charset.StandardCharsets.UTF_8);
          
          int numPids = buf.getInt();
          String[] partitionUniqueIds = new String[numPids];
          for (int i = 0; i < numPids; i++) {
              int pidLen = buf.getInt();
              byte[] pidBytes = new byte[pidLen];
              buf.get(pidBytes);
              partitionUniqueIds[i] = new String(pidBytes, java.nio.charset.StandardCharsets.UTF_8);
          }
          
          int numOffsets = buf.getInt();
          int[] offsets = new int[numOffsets];
          for (int i = 0; i < numOffsets; i++) {
              offsets[i] = buf.getInt();
          }

          ByteBuffer slice = comms.getLocalBufferSlice(clientPeer, slotOffset, length);
          ByteBuf body = io.netty.buffer.PooledByteBufAllocator.DEFAULT.directBuffer(length);
          body.writeBytes(slice);
          
          chunkPushHandler.pushMergedData(shuffleKey, partitionUniqueIds, offsets, body, new RpcResponseCallback() {
            @Override
            public void onSuccess(ByteBuffer response) {
              byte statusCode = (response != null && response.remaining() > 0) ? response.get(response.position()) : 0;
              comms.serverPushComplete(comms.getPeerId(clientPeer), slotOffset, statusCode);
            }
            @Override
            public void onFailure(Throwable e) {
              comms.serverPushFailed(comms.getPeerId(clientPeer), slotOffset, e.getMessage());
            }
          });
          body.release();
        } catch (Throwable t) {
          comms.serverPushFailed(comms.getPeerId(clientPeer), slotOffset, t.getMessage());
        }
      });
    } finally {
      if (rdmaTrackerEnabled) rdma_comms.RDMATracker.record(rdma_comms.RDMATracker.CallType.SERVER_PUSH_MERGED_DATA_REQ, System.nanoTime() - t0);
    }
  }

  public void shutdown() {
    running.set(false);
    if (fetchExecutor != null) fetchExecutor.shutdownNow();
    if (registerExecutor != null) registerExecutor.shutdownNow();
    if (listenSock != null) { try { listenSock.close(); } catch (Exception e) {} }
    if (comms != null) { try { comms.close(); } catch (Exception e) {} comms = null; }
  }

  public String getClientPeerName(String clientIp) { return clientPeerNames.get(clientIp); }
  public CommsWrapper getComms() { return comms; }
}
