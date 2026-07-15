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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.celeborn.common.network.client.RpcResponseCallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommsServer {
  private static final Logger logger = LoggerFactory.getLogger(CommsServer.class);

  private final String transportType;
  private final String localPeerName;
  private final String localIp;
  private final int bootstrapPort;
  private final int oobPort;
  private final boolean testMode;

  // Dynamic slotted pool fields
  private final int poolSize;
  private final int slotSize;
  private final int numSlots;
  private final int pushSlotsCount;
  private final int fetchSlotsCount;
  private final int pushSlotSize;
  private final int fetchSlotSize;

  // Persistent fields kept alive after setup completes
  private CommsWrapper comms;
  private ServerSocket listenSock;
  private final Map<String, String> clientPeerNames = new ConcurrentHashMap<>();
  private final Map<String, ClientContext> clientContexts = new ConcurrentHashMap<>();
  private final AtomicBoolean running = new AtomicBoolean(false);
  public boolean rdmaTrackerEnabled = true;

  // Callback interface for fetching chunks from Celeborn Worker
  public interface ChunkFetchHandler {
    /**
     * Fetches the chunk and writes it into the target ByteBuffer.
     * Returns the actual length of the chunk.
     */
    int fetchChunk(long streamId, int chunkIndex, ByteBuffer target) throws IOException;
  }

  // Callback interface for pushing chunks to Celeborn Worker
  public interface ChunkPushHandler {
    /**
     * Pushes data into the worker's storage.
     */
    void pushData(String shuffleKey, String partitionUniqueId, ByteBuf body, RpcResponseCallback callback) throws IOException;
    void pushMergedData(String shuffleKey, String[] partitionUniqueIds, int[] offsets, ByteBuf body, RpcResponseCallback callback) throws IOException;
  }

  private ChunkFetchHandler chunkFetchHandler;
  private ChunkPushHandler chunkPushHandler;
  private java.util.concurrent.ExecutorService fetchExecutor;
  private java.util.concurrent.ExecutorService registerExecutor;

  /**
   * Constructor.
   */
  public CommsServer(String transportType, String localPeerName, String localIp, 
                     int bootstrapPort, int bufferSize, int oobPort, boolean testMode,
                     int pushSlotsCount, int fetchSlotsCount, int pushSlotSize, int fetchSlotSize) {
    this.transportType = transportType;
    this.localPeerName = localPeerName;
    this.localIp = (localIp == null || localIp.isEmpty())
        ? org.apache.celeborn.common.util.JavaUtils.getLocalHost()
        : localIp;
    this.bootstrapPort = bootstrapPort;
    this.oobPort = oobPort;
    this.testMode = testMode;

    this.slotSize = bufferSize + 4 * 1024 * 1024;
    
    this.pushSlotsCount = pushSlotsCount;
    this.fetchSlotsCount = fetchSlotsCount;
    this.pushSlotSize = pushSlotSize;
    this.fetchSlotSize = fetchSlotSize == 0 ? this.slotSize : fetchSlotSize;

    this.numSlots = this.pushSlotsCount + this.fetchSlotsCount;
    this.poolSize = (this.pushSlotsCount * this.pushSlotSize) + (this.fetchSlotsCount * this.fetchSlotSize);

    logger.info("CommsServer initialized with pushSlots: {} (size: {}KB), fetchSlots: {} (size: {}MB), totalPoolSize: {}MB", 
        this.pushSlotsCount, this.pushSlotSize / 1024, this.fetchSlotsCount, this.fetchSlotSize / (1024 * 1024), poolSize / (1024 * 1024));
  }

  public void registerChunkFetchHandler(ChunkFetchHandler handler) {
    this.chunkFetchHandler = handler;
    logger.info("ChunkFetchHandler registered.");
  }

  public void registerChunkPushHandler(ChunkPushHandler handler) {
    this.chunkPushHandler = handler;
    logger.info("ChunkPushHandler registered.");
  }

  /**
   * Sets up the Control Plane:
   * 1. Initializes the Comms library.
   * 2. Starts the OOB TCP server to accept client connections.
   * 3. For each client, allocates and registers a dedicated memory pool.
   */
  public void setup() {
    long t0_tracker = rdmaTrackerEnabled ? System.nanoTime() : 0;
    try {
    String transport = "0"; 
    if ("RDMA".equalsIgnoreCase(transportType)) {
      transport = "1";
    }

    try {
      // 1. Initialize Comms
      this.comms = new CommsWrapper();
      Map<String, String> params = new HashMap<>();
      params.put("AP_TRANSPORT", transport);
      params.put("AP_LOCAL_PEER_NAME", localPeerName);
      params.put("AP_BOOTSTRAP_PORT", String.valueOf(bootstrapPort));
      params.put("AP_BOOTSTRAP_IP", localIp);

      comms.init(params);
      logger.info("Comms library initialized.");

      this.running.set(true);
      this.fetchExecutor = java.util.concurrent.Executors.newFixedThreadPool(64, r -> {
        Thread t = new Thread(r, "RDMA-Server-Fetch");
        t.setDaemon(true);
        return t;
      });

      int threadCount = Math.max(1, Runtime.getRuntime().availableProcessors() / 4);
      this.registerExecutor = java.util.concurrent.Executors.newFixedThreadPool(threadCount, r -> {
        Thread t = new Thread(r, "RDMA-Server-Register");
        t.setDaemon(true);
        return t;
      });

      // 2. Start OOB TCP Server to accept clients
      new Thread(() -> {
        try {
          this.listenSock = new ServerSocket(oobPort);
          logger.info("Starting OOB server on port {}...", oobPort);

          while (running.get() && !listenSock.isClosed()) {
            try {
              Socket clientSock = listenSock.accept();
              String clientIp = clientSock.getInetAddress().getHostAddress();
              logger.info("OOB Client connected from {}", clientSock.getRemoteSocketAddress());

              // Handle client registration in a bounded pool to allow parallel memory allocations while preventing RDMA JNI storms
              registerExecutor.submit(() -> handleClientRegistration(clientSock, clientIp));

            } catch (IOException e) {
              if (running.get() && !listenSock.isClosed()) {
                logger.error("Client connection handle error", e);
              }
            }
          }
        } catch (IOException e) {
          logger.error("CommsServer socket failed", e);
        }
      }, "OOB-CommsServer-Accept-Loop").start();

    } catch (Exception e) {
      logger.error("Comms initialization failed", e);
      shutdown();
    }
    } finally {
      if (rdmaTrackerEnabled) {
        rdma_comms.RDMATracker.record(rdma_comms.RDMATracker.CallType.SERVER_SETUP, System.nanoTime() - t0_tracker);
      }
    }
  }

  /**
   * Handles client handshake, allocates client-specific buffer, registers memory,
   * and starts the client OOB poller thread.
   */
  private void handleClientRegistration(Socket clientSock, String clientIp) {
    long t0_tracker = rdmaTrackerEnabled ? System.nanoTime() : 0;
    try {
    try (DataInputStream in = new DataInputStream(clientSock.getInputStream());
      DataOutputStream out = new DataOutputStream(clientSock.getOutputStream())) {
      
      logger.debug("Reading clientPeerName...");
      String clientPeerName = in.readUTF();
      logger.debug("Read clientPeerName: {}", clientPeerName);
      
      logger.debug("Reading clientHandleSize...");
      long clientHandleSize = in.readLong();
      
      byte[] clientHandleOpaque = new byte[(int) clientHandleSize];
      in.readFully(clientHandleOpaque);
      logger.debug("Read clientHandleOpaque successfully.");
      
      clientPeerNames.put(clientIp, clientPeerName);
      logger.info("Registered Client: {} IP: {}", clientPeerName, clientIp);

      // Register Client remote endpoint in CommsServer's context
      comms.addRemoteEndpoint(clientPeerName, clientHandleOpaque, true);
      logger.debug("comms.addRemoteEndpoint returned successfully.");

      // Allocate dedicated memory pool for this client
      logger.info("Allocating {}MB native RDMA buffer pool for client: {}...", poolSize / (1024 * 1024), clientPeerName);
      CommsWrapper.NativeBuffer nativeBuffer = comms.allocateAndRegMem(poolSize, CommsWrapper.MemoryType.Dram);
      ByteBuffer clientBuffer = nativeBuffer.buffer;
      CommsWrapper.MemToken clientToken = nativeBuffer.token;
      logger.info("Registered and mapped {}MB DRAM buffer pool for client: {}", poolSize / (1024 * 1024), clientPeerName);

      // Initialize Slot Pool (only use fetch slots for fetching)
      int pushBoundary = this.pushSlotsCount * this.pushSlotSize;

      BlockingQueue<Integer> freeSlots = new LinkedBlockingQueue<>(this.fetchSlotsCount);
      for (int i = 0; i < this.fetchSlotsCount; i++) {
        freeSlots.offer(pushBoundary + i * this.fetchSlotSize);
      }

      // Start OOB notification poller thread for this client
      ClientContext ctx = new ClientContext(clientPeerName, clientBuffer, clientToken, freeSlots, null);
      
      Thread poller = new Thread(() -> pollClientNotifications(clientPeerName), "RDMA-Server-Poller-" + clientPeerName);
      poller.setDaemon(true);
      ctx.pollerThread = poller;
      
      clientContexts.put(clientPeerName, ctx);
      
      poller.start();

      // Send server handles and memory token back to the client
      byte[] serverHandleOpaque = comms.getEndpointInfo();
      long handleSize = serverHandleOpaque.length;
      byte[] serverTokenOpaque = clientToken.serialize();
      long addrVal = CommsWrapper.getDirectBufferAddress(clientBuffer);

      out.writeLong(handleSize);
      out.write(serverHandleOpaque);
      out.writeLong(addrVal);
      out.writeLong(this.poolSize);
      out.write(serverTokenOpaque);
      out.flush();
      logger.info("Shared handles and {}MB pool token sent to client: {}", this.poolSize / (1024 * 1024), clientPeerName);

      logger.info("Waiting for client JNI connection confirmation for {}...", clientPeerName);
      in.readByte();
      logger.info("Client JNI connection confirmed for: {}", clientPeerName);

    } catch (Exception e) {
      logger.error("Failed to register client from IP {}", clientIp, e);
      try { clientSock.close(); } catch (IOException ignored) {}
    }
    } finally {
      if (rdmaTrackerEnabled) {
        rdma_comms.RDMATracker.record(rdma_comms.RDMATracker.CallType.SERVER_HANDLE_CLIENT_REGISTRATION, System.nanoTime() - t0_tracker);
      }
    }
  }

  /**
   * Background loop that polls for OOB notifications from a specific client.
   */
  private void pollClientNotifications(String clientPeerName) {
    logger.info("RDMA Server Poller thread started for client: {}", clientPeerName);
    ClientContext ctx = clientContexts.get(clientPeerName);
    if (ctx == null) {
      logger.error("ClientContext not found for client: {}", clientPeerName);
      return;
    }

    while (running.get() && ctx.running.get()) {
      try {
        byte[] msgBytes = comms.waitPeerNotification(clientPeerName);
        if (msgBytes == null) {
          if (Thread.currentThread().isInterrupted()) {
            break;
          }
          continue;
        }

        String msg = new String(msgBytes, java.nio.charset.StandardCharsets.UTF_8);
        logger.debug("Received OOB notification from client {}: {}", clientPeerName, msg);

        if (msg.startsWith("FETCH_REQUEST:")) {
          // Format: FETCH_REQUEST:streamId:chunkIndex:slotOffset
          String[] parts = msg.split(":");
          long streamId = Long.parseLong(parts[1]);
          int chunkIndex = Integer.parseInt(parts[2]);
          int slotOffset = Integer.parseInt(parts[3]);
          
          fetchExecutor.submit(() -> {
            logger.info("handleFetchChunkRequest: Task started executing for {}_{} from client {}", streamId, chunkIndex, clientPeerName);
            handleFetchChunkRequest(ctx, streamId, chunkIndex, slotOffset); // Pass slotOffset directly
          });
        } else if (msg.startsWith("CHUNK_DONE:")) {
          // Format: CHUNK_DONE:serverOffset:streamId:chunkIndex
          String[] parts = msg.split(":");
          int serverOffset = Integer.parseInt(parts[1]);
          
          ctx.freeSlots.offer(serverOffset);
          logger.debug("pollClientNotifications: Client {} released slot at offset {}. Free slots: {}", 
              clientPeerName, serverOffset, ctx.freeSlots.size());

          if (parts.length >= 4) {
             long streamId = Long.parseLong(parts[2]);
             int chunkIndex = Integer.parseInt(parts[3]);
             try {
                comms.notify(clientPeerName, "CLIENT_FETCH_SUCCESS:" + streamId + ":" + chunkIndex);
             } catch (Exception ne) {
                logger.error("Failed to bounce CLIENT_FETCH_SUCCESS back to client", ne);
             }
          }
        } else if (msg.startsWith("PUSH_DATA:")) {
          // Format: PUSH_DATA:slotOffset:length:shuffleKey:partitionUniqueId
          String[] parts = msg.split(":");
          int slotOffset = Integer.parseInt(parts[1]);
          int length = Integer.parseInt(parts[2]);
          String shuffleKey = parts[3];
          String partitionUniqueId = parts[4];
          
          fetchExecutor.submit(() -> {
            logger.debug("handlePushDataRequest: Task started executing for slot {} from client {}", slotOffset, clientPeerName);
            handlePushDataRequest(ctx, slotOffset, length, shuffleKey, partitionUniqueId);
          });
        } else if (msg.startsWith("PUSH_MERGED_DATA:")) {
          // Format: PUSH_MERGED_DATA:slotOffset:length:shuffleKey:partitionIdsString;offsetsString
          String[] parts = msg.split(":", 5);
          int slotOffset = Integer.parseInt(parts[1]);
          int length = Integer.parseInt(parts[2]);
          String shuffleKey = parts[3];
          String payload = parts[4];
          
          String[] subParts = payload.split(";");
          String[] partitionUniqueIds = subParts[0].split(",");
          int[] offsets = java.util.Arrays.stream(subParts[1].split(","))
              .mapToInt(Integer::parseInt)
              .toArray();
          
          logger.debug("pollClientNotifications: Queuing merged push request for slot {} from client {} in fetchExecutor.", slotOffset, clientPeerName);
          fetchExecutor.submit(() -> {
            logger.debug("handlePushMergedDataRequest: Task started executing for slot {} from client {}", slotOffset, clientPeerName);
            handlePushMergedDataRequest(ctx, slotOffset, length, shuffleKey, partitionUniqueIds, offsets);
          });
        }


      } catch (Throwable t) {
        logger.error("FATAL: Error in poller thread for client {}", clientPeerName, t);
        break;
      }
    }
    logger.info("RDMA Server Poller thread stopped for client: {}", clientPeerName);
  }

  private void handlePushDataRequest(ClientContext ctx, int slotOffset, int length, String shuffleKey, String partitionUniqueId) {
    if (chunkPushHandler == null) {
      logger.error("handlePushDataRequest: Cannot process PUSH_DATA, ChunkPushHandler is not registered!");
      sendPushFailed(ctx, slotOffset, "ChunkPushHandler not registered");
      return;
    }

    final java.util.concurrent.atomic.AtomicBoolean completed = new java.util.concurrent.atomic.AtomicBoolean(false);
    ByteBuf body = null;
    try {
      // 1. Slice the client's buffer at the slot offset containing the pushed data
      ByteBuffer duplicate = ctx.localBuffer.duplicate();
      duplicate.clear();
      duplicate.position(slotOffset);
      duplicate.limit(slotOffset + length);
      ByteBuffer slice = duplicate.slice();

      // 2. Allocate a new pooled direct buffer and copy the data to isolate it from the RDMA slot
      body = io.netty.buffer.PooledByteBufAllocator.DEFAULT.directBuffer(length);
      body.writeBytes(slice);

      // 3. Forward to the registered handler (which writes it to disk/replica)
      chunkPushHandler.pushData(shuffleKey, partitionUniqueId, body, new RpcResponseCallback() {
        @Override
        public void onSuccess(ByteBuffer response) {
          logger.debug("handlePushDataRequest: Push queued successfully for slot {} from client {}. Sending PUSH_COMPLETE immediately.", slotOffset, ctx.peerName);
          if (completed.compareAndSet(false, true)) {
            byte statusCode = 0;
            if (response != null && response.remaining() > 0) {
              statusCode = response.get(response.position());
            }
            String reply = "PUSH_COMPLETE:" + slotOffset + ":" + statusCode;
            try {
              comms.notify(ctx.peerName, reply);
            } catch (Exception ne) {
              logger.error("Server: Failed to send PUSH_COMPLETE to {}", ctx.peerName, ne);
            }
          }
        }

        @Override
        public void onFailure(Throwable e) {
          logger.error("handlePushDataRequest: Push failed for slot {} from client {}", slotOffset, ctx.peerName, e);
          if (completed.compareAndSet(false, true)) {
            sendPushFailed(ctx, slotOffset, e.getMessage());
          }
        }
      });

      // 4. CRITICAL: Release our initial reference now that the handler has retained it!
      body.release();

    } catch (Throwable t) {
      logger.error("handlePushDataRequest: Error processing push for slot {} from client {}", slotOffset, ctx.peerName, t);
      if (completed.compareAndSet(false, true)) {
        sendPushFailed(ctx, slotOffset, t.getMessage());
      }
      if (body != null && body.refCnt() > 0) {
        body.release();
      }
    }
  }

  private void handlePushMergedDataRequest(ClientContext ctx, int slotOffset, int length, String shuffleKey, String[] partitionUniqueIds, int[] offsets) {
    long t0_tracker = rdmaTrackerEnabled ? System.nanoTime() : 0;
    try {
    if (chunkPushHandler == null) {
      logger.error("handlePushMergedDataRequest: Cannot process PUSH_MERGED_DATA, ChunkPushHandler is not registered!");
      sendPushFailed(ctx, slotOffset, "ChunkPushHandler not registered");
      return;
    }

    ByteBuf body = null;
    try {
      // 1. Slice the client's buffer at the slot offset containing the pushed data (lock-free!)
      ByteBuffer duplicate = ctx.localBuffer.duplicate();
      duplicate.clear();
      duplicate.position(slotOffset);
      duplicate.limit(slotOffset + length);
      ByteBuffer slice = duplicate.slice();

      // 2. Allocate a new pooled direct buffer and copy the data to isolate it from the RDMA slot
      body = io.netty.buffer.PooledByteBufAllocator.DEFAULT.directBuffer(length);
      body.writeBytes(slice);

      // 3. Forward to the registered handler
      chunkPushHandler.pushMergedData(shuffleKey, partitionUniqueIds, offsets, body, new RpcResponseCallback() {
        @Override
        public void onSuccess(ByteBuffer response) {
          logger.debug("handlePushMergedDataRequest: Push merged successfully processed for slot {} from client {}. Sending PUSH_COMPLETE immediately.", slotOffset, ctx.peerName);
          byte statusCode = 0;
          if (response != null && response.remaining() > 0) {
            statusCode = response.get(response.position());
          }
          String reply = "PUSH_COMPLETE:" + slotOffset + ":" + statusCode;
          try {
            comms.notify(ctx.peerName, reply);
          } catch (Exception ne) {
            logger.error("Server: Failed to send PUSH_COMPLETE to {}", ctx.peerName, ne);
          }
        }

        @Override
        public void onFailure(Throwable e) {
          logger.error("handlePushMergedDataRequest: Push merged failed for slot {} from client {}", slotOffset, ctx.peerName, e);
          sendPushFailed(ctx, slotOffset, e.getMessage());
        }
      });

      // Release our initial reference now that the handler has retained it!
      body.release();

    } catch (Throwable t) {
      logger.error("handlePushMergedDataRequest: Error processing push for slot {} from client {}", slotOffset, ctx.peerName, t);
      sendPushFailed(ctx, slotOffset, t.getMessage());
      if (body != null && body.refCnt() > 0) {
        body.release();
      }
    }
    } finally {
      if (rdmaTrackerEnabled) {
        rdma_comms.RDMATracker.record(rdma_comms.RDMATracker.CallType.SERVER_PUSH_MERGED_DATA_REQ, System.nanoTime() - t0_tracker);
      }
    }
  }

  private void sendPushFailed(ClientContext ctx, int slotOffset, String errorMsg) {
    String safeError = errorMsg != null ? errorMsg.replace('\n', ' ') : "Unknown error";
    String reply = "PUSH_FAILED:" + slotOffset + ":" + safeError;
    try {
      comms.notify(ctx.peerName, reply);
    } catch (Exception ne) {
      logger.error("sendPushFailed: Failed to send PUSH_FAILED reply to {}", ctx.peerName, ne);
    }
  }

  /**
   * Processes FETCH_CHUNK by borrowing a slot, reading data via ChunkFetchHandler,
   * and notifying the client.
   */
  private void handleFetchChunkRequest(ClientContext ctx, long streamId, int chunkIndex, int slotOffset) {
    long t0_tracker = rdmaTrackerEnabled ? System.nanoTime() : 0;
    try {
      if (chunkFetchHandler == null) {
        logger.error("handleFetchChunkRequest: Cannot process FETCH_CHUNK, ChunkFetchHandler is not registered!");
        return;
      }
      
      ByteBuffer target;
      synchronized (ctx.localBuffer) {
        ctx.localBuffer.clear();
        ctx.localBuffer.position(slotOffset);
        ctx.localBuffer.limit(slotOffset + this.slotSize);
        target = ctx.localBuffer.slice();
      }

      int length = chunkFetchHandler.fetchChunk(streamId, chunkIndex, target);
      
      // Instantly notify client
      String msg = "CHUNK_READY:" + streamId + ":" + chunkIndex + ":" + length + ":" + slotOffset;
      comms.notify(ctx.peerName, msg);
      
    } catch (Exception e) {
      logger.error("handleFetchChunkRequest: Failed for {}_{}", streamId, chunkIndex, e);
      try {
        comms.notify(ctx.peerName, "CHUNK_FAILED:" + streamId + ":" + chunkIndex + ":" + e.getMessage());
      } catch (Exception ne) {}
    } finally {
      if (rdmaTrackerEnabled) {
        rdma_comms.RDMATracker.record(rdma_comms.RDMATracker.CallType.SERVER_FETCH_CHUNK_REQ, System.nanoTime() - t0_tracker);
      }
    }
  }

  /**
   * Call this when the server shuts down to release the registered memory
   * and close resources.
   */
  public void shutdown() {
    running.set(false);
    if (fetchExecutor != null) {
      fetchExecutor.shutdownNow();
    }
    if (registerExecutor != null) {
      registerExecutor.shutdownNow();
    }
    if (listenSock != null) {
      try {
        listenSock.close();
        logger.info("OOB ServerSocket closed.");
      } catch (Exception e) {
        logger.error("Failed to close OOB ServerSocket", e);
      }
    }

    // Stop all client poller and scheduler threads, and deregister client memory
    for (ClientContext ctx : clientContexts.values()) {
      ctx.running.set(false);
      if (ctx.pollerThread != null) {
        ctx.pollerThread.interrupt();
      }

      if (ctx.localToken != null && ctx.localBuffer != null) {
      try {
          comms.deregAndFreeMem(ctx.localBuffer, ctx.localToken);
          logger.info("Deregistered and freed memory pool for client: {}", ctx.peerName);
      } catch (Exception e) {
          logger.error("Failed to deregister memory for client {}", ctx.peerName, e);
        }
      }
    }
    clientContexts.clear();

    if (comms != null) {
      try {
        comms.close();
        logger.info("Comms closed.");
        comms = null;
      } catch (Exception e) {
        logger.error("Failed to close Comms", e);
      }
    }
  }

  // Getters for external access
  public CommsWrapper getComms() {
    return comms;
  }

  public String getClientPeerName(String clientIp) {
    return clientPeerNames.get(clientIp);
  }

  // -------------------------------------------------------------------------
  // Helper Classes
  // -------------------------------------------------------------------------

  private static class ClientContext {
    final String peerName;
    final ByteBuffer localBuffer;
    final CommsWrapper.MemToken localToken;
    final BlockingQueue<Integer> freeSlots;
    Thread pollerThread;
    final java.util.concurrent.atomic.AtomicBoolean running = new java.util.concurrent.atomic.AtomicBoolean(true);

    ClientContext(String peerName, ByteBuffer localBuffer, CommsWrapper.MemToken localToken, 
                  BlockingQueue<Integer> freeSlots, Object dummyReadyQueue) {
      this.peerName = peerName;
      this.localBuffer = localBuffer;
      this.localToken = localToken;
      this.freeSlots = freeSlots;
    }
  }
}
