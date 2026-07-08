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

      this.registerExecutor = java.util.concurrent.Executors.newCachedThreadPool(r -> {
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

              // Handle client registration sequentially to avoid connection storms and interleaving race conditions
              handleClientRegistration(clientSock, clientIp);

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
  }

  /**
   * Handles client handshake, allocates client-specific buffer, registers memory,
   * and starts the client OOB poller thread.
   */
  private void handleClientRegistration(Socket clientSock, String clientIp) {
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
      logger.info("Allocating {}MB RDMA buffer pool for client: {}...", poolSize / (1024 * 1024), clientPeerName);
      ByteBuffer clientBuffer = ByteBuffer.allocateDirect(poolSize);
      // Touch memory pages to avoid page faults during RDMA transfers
      for (int i = 0; i < poolSize; i += 4096) {
        clientBuffer.put(i, (byte) 0);
      }

      // Register the memory pool
      CommsWrapper.MemToken clientToken = comms.regMem(clientBuffer, poolSize, CommsWrapper.MemoryType.Dram);
      logger.info("Registered {}MB DRAM buffer pool for client: {}", poolSize / (1024 * 1024), clientPeerName);

      // Initialize Slot Pool (only use fetch slots for fetching)
      int pushBoundary = this.pushSlotsCount * this.pushSlotSize;

      BlockingQueue<Integer> freeSlots = new LinkedBlockingQueue<>(this.fetchSlotsCount);
      for (int i = 0; i < this.fetchSlotsCount; i++) {
        freeSlots.offer(pushBoundary + i * this.fetchSlotSize);
      }

      // Start OOB notification poller thread and Ready Scheduler thread for this client
      BlockingQueue<ReadyChunk> readyQueue = new LinkedBlockingQueue<>();
      ClientContext ctx = new ClientContext(clientPeerName, clientBuffer, clientToken, freeSlots, readyQueue);
      
      Thread poller = new Thread(() -> pollClientNotifications(clientPeerName), "RDMA-Server-Poller-" + clientPeerName);
      poller.setDaemon(true);
      ctx.pollerThread = poller;

      Thread readyScheduler = new Thread(() -> runReadyScheduler(ctx), "RDMA-Server-Ready-Scheduler-" + clientPeerName);
      readyScheduler.setDaemon(true);
      ctx.readySchedulerThread = readyScheduler;
      
      clientContexts.put(clientPeerName, ctx);
      
      poller.start();
      readyScheduler.start();

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
        byte[] msgBytes = comms.getPeerNotification(clientPeerName);
        if (msgBytes == null) {
          // getPeerNotification is non-blocking, sleep to avoid busy spin
          Thread.sleep(5);
          continue;
        }

        String msg = new String(msgBytes, java.nio.charset.StandardCharsets.UTF_8);
        logger.debug("Received OOB notification from client {}: {}", clientPeerName, msg);

        if (msg.startsWith("BATCH_FETCH_CHUNK:")) {
          // Format: BATCH_FETCH_CHUNK:streamId1,chunk1;streamId2,chunk2;...
          String[] parts = msg.split(":", 2);
          if (parts.length < 2 || parts[1].isEmpty()) {
            logger.warn("pollClientNotifications: Received empty BATCH_FETCH_CHUNK from client {}", clientPeerName);
            continue;
          }
          String payload = parts[1];
          String[] items = payload.split(";");
          if (CommsWrapper.RDMA_TRACKER_ENABLED) {
            RDMATracker.recordBatchSize(RDMATracker.BatchType.SERVER_FETCH_REQUEST, items.length);
          }
          logger.info("pollClientNotifications: Queuing batch fetch request of size {} from client {} in fetchExecutor.", items.length, clientPeerName);
          
          for (String item : items) {
            String[] fields = item.split(",");
            long streamId = Long.parseLong(fields[0]);
            int chunkIndex = Integer.parseInt(fields[1]);
            
            fetchExecutor.submit(() -> {
              logger.info("handleFetchChunkRequest (Batch): Task started executing for {}_{} from client {}", streamId, chunkIndex, clientPeerName);
              handleFetchChunkRequest(ctx, streamId, chunkIndex);
            });
          }
        } else if (msg.startsWith("CHUNK_DONE:")) {
          // Format: CHUNK_DONE:serverOffset
          String[] parts = msg.split(":");
          int serverOffset = Integer.parseInt(parts[1]);
          
          ctx.freeSlots.offer(serverOffset); // Free the slot for reuse
          logger.debug("pollClientNotifications: Client {} released slot at offset {}. Free slots: {}", 
              clientPeerName, serverOffset, ctx.freeSlots.size());
        } else if (msg.startsWith("BATCH_PUSH_DATA:")) {
          // Format: BATCH_PUSH_DATA:slot1,len1,shuffleKey1,part1;slot2,len2,shuffleKey2,part2;...
          String[] parts = msg.split(":", 2);
          if (parts.length < 2 || parts[1].isEmpty()) {
            logger.warn("pollClientNotifications: Received empty BATCH_PUSH_DATA from client {}", clientPeerName);
            continue;
          }
          String payload = parts[1];
          String[] items = payload.split(";");
          if (CommsWrapper.RDMA_TRACKER_ENABLED) {
            RDMATracker.recordBatchSize(RDMATracker.BatchType.SERVER_PUSH_REQUEST, items.length);
          }
          logger.info("pollClientNotifications: Queuing batch push request of size {} from client {} in fetchExecutor.", items.length, clientPeerName);
          
          for (String item : items) {
            String[] fields = item.split(",");
            int slotOffset = Integer.parseInt(fields[0]);
            int length = Integer.parseInt(fields[1]);
            String shuffleKey = fields[2];
            String partitionUniqueId = fields[3];
            
            fetchExecutor.submit(() -> {
              logger.info("handlePushDataRequest (Batch): Task started executing for slot {} from client {}", slotOffset, clientPeerName);
              handlePushDataRequest(ctx, slotOffset, length, shuffleKey, partitionUniqueId);
            });
          }
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

      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
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
            String reply = "PUSH_COMPLETE:" + slotOffset;
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

    } catch (Exception e) {
      logger.error("handlePushDataRequest: Error processing push for slot {} from client {}", slotOffset, ctx.peerName, e);
      if (completed.compareAndSet(false, true)) {
        sendPushFailed(ctx, slotOffset, e.getMessage());
      }
      if (body != null && body.refCnt() > 0) {
        body.release();
      }
    }
  }

  private void handlePushMergedDataRequest(ClientContext ctx, int slotOffset, int length, String shuffleKey, String[] partitionUniqueIds, int[] offsets) {
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
          String reply = "PUSH_COMPLETE:" + slotOffset;
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

    } catch (Exception e) {
      logger.error("handlePushMergedDataRequest: Error processing push for slot {} from client {}", slotOffset, ctx.peerName, e);
      sendPushFailed(ctx, slotOffset, e.getMessage());
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
  private void handleFetchChunkRequest(ClientContext ctx, long streamId, int chunkIndex) {
    if (chunkFetchHandler == null) {
      logger.error("handleFetchChunkRequest: Cannot process FETCH_CHUNK, ChunkFetchHandler is not registered!");
      return;
    }

    int slotOffset = -1;
    try {
      logger.debug("handleFetchChunkRequest: Processing {}_{} for client {}. Free slots before borrow: {}", 
          streamId, chunkIndex, ctx.peerName, ctx.freeSlots.size());
      
      long startTime = System.currentTimeMillis();
      slotOffset = ctx.freeSlots.take(); // Blocking wait if all slots are full
      long borrowTime = System.currentTimeMillis() - startTime;
      
      logger.debug("handleFetchChunkRequest: Borrowed slot offset {} for {}_{} in {}ms. Remaining free slots: {}", 
          slotOffset, streamId, chunkIndex, borrowTime, ctx.freeSlots.size());

      // Slice the client's buffer at the slot offset to write the chunk data
      ByteBuffer target;
      synchronized (ctx.localBuffer) {
        ctx.localBuffer.clear();
        ctx.localBuffer.position(slotOffset);
        ctx.localBuffer.limit(slotOffset + this.slotSize);
        target = ctx.localBuffer.slice();
      }

      // Fetch the chunk from Celeborn Worker (disk I/O) into our RDMA slot
      long readStartTime = System.currentTimeMillis();
      int length = chunkFetchHandler.fetchChunk(streamId, chunkIndex, target);
      long readDuration = System.currentTimeMillis() - readStartTime;
      
      logger.debug("handleFetchChunkRequest: Disk fetch complete for {}_{} (len: {}) in {}ms.", 
          streamId, chunkIndex, length, readDuration);

      // Queue the ready chunk for batched notification
      ctx.readyQueue.offer(new ReadyChunk(streamId, chunkIndex, length, slotOffset));
      logger.info("handleFetchChunkRequest: Queued ready chunk {}_{} (len: {}, serverOffset: {}) for client {}", 
          streamId, chunkIndex, length, slotOffset, ctx.peerName);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("handleFetchChunkRequest: Fetch interrupted for {}_{}", streamId, chunkIndex, e);
      if (slotOffset != -1) {
        ctx.freeSlots.offer(slotOffset);
      }
      String reply = "CHUNK_FAILED:" + streamId + ":" + chunkIndex + ":Interrupted";
      try {
        comms.notify(ctx.peerName, reply);
      } catch (Exception ne) {
        logger.error("handleFetchChunkRequest: Failed to send CHUNK_FAILED to {}", ctx.peerName, ne);
      }
    } catch (Exception e) {
      logger.error("handleFetchChunkRequest: Failed to process fetch {}_{} for client {}", 
          streamId, chunkIndex, ctx.peerName, e);
      if (slotOffset != -1) {
        ctx.freeSlots.offer(slotOffset);
      }
      String errorMsg = e.getMessage() != null ? e.getMessage().replace('\n', ' ') : e.toString();
      String reply = "CHUNK_FAILED:" + streamId + ":" + chunkIndex + ":" + errorMsg;
      try {
        comms.notify(ctx.peerName, reply);
      } catch (Exception ne) {
        logger.error("Failed to send CHUNK_FAILED reply to {}", ctx.peerName, ne);
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
      if (ctx.readySchedulerThread != null) {
        ctx.readySchedulerThread.interrupt();
      }
      if (ctx.localToken != null) {
      try {
          comms.deregMem(ctx.localToken);
          logger.info("Deregistered memory pool for client: {}", ctx.peerName);
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

  private void runReadyScheduler(ClientContext ctx) {
    logger.info("RDMA Server Ready Scheduler thread started for client: {}", ctx.peerName);
    while (running.get() && ctx.running.get()) {
      try {
        java.util.List<ReadyChunk> batch = new java.util.ArrayList<>();
        ReadyChunk first = ctx.readyQueue.take();
        batch.add(first);
        ctx.readyQueue.drainTo(batch);
        
        if (!batch.isEmpty()) {
          if (CommsWrapper.RDMA_TRACKER_ENABLED) {
            RDMATracker.recordBatchSize(RDMATracker.BatchType.SERVER_CHUNK_READY, batch.size());
          }
          StringBuilder sb = new StringBuilder("BATCH_CHUNK_READY:");
          for (int i = 0; i < batch.size(); i++) {
            if (i > 0) sb.append(";");
            ReadyChunk rc = batch.get(i);
            sb.append(rc.streamId).append(",")
              .append(rc.chunkIndex).append(",")
              .append(rc.length).append(",")
              .append(rc.slotOffset);
          }
          comms.notify(ctx.peerName, sb.toString());
          logger.info("runReadyScheduler: Sent BATCH_CHUNK_READY of size {} to {}", batch.size(), ctx.peerName);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        logger.error("Error in Ready Scheduler for client {}", ctx.peerName, e);
      }
    }
    logger.info("RDMA Server Ready Scheduler thread stopped for client: {}", ctx.peerName);
  }

  // -------------------------------------------------------------------------
  // Helper Classes
  // -------------------------------------------------------------------------

  private static class ReadyChunk {
    final long streamId;
    final int chunkIndex;
    final int length;
    final int slotOffset;

    ReadyChunk(long streamId, int chunkIndex, int length, int slotOffset) {
      this.streamId = streamId;
      this.chunkIndex = chunkIndex;
      this.length = length;
      this.slotOffset = slotOffset;
    }
  }

  private static class ClientContext {
    final String peerName;
    final ByteBuffer localBuffer;
    final CommsWrapper.MemToken localToken;
    final BlockingQueue<Integer> freeSlots;
    Thread pollerThread;
    Thread readySchedulerThread;
    final BlockingQueue<ReadyChunk> readyQueue;
    final java.util.concurrent.atomic.AtomicBoolean running = new java.util.concurrent.atomic.AtomicBoolean(true);

    ClientContext(String peerName, ByteBuffer localBuffer, CommsWrapper.MemToken localToken, 
                  BlockingQueue<Integer> freeSlots, BlockingQueue<ReadyChunk> readyQueue) {
      this.peerName = peerName;
      this.localBuffer = localBuffer;
      this.localToken = localToken;
      this.freeSlots = freeSlots;
      this.readyQueue = readyQueue;
    }
  }
}
