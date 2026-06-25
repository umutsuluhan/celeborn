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

  private ChunkFetchHandler chunkFetchHandler;
  private java.util.concurrent.ExecutorService fetchExecutor;

  /**
   * Constructor.
   */
  public CommsServer(String transportType, String localPeerName, String localIp, 
                     int bootstrapPort, int bufferSize, int oobPort, boolean testMode) {
    this.transportType = transportType;
    this.localPeerName = localPeerName;
    this.localIp = localIp;
    this.bootstrapPort = bootstrapPort;
    this.oobPort = oobPort;
    this.testMode = testMode;

    // bufferSize is the shuffle chunk size passed from Worker.scala.
    // Add 4MB headroom for record boundary overflow.
    this.slotSize = bufferSize + 4 * 1024 * 1024;
    
    // Read slots count from JVM system properties (set by Spark/Celeborn --conf)
    int slotsCount = Integer.parseInt(System.getProperty("celeborn.rdma.slots.count", "32"));
    this.numSlots = slotsCount;
    this.poolSize = slotsCount * this.slotSize;

    logger.info("CommsServer initialized with slotSize: {}MB, numSlots: {}, poolSize: {}MB (calculated from bufferSize: {}MB)", 
        slotSize / (1024 * 1024), numSlots, poolSize / (1024 * 1024), bufferSize / (1024 * 1024));
  }

  public void registerChunkFetchHandler(ChunkFetchHandler handler) {
    this.chunkFetchHandler = handler;
    logger.info("ChunkFetchHandler registered.");
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

              // Handle client registration in a separate thread to avoid blocking accept loop
              new Thread(() -> handleClientRegistration(clientSock, clientIp), "RDMA-Server-Register-" + clientIp).start();

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

      // Initialize Slot Pool
      BlockingQueue<Integer> freeSlots = new LinkedBlockingQueue<>(this.numSlots);
      for (int i = 0; i < this.numSlots; i++) {
        freeSlots.offer(i * this.slotSize);
      }

      // Start OOB notification poller thread for this client
      Thread poller = new Thread(() -> pollClientNotifications(clientPeerName), "RDMA-Server-Poller-" + clientPeerName);
      poller.setDaemon(true);
      
      ClientContext ctx = new ClientContext(clientPeerName, clientBuffer, clientToken, freeSlots, poller);
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

    while (running.get()) {
      try {
        byte[] msgBytes = comms.getPeerNotification(clientPeerName);
        if (msgBytes == null) {
          // getPeerNotification is non-blocking, sleep to avoid busy spin
          Thread.sleep(5);
          continue;
        }

        String msg = new String(msgBytes, java.nio.charset.StandardCharsets.UTF_8);
        logger.info("Received OOB notification from client {}: {}", clientPeerName, msg);

        if (msg.startsWith("FETCH_CHUNK:")) {
          // Format: FETCH_CHUNK:streamId:chunkIndex
          String[] parts = msg.split(":");
          long streamId = Long.parseLong(parts[1]);
          int chunkIndex = Integer.parseInt(parts[2]);
          String key = streamId + "_" + chunkIndex;

          logger.info("pollClientNotifications: Queuing fetch request for {} from client {} in fetchExecutor.", key, clientPeerName);
          fetchExecutor.submit(() -> {
            logger.info("handleFetchChunkRequest: Task started executing for {} from client {}", key, clientPeerName);
            handleFetchChunkRequest(ctx, streamId, chunkIndex);
          });

        } else if (msg.startsWith("CHUNK_DONE:")) {
          // Format: CHUNK_DONE:serverOffset
          String[] parts = msg.split(":");
          int serverOffset = Integer.parseInt(parts[1]);
          
          ctx.freeSlots.offer(serverOffset); // Free the slot for reuse
          logger.info("pollClientNotifications: Client {} released slot at offset {}. Free slots: {}", 
              clientPeerName, serverOffset, ctx.freeSlots.size());
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
      logger.info("handleFetchChunkRequest: Processing {}_{} for client {}. Free slots before borrow: {}", 
          streamId, chunkIndex, ctx.peerName, ctx.freeSlots.size());
      
      long startTime = System.currentTimeMillis();
      slotOffset = ctx.freeSlots.take(); // Blocking wait if all slots are full
      long borrowTime = System.currentTimeMillis() - startTime;
      
      logger.info("handleFetchChunkRequest: Borrowed slot offset {} for {}_{} in {}ms. Remaining free slots: {}", 
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
      
      logger.info("handleFetchChunkRequest: Disk fetch complete for {}_{} (len: {}) in {}ms.", 
          streamId, chunkIndex, length, readDuration);

      // Notify the client that the chunk is ready to be pulled via RDMA Read
      String reply = "CHUNK_READY:" + streamId + ":" + chunkIndex + ":" + length + ":" + slotOffset;
      logger.info("handleFetchChunkRequest: Sending CHUNK_READY OOB reply to {} for {}_{} (serverOffset: {})", 
          ctx.peerName, streamId, chunkIndex, slotOffset);
      comms.notify(ctx.peerName, reply);

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
    if (listenSock != null) {
      try {
        listenSock.close();
        logger.info("OOB ServerSocket closed.");
      } catch (Exception e) {
        logger.error("Failed to close OOB ServerSocket", e);
      }
    }

    // Stop all client poller threads and deregister client memory
    for (ClientContext ctx : clientContexts.values()) {
      if (ctx.pollerThread != null) {
        ctx.pollerThread.interrupt();
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

  // -------------------------------------------------------------------------
  // Helper Classes
  // -------------------------------------------------------------------------

  private static class ClientContext {
    final String peerName;
    final ByteBuffer localBuffer;
    final CommsWrapper.MemToken localToken;
    final BlockingQueue<Integer> freeSlots;
    final Thread pollerThread;

    ClientContext(String peerName, ByteBuffer localBuffer, CommsWrapper.MemToken localToken, 
                  BlockingQueue<Integer> freeSlots, Thread pollerThread) {
      this.peerName = peerName;
      this.localBuffer = localBuffer;
      this.localToken = localToken;
      this.freeSlots = freeSlots;
      this.pollerThread = pollerThread;
    }
  }
}
