package rdma_comms;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.celeborn.common.network.client.ChunkReceivedCallback;
import org.apache.celeborn.common.network.client.RpcResponseCallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommsClient {
  private static volatile CommsClient _instance = null;

  public static CommsClient getOrCreate(org.apache.celeborn.common.CelebornConf conf) {
    if (_instance == null) {
      synchronized (CommsClient.class) {
        if (_instance == null) {
          String baseName = conf.rdmaClientLocalPeerName();
          String uniqueClientName = baseName + "_" + java.util.UUID.randomUUID().toString().substring(0, 8);
          long chunkSize = conf.shuffleChunkSize();
          int slotSize = (int) chunkSize + 4 * 1024 * 1024; // 4MB headroom for record boundary overflow
          CommsClient client = new CommsClient(
              conf.rdmaTransport(),
              uniqueClientName,
              conf.rdmaLocalIp(),
              conf.rdmaRemoteIp(),
              conf.rdmaOobPort(),
              conf.rdmaRemotePeerName(),
              slotSize,
              conf.rdmaPushSlotsCount(),
              conf.rdmaFetchSlotsCount(),
              (int) conf.rdmaPushSlotSize(),
              (int) conf.rdmaFetchSlotSize(),
              conf.rdmaWriteBatchSize(),
              conf.rdmaWriteBatchLingerMs(),
              conf.rdmaReadBatchSize(),
              conf.rdmaReadBatchLingerMs()
          );
          client.rdmaTrackerEnabled = conf.rdmaTrackerEnabled();
          client.setup();
          _instance = client;
          logger.info("EAGER BOOT GATING: JNI CommsClient setup successful and registered!");
        }
      }
    }
    return _instance;
  }
  private static final Logger logger = LoggerFactory.getLogger(CommsClient.class);
  private boolean rdmaTrackerEnabled = true;

  private final String transportType;
  private final String localPeerName;
  private final String localIp;
  private final String serverIp;
  private final int oobPort;
  private final String serverPeerName;
  private final int slotSize;
  private final int pushSlotsCount;
  private final int fetchSlotsCount;
  private final int pushSlotSize;
  private final int fetchSlotSize;

  // Batching configurations
  private final int writeBatchSize;
  private final long writeLingerMs;
  private final int readBatchSize;
  private final long readLingerMs;

  // Persistent objects
  private CommsWrapper comms;
  private CommsWrapper.MemToken localToken;
  private CommsWrapper.MemToken remoteToken;
  private ByteBuffer localBuffer;

  private long remoteBaseAddress;
  private long remoteSize;
  private long localBaseAddress;

  // Slot Management
  private BlockingQueue<Integer> pushFreeSlots;
  private BlockingQueue<Integer> fetchFreeSlots;
  private int numSlots;
  private int pushBoundary;

  // Fetcher and Pusher components
  private RdmaFetcher fetcher;
  private RdmaPusher pusher;
  
  // Threads & Executors
  private ExecutorService transferExecutor;
  private Thread pollerThread;
  private final AtomicBoolean running = new AtomicBoolean(false);

  /**
   * Constructor.
   * @param transportType Transport to use: "TCP" or "RDMA".
   * @param localPeerName Local name for this client.
   * @param serverIp IP of the remote OOB server.
   * @param oobPort Port of the remote OOB server.
   * @param serverPeerName Name of the remote server.
   */
  public CommsClient(String transportType, String localPeerName, String localIp, String serverIp, 
                int oobPort, String serverPeerName, int slotSize,
                int pushSlotsCount, int fetchSlotsCount, int pushSlotSize, int fetchSlotSize,
                int writeBatchSize, long writeLingerMs, int readBatchSize, long readLingerMs) {
    this.transportType = transportType;
    this.localPeerName = localPeerName;
    this.localIp = (localIp == null || localIp.isEmpty())
        ? org.apache.celeborn.common.util.JavaUtils.getLocalHost()
        : localIp;
    this.serverIp = serverIp;
    this.oobPort = oobPort;
    this.serverPeerName = serverPeerName;
    this.slotSize = slotSize;
    this.pushSlotsCount = pushSlotsCount;
    this.fetchSlotsCount = fetchSlotsCount;
    this.pushSlotSize = pushSlotSize;
    this.fetchSlotSize = fetchSlotSize == 0 ? slotSize : fetchSlotSize;
    this.writeBatchSize = writeBatchSize;
    this.writeLingerMs = writeLingerMs;
    this.readBatchSize = readBatchSize;
    this.readLingerMs = readLingerMs;
  }

  /**
   * Sets up the CommsClient Control Plane:
   * 1. Initializes Comms library.
   * 2. Connects to OOB TCP server and receives server handles/tokens.
   * 3. Establishes transport connection via addRemoteEndpoint.
   * 4. Allocates and registers local buffer.
   * 5. Initializes Slot Pool and background threads.
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
        params.put("AP_BOOTSTRAP_IP", localIp);
        params.put("AP_BOOTSTRAP_PORT", "0");
        params.put("AP_RDMA_TRACKER_ENABLED", String.valueOf(rdmaTrackerEnabled));

        comms.init(params);
        logger.info("Comms library initialized.");

        // 2. Connect to OOB server and read handles
        logger.info("Connecting to OOB server {}:{}...", serverIp, oobPort);
        Socket socket = null;
        int retries = 0;
        int maxRetries = 10; // Retry 10 times with 500ms sleep = 5 seconds total
        while (retries < maxRetries) {
          try {
            socket = new Socket(serverIp, oobPort);
            break; // Success
          } catch (IOException e) {
            retries++;
            logger.warn("Connection failed, retrying in 500ms ({}/{})...", retries, maxRetries);
            try {
              Thread.sleep(500);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              throw new IOException("Connection loop interrupted", ie);
            }
          }
        }

        if (socket == null) {
          throw new IOException("Failed to connect to OOB server at " + serverIp + ":" + oobPort + " after " + maxRetries + " attempts");
        }

        try (Socket finalSocket = socket;
          DataOutputStream out = new DataOutputStream(finalSocket.getOutputStream());
          DataInputStream in = new DataInputStream(finalSocket.getInputStream())) {
          
          byte[] clientHandleOpaque = comms.getEndpointInfo();
          long clientHandleSize = clientHandleOpaque.length;

          logger.info("Connected. Sending client configuration...");
          logger.debug("Writing localPeerName: {}", localPeerName);
          out.writeUTF(localPeerName);
          logger.debug("Writing clientHandleSize: {}", clientHandleSize);
          out.writeLong(clientHandleSize);
          logger.debug("Writing clientHandleOpaque bytes (length: {})...", clientHandleOpaque.length);
          out.write(clientHandleOpaque);
          logger.debug("Flushing output stream...");
          out.flush();
          logger.debug("Config sent and flushed.");

          logger.debug("Receiving keys...");

          // Read server handle
          logger.debug("Reading server handle size (blocking readLong)...");
          long handleSize = in.readLong();
          logger.debug("Read server handle size: {}", handleSize);
          
          byte[] serverHandleOpaque = new byte[(int) handleSize];
          logger.debug("Reading server handle bytes (blocking readFully)...");
          in.readFully(serverHandleOpaque);
          logger.debug("Read server handle bytes successfully.");

          // Read memory info
          logger.debug("Reading remote memory address (blocking readLong)...");
          this.remoteBaseAddress = in.readLong();
          logger.debug("Read remote memory address: 0x{}", Long.toHexString(remoteBaseAddress));
          
          logger.debug("Reading remote memory size (blocking readLong)...");
          this.remoteSize = in.readLong();
          logger.debug("Read remote memory size: {}", remoteSize);
          
          byte[] remoteTokenOpaque = new byte[4096];
          logger.debug("Reading remote token bytes (blocking readFully)...");
          in.readFully(remoteTokenOpaque);
          logger.debug("Read remote token bytes successfully.");

          logger.info("Received all handles and memory metadata.");

          // 3. Connect to server transport layer
          comms.addRemoteEndpoint(serverPeerName, serverHandleOpaque, true);
          comms.connect(serverPeerName);
          logger.info("Added remote endpoint and connected. Confirming connection to OOB server...");
          out.writeByte(1);
          out.flush();

          // 4. Allocate local buffer and natively map it (same size as server's buffer)
          CommsWrapper.NativeBuffer nativeBuffer = comms.allocateAndRegMem(remoteSize, CommsWrapper.MemoryType.Dram);
          this.localBuffer = nativeBuffer.buffer;
          this.localToken = nativeBuffer.token;
          this.remoteToken = comms.getMemToken(remoteTokenOpaque);
          this.localBaseAddress = CommsWrapper.getDirectBufferAddress(localBuffer);

          // 5. Initialize Slot Pool (Partitioned push and fetch)
          this.pushBoundary = this.pushSlotsCount * this.pushSlotSize;
          this.numSlots = this.pushSlotsCount + this.fetchSlotsCount;

          this.pushFreeSlots = new LinkedBlockingQueue<>(this.pushSlotsCount);
          for (int i = 0; i < this.pushSlotsCount; i++) {
            pushFreeSlots.offer(i * this.pushSlotSize);
          }
          this.fetchFreeSlots = new LinkedBlockingQueue<>(this.fetchSlotsCount);
          for (int i = 0; i < this.fetchSlotsCount; i++) {
            fetchFreeSlots.offer(this.pushBoundary + i * this.fetchSlotSize);
          }
          logger.info("Initialized local RDMA buffer pool with {} push slots (size: {}KB) and {} fetch slots (size: {}MB). Push boundary: {}MB", 
              this.pushSlotsCount, this.pushSlotSize / 1024, this.fetchSlotsCount, this.fetchSlotSize / (1024 * 1024), this.pushBoundary / (1024 * 1024));

          // Set up Handlers
          this.fetcher = new RdmaFetcher(this, fetchFreeSlots);
          this.pusher = new RdmaPusher(this, pushFreeSlots);

          // 6. Start Poller and Executor
          this.transferExecutor = Executors.newFixedThreadPool(32, r -> {
            Thread t = new Thread(r, "RDMA-Client-Transfer-Thread");
            t.setDaemon(true);
            return t;
          });
          
          this.running.set(true);
          this.pollerThread = new Thread(this::pollNotifications, "RDMA-Client-Poller");
          this.pollerThread.setDaemon(true);
          this.pollerThread.start();

          logger.info("Control plane setup complete. Ready for RDMA transfer.");
        }
      } catch (Exception e) {
        logger.error("Control setup failed", e);
        shutdown();
      }
    } finally {
      if (rdmaTrackerEnabled) {
        rdma_comms.RDMATracker.record(rdma_comms.RDMATracker.CallType.CLIENT_SETUP, System.nanoTime() - t0_tracker);
      }
    }
  }

  /**
   * Asynchronously fetches a chunk via RDMA.
   */
  public void fetchChunk(long streamId, int chunkIndex, ChunkReceivedCallback callback) {
    if (fetcher != null) {
      fetcher.fetchChunk(streamId, chunkIndex, callback);
    } else {
      callback.onFailure(chunkIndex, new IllegalStateException("CommsClient is not setup"));
    }
  }

  /**
   * Asynchronously pushes data via RDMA.
   */
  public void pushData(byte[] body, String shuffleKey, String partitionUniqueId, RpcResponseCallback callback) {
    if (pusher != null) {
      pusher.pushData(body, shuffleKey, partitionUniqueId, callback);
    } else {
      callback.onFailure(new IllegalStateException("CommsClient is not setup"));
    }
  }

  public void pushMergedData(byte[] body, String shuffleKey, String[] partitionUniqueIds, int[] offsets, RpcResponseCallback callback) {
    if (pusher != null) {
      pusher.pushMergedData(body, shuffleKey, partitionUniqueIds, offsets, callback);
    } else {
      callback.onFailure(new IllegalStateException("CommsClient is not setup"));
    }
  }

  private void pollNotifications() {
    logger.info("RDMA Client Poller thread started.");
    while (running.get()) {
      try {
        byte[] msgBytes = comms.waitPeerNotification(serverPeerName);
        if (msgBytes == null) {
          continue;
        }

        String msg = new String(msgBytes, java.nio.charset.StandardCharsets.UTF_8);
        logger.debug("Received OOB notification from server: {}", msg);

        if (msg.startsWith("CHUNK_READY:")) {
          // Format: CHUNK_READY:streamId:chunkIndex:length:serverOffset
          String[] parts = msg.split(":");
          long streamId = Long.parseLong(parts[1]);
          int chunkIndex = Integer.parseInt(parts[2]);
          int length = Integer.parseInt(parts[3]);
          long serverOffset = Long.parseLong(parts[4]);

          fetcher.processChunkReady(streamId, chunkIndex, length, serverOffset);
        } else if (msg.startsWith("CHUNK_FAILED:")) {
          // Format: CHUNK_FAILED:streamId:chunkIndex:errorMsg
          String[] parts = msg.split(":");
          long streamId = Long.parseLong(parts[1]);
          int chunkIndex = Integer.parseInt(parts[2]);
          StringBuilder errorMsgBuilder = new StringBuilder();
          for (int idx = 3; idx < parts.length; idx++) {
            if (idx > 3) errorMsgBuilder.append(":");
            errorMsgBuilder.append(parts[idx]);
          }
          String errorMsg = errorMsgBuilder.toString();

          fetcher.processChunkFailed(streamId, chunkIndex, errorMsg);
        } else if (msg.startsWith("PUSH_COMPLETE:")) {
          // Format: PUSH_COMPLETE:slotOffset:statusCode
          String[] parts = msg.split(":");
          int slotOffset = Integer.parseInt(parts[1]);
          byte statusCode = 0;
          if (parts.length > 2) {
            statusCode = Byte.parseByte(parts[2]);
          }
          pusher.processPushComplete(slotOffset, statusCode);
        } else if (msg.startsWith("PUSH_FAILED:")) {
          // Format: PUSH_FAILED:slotOffset:errorMsg
          String[] parts = msg.split(":");
          int slotOffset = Integer.parseInt(parts[1]);
          String errorMsg = parts[2];
          pusher.processPushFailed(slotOffset, errorMsg);
        }

      } catch (Throwable t) {
        logger.error("FATAL: Error in OOB poller thread", t);
        break;
      }
    }
    logger.info("RDMA Client Poller thread stopped. Failing any pending tasks...");
    failPendingTasks(new IOException("Connection lost or poller thread stopped"));
  }

  private void failPendingTasks(Throwable cause) {
    logger.info("Failing all pending tasks.");
    if (fetcher != null) fetcher.failPendingTasks(cause);
    if (pusher != null) pusher.failPendingTasks(cause);
  }

  /**
   * Releases resources, memory registrations and threads.
   */
  public void shutdown() {
    running.set(false);
    failPendingTasks(new IOException("CommsClient was shut down"));
    if (pollerThread != null) {
      pollerThread.interrupt();
      try {
        pollerThread.join(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    if (transferExecutor != null) {
      transferExecutor.shutdownNow();
    }
    if (localToken != null && localBuffer != null) {
      try {
        comms.deregAndFreeMem(localBuffer, localToken);
        logger.info("Local JNI-mapped memory deregistered and freed.");
      } catch (Exception e) {
        logger.error("Failed to deregister local memory", e);
      }
      localToken = null;
    }
    if (remoteToken != null) {
      remoteToken.close();
      remoteToken = null;
    }
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

  // Package Private API for Sub-components
  void releaseSlot(int slotOffset) {
    if (slotOffset < pushBoundary) {
      if (pusher != null) pusher.releaseSlot(slotOffset);
    } else {
      if (fetcher != null) fetcher.releaseSlot(slotOffset);
    }
  }

  boolean isRunning() { return running.get(); }
  boolean isRdmaTrackerEnabled() { return rdmaTrackerEnabled; }
  ExecutorService getTransferExecutor() { return transferExecutor; }
  ByteBuffer getLocalBuffer() { return localBuffer; }
  long getLocalBaseAddress() { return localBaseAddress; }
  long getRemoteBaseAddress() { return remoteBaseAddress; }
  CommsWrapper.MemToken getLocalToken() { return localToken; }
  CommsWrapper.MemToken getRemoteToken() { return remoteToken; }
  CommsWrapper getComms() { return comms; }
  String getServerPeerName() { return serverPeerName; }
  public long getRemoteSize() { return remoteSize; } // Backwards compatibility if needed
}
