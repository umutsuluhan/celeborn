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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.celeborn.common.network.buffer.ManagedBuffer;
import org.apache.celeborn.common.network.buffer.NettyManagedBuffer;
import org.apache.celeborn.common.network.client.ChunkReceivedCallback;
import org.apache.celeborn.common.network.client.RpcResponseCallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommsClient {
  private final Map<Integer, RpcResponseCallback> pendingPushes = new ConcurrentHashMap<>();
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
  private final Map<String, FetchTask> pendingTasks = new ConcurrentHashMap<>();
  private final java.util.Queue<FetchRequest> waitingQueue = new java.util.concurrent.ConcurrentLinkedQueue<>();

  // New fields for read batching
  private final BlockingQueue<FetchTask> fetchRequestQueue = new LinkedBlockingQueue<>();
  private Thread fetchRequestSchedulerThread;
  private final BlockingQueue<FetchTask> readQueue = new LinkedBlockingQueue<>();
  private Thread readSchedulerThread;
  
  // Threads & Executors
  private ExecutorService transferExecutor;
  private Thread pollerThread;
  private final AtomicBoolean running = new AtomicBoolean(false);

  // Batching fields
  private final BlockingQueue<PushRequest> pushQueue = new LinkedBlockingQueue<>();
  private Thread batchSchedulerThread;

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

        // 4. Allocate local buffer and register it (same size as server's buffer)
        this.localBuffer = ByteBuffer.allocateDirect((int) remoteSize);
        this.localToken = comms.regMem(localBuffer, remoteSize, CommsWrapper.MemoryType.Dram);
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

        // 6. Start Poller and Executor
        this.transferExecutor = Executors.newFixedThreadPool(8, r -> {
          Thread t = new Thread(r, "RDMA-Client-Transfer-Thread");
          t.setDaemon(true);
          return t;
        });
        
        this.running.set(true);
        this.pollerThread = new Thread(this::pollNotifications, "RDMA-Client-Poller");
        this.pollerThread.setDaemon(true);
        this.pollerThread.start();

        this.batchSchedulerThread = new Thread(this::runBatchScheduler, "RDMA-Client-Batch-Scheduler");
        this.batchSchedulerThread.setDaemon(true);
        this.batchSchedulerThread.start();

        this.fetchRequestSchedulerThread = new Thread(this::runFetchRequestScheduler, "RDMA-Client-Fetch-Request-Scheduler");
        this.fetchRequestSchedulerThread.setDaemon(true);
        this.fetchRequestSchedulerThread.start();

        this.readSchedulerThread = new Thread(this::runReadScheduler, "RDMA-Client-Read-Scheduler");
        this.readSchedulerThread.setDaemon(true);
        this.readSchedulerThread.start();

        logger.info("Control plane setup complete. Ready for RDMA transfer.");
      }
    } catch (Exception e) {
      logger.error("Control setup failed", e);
      shutdown();
    }
  }

  /**
   * Asynchronously fetches a chunk via RDMA.
   */
  public void fetchChunk(long streamId, int chunkIndex, ChunkReceivedCallback callback) {
    if (!running.get()) {
      callback.onFailure(chunkIndex, new IllegalStateException("CommsClient is not running"));
      return;
    }

    Integer slot = fetchFreeSlots.poll();
    if (slot != null) {
      logger.debug("fetchChunk: Acquired fetch slot offset {} for chunk {}_{} immediately. Free fetch slots: {}, Queue size: {}", 
          slot, streamId, chunkIndex, fetchFreeSlots.size(), waitingQueue.size());
      dispatchFetch(streamId, chunkIndex, callback, slot);
    } else {
      logger.debug("fetchChunk: No fetch slots available for chunk {}_{}. Queueing request. Free fetch slots: 0, Queue size: {}", 
          streamId, chunkIndex, waitingQueue.size() + 1);
      waitingQueue.offer(new FetchRequest(streamId, chunkIndex, callback));
    }
  }

  /**
   * Asynchronously pushes data via RDMA.
   */
  public void pushData(byte[] body, String shuffleKey, String partitionUniqueId, RpcResponseCallback callback) {
    if (!running.get()) {
      callback.onFailure(new IllegalStateException("CommsClient is not running"));
      return;
    }

    Integer slot;
    try {
      slot = pushFreeSlots.take(); // Block until a push slot is available
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      callback.onFailure(e);
      return;
    }

    logger.debug("pushData: Acquired push slot offset {} for push to {} partition {}. Queueing for batching.", 
        slot, shuffleKey, partitionUniqueId);
    
    pushQueue.offer(new PushRequest(body, shuffleKey, partitionUniqueId, callback, slot));
  }

  private void runBatchScheduler() {
    logger.info("RDMA Client Batch Scheduler thread started. Target size: {}, Linger: {}ms", writeBatchSize, writeLingerMs);
    while (running.get()) {
      try {
        // Create a new list for each batch to avoid ConcurrentModificationException in async dispatch
        java.util.List<PushRequest> batch = new java.util.ArrayList<>();
        // Block until at least one request is available
        PushRequest first = pushQueue.take();
        batch.add(first);

        if (writeLingerMs > 0) {
          long startTime = System.currentTimeMillis();
          while (batch.size() < writeBatchSize && running.get()) {
            long elapsed = System.currentTimeMillis() - startTime;
            long remaining = writeLingerMs - elapsed;
            if (remaining <= 0) break;
            
            PushRequest next = pushQueue.poll(remaining, java.util.concurrent.TimeUnit.MILLISECONDS);
            if (next != null) {
              batch.add(next);
            } else {
              break;
            }
          }
        } else {
          pushQueue.drainTo(batch, writeBatchSize - 1);
        }

        if (!batch.isEmpty()) {
          dispatchBatch(batch);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Throwable t) {
        logger.error("Error in Batch Scheduler thread", t);
      }
    }
    logger.info("RDMA Client Batch Scheduler thread stopped.");
  }

  private void dispatchBatch(java.util.List<PushRequest> batch) {
    if (CommsWrapper.RDMA_TRACKER_ENABLED) {
      RDMATracker.recordBatchSize(RDMATracker.BatchType.CLIENT_PUSH, batch.size());
    }
    transferExecutor.submit(() -> {
      try {
        // 1. Copy all bodies into localBuffer at their respective slots
        for (PushRequest req : batch) {
          ByteBuffer duplicate = localBuffer.duplicate();
          duplicate.position(req.slot);
          duplicate.put(req.body);
          
          // Register callback
          pendingPushes.put(req.slot, req.callback);
        }

        // 2. Create IOVs with multiple segments
        try (CommsWrapper.TransferIov localIov = new CommsWrapper.TransferIov(false);
             CommsWrapper.TransferIov remoteIov = new CommsWrapper.TransferIov(true)) {
          
          for (PushRequest req : batch) {
            long localAddr = localBaseAddress + req.slot;
            long remoteAddr = remoteBaseAddress + req.slot;
            localIov.addSegment(localAddr, req.body.length, localToken);
            remoteIov.addSegment(remoteAddr, req.body.length, remoteToken);
          }

          // 3. Post SINGLE RDMA Write for the whole batch
          try (CommsWrapper.Request req = comms.postTransfer(serverPeerName, CommsWrapper.TransferOpType.Write, localIov, remoteIov, "")) {
            CommsWrapper.TransferStatus status;
            long startTime = System.currentTimeMillis();
            int spinCount = 0;
            while (req.isInProgress() && running.get()) {
              if (spinCount < 10) {
                Thread.onSpinWait();
                spinCount++;
              } else {
                java.util.concurrent.locks.LockSupport.parkNanos(25_000);
              }
            }
            status = req.getStatus();

            long duration = System.currentTimeMillis() - startTime;
            if (status.state != CommsWrapper.State.Done) {
              throw new IOException("RDMA Batch Write failed with state: " + status.state + " after " + duration + "ms");
            }
            logger.debug("dispatchBatch: RDMA Write complete for batch of size {} in {}ms.", batch.size(), duration);
          }
        }

        // 4. Send ONE OOB notification for the whole batch
        StringBuilder sb = new StringBuilder("BATCH_PUSH_DATA:");
        for (int i = 0; i < batch.size(); i++) {
          if (i > 0) sb.append(";");
          PushRequest req = batch.get(i);
          sb.append(req.slot).append(",")
            .append(req.body.length).append(",")
            .append(req.shuffleKey).append(",")
            .append(req.partitionUniqueId);
        }
        String msg = sb.toString();
        logger.info("dispatchBatch: Sending BATCH_PUSH_DATA OOB for batch of size {} to server.", batch.size());
        
        comms.notify(serverPeerName, msg);

      } catch (Exception e) {
        logger.error("dispatchBatch: Failed for batch", e);
        for (PushRequest req : batch) {
          pendingPushes.remove(req.slot);
          req.callback.onFailure(e);
          releaseSlot(req.slot);
        }
      }
    });
  }

  /**
   * Asynchronously pushes merged data via RDMA.
   */
  public void pushMergedData(byte[] body, String shuffleKey, String[] partitionUniqueIds, int[] offsets, RpcResponseCallback callback) {
    if (!running.get()) {
      callback.onFailure(new IllegalStateException("CommsClient is not running"));
      return;
    }

    Integer slot;
    try {
      slot = pushFreeSlots.take(); // Block until a push slot is available
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      callback.onFailure(e);
      return;
    }

    logger.debug("pushMergedData: Acquired push slot offset {} for merged push to {}. Partitions: {}, Free push slots: {}, Queue size: {}", 
        slot, shuffleKey, java.util.Arrays.toString(partitionUniqueIds), pushFreeSlots.size(), waitingQueue.size());
    dispatchPushMerged(body, shuffleKey, partitionUniqueIds, offsets, callback, slot);
  }

  private void dispatchPushMerged(byte[] body, String shuffleKey, String[] partitionUniqueIds, int[] offsets, RpcResponseCallback callback, int slot) {
    transferExecutor.submit(() -> {
      try {
        // 1. Copy body bytes into localBuffer at slot offset
        ByteBuffer duplicate = localBuffer.duplicate();
        duplicate.position(slot);
        duplicate.put(body);

        // 2. Register callback
        pendingPushes.put(slot, callback);

        // 3. Post RDMA Write
        long localAddr = localBaseAddress + slot;
        long remoteAddr = remoteBaseAddress + slot;
        int length = body.length;

        try (CommsWrapper.TransferIov localIov = new CommsWrapper.TransferIov(false);
             CommsWrapper.TransferIov remoteIov = new CommsWrapper.TransferIov(true)) {
          
          localIov.addSegment(localAddr, length, localToken);
          remoteIov.addSegment(remoteAddr, length, remoteToken);

          // Post RDMA Write
          try (CommsWrapper.Request req = comms.postTransfer(serverPeerName, CommsWrapper.TransferOpType.Write, localIov, remoteIov, "")) {
            CommsWrapper.TransferStatus status;
            long startTime = System.currentTimeMillis();
            int spinCount = 0;
            while (req.isInProgress() && running.get()) {
              if (spinCount < 10) {
                Thread.onSpinWait();
                spinCount++;
              } else {
                java.util.concurrent.locks.LockSupport.parkNanos(25_000);
              }
            }
            status = req.getStatus();

            long duration = System.currentTimeMillis() - startTime;
            if (status.state != CommsWrapper.State.Done) {
              throw new IOException("RDMA Write (Merged) failed with state: " + status.state + " after " + duration + "ms");
            }
            logger.debug("dispatchPushMerged: RDMA Write complete for slot {} (len: {}) in {}ms.", slot, length, duration);
          }
        }

        // 4. Serialize partition IDs and offsets arrays
        String partitionIdsStr = String.join(",", partitionUniqueIds);
        String offsetsStr = java.util.Arrays.stream(offsets)
            .mapToObj(String::valueOf)
            .collect(java.util.stream.Collectors.joining(","));

        // 5. Send PUSH_MERGED_DATA OOB notification to server
        String msg = "PUSH_MERGED_DATA:" + slot + ":" + length + ":" + shuffleKey + ":" + partitionIdsStr + ";" + offsetsStr;
        logger.debug("dispatchPushMerged: Sending PUSH_MERGED_DATA OOB for slot {} to server.", slot);
        comms.notify(serverPeerName, msg);

      } catch (Exception e) {
        logger.error("dispatchPushMerged: Failed for slot {}", slot, e);
        pendingPushes.remove(slot);
        callback.onFailure(e);
        releaseSlot(slot);
      }
    });
  }

  private void dispatchFetch(long streamId, int chunkIndex, ChunkReceivedCallback callback, int slot) {
    try {
      FetchTask task = new FetchTask(streamId, chunkIndex, callback, slot);
      String key = streamId + "_" + chunkIndex;
      pendingTasks.put(key, task);

      fetchRequestQueue.offer(task);
      logger.info("dispatchFetch: Queued FETCH_CHUNK for {}_{} with slot offset {}. Pending tasks count: {}", 
          streamId, chunkIndex, slot, pendingTasks.size());
    } catch (Exception e) {
      logger.error("dispatchFetch: Failed to dispatch fetch for {}_{}", streamId, chunkIndex, e);
      callback.onFailure(chunkIndex, e);
      releaseSlot(slot);
    }
  }

  private void runFetchRequestScheduler() {
    logger.info("RDMA Client Fetch Request Scheduler thread started. Target size: {}, Linger: {}ms", readBatchSize, readLingerMs);
    while (running.get()) {
      try {
        java.util.List<FetchTask> batch = new java.util.ArrayList<>();
        FetchTask first = fetchRequestQueue.take();
        batch.add(first);

        if (readLingerMs > 0) {
          long startTime = System.currentTimeMillis();
          while (batch.size() < readBatchSize && running.get()) {
            long elapsed = System.currentTimeMillis() - startTime;
            long remaining = readLingerMs - elapsed;
            if (remaining <= 0) break;
            
            FetchTask next = fetchRequestQueue.poll(remaining, java.util.concurrent.TimeUnit.MILLISECONDS);
            if (next != null) {
              batch.add(next);
            } else {
              break;
            }
          }
        } else {
          fetchRequestQueue.drainTo(batch, readBatchSize - 1);
        }
        
        if (!batch.isEmpty()) {
          StringBuilder sb = new StringBuilder("BATCH_FETCH_CHUNK:");
          for (int i = 0; i < batch.size(); i++) {
            if (i > 0) sb.append(";");
            FetchTask task = batch.get(i);
            sb.append(task.streamId).append(",")
              .append(task.chunkIndex);
          }
          comms.notify(serverPeerName, sb.toString());
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        logger.error("Error in Fetch Request Scheduler thread", e);
      }
    }
    logger.info("RDMA Client Fetch Request Scheduler thread stopped.");
  }

  private void runReadScheduler() {
    logger.info("RDMA Client Read Scheduler thread started. Target size: {}, Linger: {}ms", readBatchSize, readLingerMs);
    while (running.get()) {
      try {
        java.util.List<FetchTask> batch = new java.util.ArrayList<>();
        FetchTask first = readQueue.take();
        batch.add(first);

        if (readLingerMs > 0) {
          long startTime = System.currentTimeMillis();
          while (batch.size() < readBatchSize && running.get()) {
            long elapsed = System.currentTimeMillis() - startTime;
            long remaining = readLingerMs - elapsed;
            if (remaining <= 0) break;
            
            FetchTask next = readQueue.poll(remaining, java.util.concurrent.TimeUnit.MILLISECONDS);
            if (next != null) {
              batch.add(next);
            } else {
              break;
            }
          }
        } else {
          readQueue.drainTo(batch, readBatchSize - 1);
        }
        
        if (!batch.isEmpty()) {
          dispatchBatchRead(batch);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        logger.error("Error in Read Scheduler thread", e);
      }
    }
    logger.info("RDMA Client Read Scheduler thread stopped.");
  }

  private void dispatchBatchRead(java.util.List<FetchTask> batch) {
    if (CommsWrapper.RDMA_TRACKER_ENABLED) {
      RDMATracker.recordBatchSize(RDMATracker.BatchType.CLIENT_FETCH, batch.size());
    }
    transferExecutor.submit(() -> {
      try {
        try (CommsWrapper.TransferIov localIov = new CommsWrapper.TransferIov(false);
             CommsWrapper.TransferIov remoteIov = new CommsWrapper.TransferIov(true)) {
          
          for (FetchTask task : batch) {
            long localAddr = localBaseAddress + task.localOffset;
            long remoteAddr = remoteBaseAddress + task.serverOffset;
            localIov.addSegment(localAddr, task.length, localToken);
            remoteIov.addSegment(remoteAddr, task.length, remoteToken);
          }
          
          try (CommsWrapper.Request req = comms.postTransfer(serverPeerName, CommsWrapper.TransferOpType.Read, localIov, remoteIov, "")) {
            CommsWrapper.TransferStatus status;
            long startTime = System.currentTimeMillis();
            int spinCount = 0;
            while (req.isInProgress() && running.get()) {
              if (spinCount < 10) {
                Thread.onSpinWait();
                spinCount++;
              } else {
                java.util.concurrent.locks.LockSupport.parkNanos(25_000);
              }
            }
            status = req.getStatus();

            long duration = System.currentTimeMillis() - startTime;
            if (status.state != CommsWrapper.State.Done) {
              throw new java.io.IOException("RDMA Batch Read failed with state: " + status.state + " after " + duration + "ms");
            }
            logger.info("dispatchBatchRead: RDMA Read complete for batch of size {} in {}ms.", batch.size(), duration);
          }
        }
        
        for (FetchTask task : batch) {
          ByteBuffer sliced;
          synchronized (localBuffer) {
            ByteBuffer duplicate = localBuffer.duplicate();
            duplicate.position(task.localOffset);
            duplicate.limit(task.localOffset + task.length);
            sliced = duplicate.slice();
          }
          
          ByteBuf customBuf = new CustomRDMAByteBuf(
              io.netty.buffer.UnpooledByteBufAllocator.DEFAULT,
              sliced,
              task.length,
              () -> {
                logger.info("CustomRDMAByteBuf release hook: Triggered for slot offset {} for chunk {}_{}", 
                    task.localOffset, task.streamId, task.chunkIndex);
                releaseSlot(task.localOffset);
                try {
                  comms.notify(serverPeerName, "CHUNK_DONE:" + task.serverOffset);
                } catch (Exception ne) {
                  logger.error("CustomRDMAByteBuf release hook: Failed to send CHUNK_DONE to server for {}_{}", 
                      task.streamId, task.chunkIndex, ne);
                }
              }
          );
          
          ManagedBuffer managedBuffer = new NettyManagedBuffer(customBuf);
          task.callback.onSuccess(task.chunkIndex, managedBuffer);
          managedBuffer.release();
        }
        
      } catch (Exception e) {
        logger.error("dispatchBatchRead: Failed for batch", e);
        for (FetchTask task : batch) {
          task.callback.onFailure(task.chunkIndex, e);
          releaseSlot(task.localOffset);
          try {
            comms.notify(serverPeerName, "CHUNK_DONE:" + task.serverOffset);
          } catch (Exception ne) {
            logger.error("dispatchBatchRead: Failed to send CHUNK_DONE on failure for {}_{}", task.streamId, task.chunkIndex, ne);
          }
        }
      }
    });
  }

  private void releaseSlot(int slot) {
    if (slot < this.pushBoundary) {
      pushFreeSlots.offer(slot);
      logger.debug("releaseSlot: Returned push slot offset {} to pool. Free push slots: {}", 
          slot, pushFreeSlots.size());
    } else {
      FetchRequest nextReq = waitingQueue.poll();
      if (nextReq != null) {
        logger.debug("releaseSlot: Reusing released fetch slot offset {} for queued request {}_{}. Remaining in queue: {}", 
            slot, nextReq.streamId, nextReq.chunkIndex, waitingQueue.size());
        dispatchFetch(nextReq.streamId, nextReq.chunkIndex, nextReq.callback, slot);
      } else {
        fetchFreeSlots.offer(slot);
        logger.debug("releaseSlot: Returned fetch slot offset {} to pool. Free fetch slots: {}, Queue size: {}", 
            slot, fetchFreeSlots.size(), waitingQueue.size());
      }
    }
  }

  /**
   * Background loop that polls for OOB notifications from the server.
   */
  private void pollNotifications() {
    logger.info("RDMA Client Poller thread started.");
    while (running.get()) {
      try {
        byte[] msgBytes = comms.getPeerNotification(serverPeerName);
        if (msgBytes == null) {
          // getPeerNotification is non-blocking in current JNI if no notifications,
          // so sleep briefly to prevent busy waiting.
          Thread.sleep(5);
          continue;
        }

        String msg = new String(msgBytes, java.nio.charset.StandardCharsets.UTF_8);
        logger.debug("Received OOB notification from server: {}", msg);

        if (msg.startsWith("BATCH_CHUNK_READY:")) {
          // Format: BATCH_CHUNK_READY:streamId1,chunk1,len1,offset1;streamId2,chunk2,len2,offset2;...
          String[] parts = msg.split(":", 2);
          if (parts.length < 2 || parts[1].isEmpty()) {
            logger.warn("pollNotifications: Received empty BATCH_CHUNK_READY");
            continue;
          }
          String[] items = parts[1].split(";");
          for (String item : items) {
            String[] fields = item.split(",");
            long streamId = Long.parseLong(fields[0]);
            int chunkIndex = Integer.parseInt(fields[1]);
            int length = Integer.parseInt(fields[2]);
            long serverOffset = Long.parseLong(fields[3]);

            String key = streamId + "_" + chunkIndex;
            FetchTask task = pendingTasks.remove(key);
            if (task != null) {
              task.length = length;
              task.serverOffset = serverOffset;
              readQueue.offer(task);
              logger.info("pollNotifications: Queued ready chunk {}_{} (len: {}, serverOffset: {}) for batch read.", 
                  streamId, chunkIndex, length, serverOffset);
            } else {
              logger.warn("pollNotifications: Received BATCH_CHUNK_READY for unknown task: {}_{}", streamId, chunkIndex);
              comms.notify(serverPeerName, "CHUNK_DONE:" + serverOffset);
            }
          }
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

          String key = streamId + "_" + chunkIndex;
          FetchTask task = pendingTasks.remove(key);
          if (task != null) {
            logger.error("Server failed to fetch chunk {}_{}: {}", streamId, chunkIndex, errorMsg);
            task.callback.onFailure(chunkIndex, new java.io.IOException("Server failed to fetch chunk: " + errorMsg));
            releaseSlot(task.localOffset);
          } else {
            logger.warn("Received CHUNK_FAILED for unknown task: {}_{}", streamId, chunkIndex);
          }
        } else if (msg.startsWith("PUSH_COMPLETE:")) {
          // Format: PUSH_COMPLETE:slotOffset
          String[] parts = msg.split(":");
          int slotOffset = Integer.parseInt(parts[1]);
          RpcResponseCallback callback = pendingPushes.remove(slotOffset);
          if (callback != null) {
            logger.debug("pollNotifications: PUSH_COMPLETE received for slotOffset {}", slotOffset);
            callback.onSuccess(ByteBuffer.wrap(new byte[] { 0 })); // SUCCESS status code (0)
          } else {
            logger.warn("pollNotifications: PUSH_COMPLETE received for unknown slotOffset {}", slotOffset);
          }
          releaseSlot(slotOffset);
        } else if (msg.startsWith("PUSH_FAILED:")) {
          // Format: PUSH_FAILED:slotOffset:errorMsg
          String[] parts = msg.split(":");
          int slotOffset = Integer.parseInt(parts[1]);
          String errorMsg = parts[2];
          RpcResponseCallback callback = pendingPushes.remove(slotOffset);
          if (callback != null) {
            logger.error("pollNotifications: PUSH_FAILED received for slotOffset {}: {}", slotOffset, errorMsg);
            callback.onFailure(new IOException("RDMA push failed: " + errorMsg));
          } else {
            logger.warn("pollNotifications: PUSH_FAILED received for unknown slotOffset {}", slotOffset);
          }
          releaseSlot(slotOffset);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Throwable t) {
        logger.error("FATAL: Error in OOB poller thread", t);
        break;
      }
    }
    logger.info("RDMA Client Poller thread stopped. Failing any pending tasks...");
    failPendingTasks(new IOException("Connection lost or poller thread stopped"));
  }

  private void failPendingTasks(Throwable cause) {
    logger.info("Failing all pending tasks. Count: {}", pendingTasks.size() + pendingPushes.size());
    for (Map.Entry<String, FetchTask> entry : pendingTasks.entrySet()) {
      FetchTask task = entry.getValue();
      try {
        task.callback.onFailure(task.chunkIndex, cause);
      } catch (Exception e) {
        logger.error("Failed to trigger onFailure callback for {}_{}", task.streamId, task.chunkIndex, e);
      }
      releaseSlot(task.localOffset);
    }
    pendingTasks.clear();

    for (Map.Entry<Integer, RpcResponseCallback> entry : pendingPushes.entrySet()) {
      int slot = entry.getKey();
      RpcResponseCallback callback = entry.getValue();
      try {
        callback.onFailure(cause);
      } catch (Exception e) {
        logger.error("Failed to trigger onFailure callback for push slot {}", slot, e);
      }
      releaseSlot(slot);
    }
    pendingPushes.clear();
  }

  /**
   * Executes the actual RDMA Read to pull the chunk from the server.
   */
  private void executeRdmaRead(FetchTask task, int length, long serverOffset) {
    long localAddr = localBaseAddress + task.localOffset;
    long remoteAddr = remoteBaseAddress + serverOffset;
    
    logger.debug("executeRdmaRead: Starting JNI postTransfer (Read) for chunk {}_{} (len: {}, serverOffset: {}, localOffset: {})...", 
        task.streamId, task.chunkIndex, length, serverOffset, task.localOffset);

    try (CommsWrapper.TransferIov localIov = new CommsWrapper.TransferIov(false);
         CommsWrapper.TransferIov remoteIov = new CommsWrapper.TransferIov(true)) {
      
      localIov.addSegment(localAddr, length, localToken);
      remoteIov.addSegment(remoteAddr, length, remoteToken);

      // Post RDMA Read
      try (CommsWrapper.Request req = comms.postTransfer(serverPeerName, CommsWrapper.TransferOpType.Read, localIov, remoteIov, "")) {
        CommsWrapper.TransferStatus status;
        long startTime = System.currentTimeMillis();
        int spinCount = 0;
        while (req.isInProgress() && running.get()) {
          if (spinCount < 10) {
            Thread.onSpinWait();
            spinCount++;
          } else {
            java.util.concurrent.locks.LockSupport.parkNanos(25_000);
          }
        }
        status = req.getStatus();

        long duration = System.currentTimeMillis() - startTime;
        if (status.state != CommsWrapper.State.Done) {
          throw new IOException("RDMA Read failed with state: " + status.state + " after " + duration + "ms");
        }
        logger.debug("executeRdmaRead: JNI transfer complete for chunk {}_{} in {}ms.", task.streamId, task.chunkIndex, duration);
      }

      logger.debug("executeRdmaRead: Completed processing for chunk {}_{}.", task.streamId, task.chunkIndex);

      // Slice the direct ByteBuffer for this slot
      ByteBuffer sliced;
      // No synchronization needed!
      ByteBuffer duplicate = localBuffer.duplicate();
      duplicate.position(task.localOffset);
      duplicate.limit(task.localOffset + length);
      sliced = duplicate.slice();

      // Wrap it with our custom delegator to intercept release()
      ByteBuf customBuf = new CustomRDMAByteBuf(
          io.netty.buffer.UnpooledByteBufAllocator.DEFAULT,
          sliced,
          length,
          () -> {
            logger.debug("CustomRDMAByteBuf release hook: Triggered for slot offset {} for chunk {}_{}", 
                task.localOffset, task.streamId, task.chunkIndex);
            releaseSlot(task.localOffset);
            // Notify server that we are done with its slot
            try {
              comms.notify(serverPeerName, "CHUNK_DONE:" + serverOffset);
            } catch (Exception ne) {
              logger.error("CustomRDMAByteBuf release hook: Failed to send CHUNK_DONE to server for {}_{}", 
                  task.streamId, task.chunkIndex, ne);
            }
          }
      );

      ManagedBuffer managedBuffer = new NettyManagedBuffer(customBuf);
      task.callback.onSuccess(task.chunkIndex, managedBuffer);
      managedBuffer.release();

    } catch (Exception e) {
      logger.error("executeRdmaRead: RDMA Read failed for chunk {}_{}", task.streamId, task.chunkIndex, e);
      task.callback.onFailure(task.chunkIndex, e);
      releaseSlot(task.localOffset);
      try {
        comms.notify(serverPeerName, "CHUNK_DONE:" + serverOffset);
      } catch (Exception ne) {
        logger.error("executeRdmaRead: Failed to send CHUNK_DONE on failure for {}_{}", task.streamId, task.chunkIndex, ne);
      }
    }
  }

  /**
   * Releases resources, memory registrations and threads.
   */
  public void shutdown() {
    running.set(false);
    if (batchSchedulerThread != null) {
      batchSchedulerThread.interrupt();
      try {
        batchSchedulerThread.join(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    if (fetchRequestSchedulerThread != null) {
      fetchRequestSchedulerThread.interrupt();
      try {
        fetchRequestSchedulerThread.join(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    if (readSchedulerThread != null) {
      readSchedulerThread.interrupt();
      try {
        readSchedulerThread.join(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    failPendingTasks(new IOException("CommsClient was shut down"));
    // Fail any remaining requests in pushQueue
    PushRequest req;
    while ((req = pushQueue.poll()) != null) {
      req.callback.onFailure(new IOException("CommsClient was shut down"));
      releaseSlot(req.slot);
    }
    // Fail any remaining requests in fetchRequestQueue and readQueue
    FetchTask fetchTask;
    while ((fetchTask = fetchRequestQueue.poll()) != null) {
      fetchTask.callback.onFailure(fetchTask.chunkIndex, new IOException("CommsClient was shut down"));
      releaseSlot(fetchTask.localOffset);
    }
    while ((fetchTask = readQueue.poll()) != null) {
      fetchTask.callback.onFailure(fetchTask.chunkIndex, new IOException("CommsClient was shut down"));
      releaseSlot(fetchTask.localOffset);
    }
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
    if (localToken != null) {
      try {
        comms.deregMem(localToken);
        logger.info("Local memory deregistered.");
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

  // Getters
  public CommsWrapper getComms() { return comms; }
  public ByteBuffer getLocalBuffer() { return localBuffer; }
  public long getRemoteSize() { return remoteSize; }

  // -------------------------------------------------------------------------
  // Helper Classes
  // -------------------------------------------------------------------------

  private static class FetchTask {
    final long streamId;
    final int chunkIndex;
    final ChunkReceivedCallback callback;
    final int localOffset;
    int length;
    long serverOffset;

    FetchTask(long streamId, int chunkIndex, ChunkReceivedCallback callback, int localOffset) {
      this.streamId = streamId;
      this.chunkIndex = chunkIndex;
      this.callback = callback;
      this.localOffset = localOffset;
    }
  }

  private static class FetchRequest {
    final long streamId;
    final int chunkIndex;
    final ChunkReceivedCallback callback;
    FetchRequest(long streamId, int chunkIndex, ChunkReceivedCallback callback) {
      this.streamId = streamId;
      this.chunkIndex = chunkIndex;
      this.callback = callback;
    }
  }

  private static class PushRequest {
    final byte[] body;
    final String shuffleKey;
    final String partitionUniqueId;
    final RpcResponseCallback callback;
    final int slot;

    PushRequest(byte[] body, String shuffleKey, String partitionUniqueId, RpcResponseCallback callback, int slot) {
      this.body = body;
      this.shuffleKey = shuffleKey;
      this.partitionUniqueId = partitionUniqueId;
      this.callback = callback;
      this.slot = slot;
    }
  }

}
