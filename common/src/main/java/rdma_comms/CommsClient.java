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
              conf.rdmaRemoteIp(),
              conf.rdmaOobPort(),
              conf.rdmaRemotePeerName(),
              slotSize,
              conf.rdmaPushSlotsCount(),
              conf.rdmaFetchSlotsCount(),
              (int) conf.rdmaPushSlotSize(),
              (int) conf.rdmaFetchSlotSize()
          );
          client.setup();
          _instance = client;
          logger.info("EAGER BOOT GATING: JNI CommsClient setup successful and registered!");
        }
      }
    }
    return _instance;
  }
  private static final Logger logger = LoggerFactory.getLogger(CommsClient.class);

  private final String transportType;
  private final String localPeerName;
  private final String serverIp;
  private final int oobPort;
  private final String serverPeerName;
  private final int slotSize;
  private final int pushSlotsCount;
  private final int fetchSlotsCount;
  private final int pushSlotSize;
  private final int fetchSlotSize;

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
  public CommsClient(String transportType, String localPeerName, String serverIp, 
                int oobPort, String serverPeerName, int slotSize,
                int pushSlotsCount, int fetchSlotsCount, int pushSlotSize, int fetchSlotSize) {
    this.transportType = transportType;
    this.localPeerName = localPeerName;
    this.serverIp = serverIp;
    this.oobPort = oobPort;
    this.serverPeerName = serverPeerName;
    this.slotSize = slotSize;
    this.pushSlotsCount = pushSlotsCount;
    this.fetchSlotsCount = fetchSlotsCount;
    this.pushSlotSize = pushSlotSize;
    this.fetchSlotSize = fetchSlotSize == 0 ? slotSize : fetchSlotSize;
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
      params.put("AP_BOOTSTRAP_PORT", "0");

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
        logger.info("Added remote endpoint and connected.");

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
      logger.info("fetchChunk: Acquired fetch slot offset {} for chunk {}_{} immediately. Free fetch slots: {}, Queue size: {}", 
          slot, streamId, chunkIndex, fetchFreeSlots.size(), waitingQueue.size());
      dispatchFetch(streamId, chunkIndex, callback, slot);
    } else {
      logger.info("fetchChunk: No fetch slots available for chunk {}_{}. Queueing request. Free fetch slots: 0, Queue size: {}", 
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

    logger.info("pushData: Acquired push slot offset {} for push to {} partition {}. Free push slots: {}, Queue size: {}", 
        slot, shuffleKey, partitionUniqueId, pushFreeSlots.size(), waitingQueue.size());
    dispatchPush(body, shuffleKey, partitionUniqueId, callback, slot);
  }

  private void dispatchPush(byte[] body, String shuffleKey, String partitionUniqueId, RpcResponseCallback callback, int slot) {
    transferExecutor.submit(() -> {
      try {
        // 1. Copy body bytes into localBuffer at slot offset using duplicate to avoid locking
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
            do {
              status = req.getStatus();
              if (status.state == CommsWrapper.State.InProgress) {
                if (spinCount < 100) {
                  Thread.onSpinWait();
                  spinCount++;
                } else {
                  Thread.yield();
                }
              }
            } while (status.state == CommsWrapper.State.InProgress && running.get());

            long duration = System.currentTimeMillis() - startTime;
            if (status.state != CommsWrapper.State.Done) {
              throw new IOException("RDMA Write failed with state: " + status.state + " after " + duration + "ms");
            }
            logger.info("dispatchPush: RDMA Write complete for slot {} (len: {}) in {}ms.", slot, length, duration);
          }
        }

        // 4. Send OOB notification to server
        String msg = "PUSH_DATA:" + slot + ":" + length + ":" + shuffleKey + ":" + partitionUniqueId;
        logger.info("dispatchPush: Sending PUSH_DATA OOB for slot {} to server.", slot);
        comms.notify(serverPeerName, msg);

      } catch (Exception e) {
        logger.error("dispatchPush: Failed for slot {}", slot, e);
        pendingPushes.remove(slot);
        callback.onFailure(e);
        releaseSlot(slot);
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

    logger.info("pushMergedData: Acquired push slot offset {} for merged push to {}. Partitions: {}, Free push slots: {}, Queue size: {}", 
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
            do {
              status = req.getStatus();
              if (status.state == CommsWrapper.State.InProgress) {
                if (spinCount < 100) {
                  Thread.onSpinWait();
                  spinCount++;
                } else {
                  Thread.yield();
                }
              }
            } while (status.state == CommsWrapper.State.InProgress && running.get());

            long duration = System.currentTimeMillis() - startTime;
            if (status.state != CommsWrapper.State.Done) {
              throw new IOException("RDMA Write (Merged) failed with state: " + status.state + " after " + duration + "ms");
            }
            logger.info("dispatchPushMerged: RDMA Write complete for slot {} (len: {}) in {}ms.", slot, length, duration);
          }
        }

        // 4. Serialize partition IDs and offsets arrays
        String partitionIdsStr = String.join(",", partitionUniqueIds);
        String offsetsStr = java.util.Arrays.stream(offsets)
            .mapToObj(String::valueOf)
            .collect(java.util.stream.Collectors.joining(","));

        // 5. Send PUSH_MERGED_DATA OOB notification to server
        String msg = "PUSH_MERGED_DATA:" + slot + ":" + length + ":" + shuffleKey + ":" + partitionIdsStr + ";" + offsetsStr;
        logger.info("dispatchPushMerged: Sending PUSH_MERGED_DATA OOB for slot {} to server.", slot);
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
    transferExecutor.submit(() -> {
      try {
        FetchTask task = new FetchTask(streamId, chunkIndex, callback, slot);
        String key = streamId + "_" + chunkIndex;
        pendingTasks.put(key, task);

        // Send OOB notification to server to prepare the chunk
        String msg = "FETCH_CHUNK:" + streamId + ":" + chunkIndex;
        logger.info("dispatchFetch: Sending FETCH_CHUNK OOB for {}_{} with slot offset {}. Pending tasks count: {}", 
            streamId, chunkIndex, slot, pendingTasks.size());
        comms.notify(serverPeerName, msg);
      } catch (Exception e) {
        logger.error("dispatchFetch: Failed to dispatch fetch for {}_{}", streamId, chunkIndex, e);
        callback.onFailure(chunkIndex, e);
        releaseSlot(slot);
      }
    });
  }

  private void releaseSlot(int slot) {
    if (slot < this.pushBoundary) {
      pushFreeSlots.offer(slot);
      logger.info("releaseSlot: Returned push slot offset {} to pool. Free push slots: {}", 
          slot, pushFreeSlots.size());
    } else {
      FetchRequest nextReq = waitingQueue.poll();
      if (nextReq != null) {
        logger.info("releaseSlot: Reusing released fetch slot offset {} for queued request {}_{}. Remaining in queue: {}", 
            slot, nextReq.streamId, nextReq.chunkIndex, waitingQueue.size());
        dispatchFetch(nextReq.streamId, nextReq.chunkIndex, nextReq.callback, slot);
      } else {
        fetchFreeSlots.offer(slot);
        logger.info("releaseSlot: Returned fetch slot offset {} to pool. Free fetch slots: {}, Queue size: {}", 
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
        logger.info("Received OOB notification from server: {}", msg);

        if (msg.startsWith("CHUNK_READY:")) {
          // Format: CHUNK_READY:streamId:chunkIndex:length:serverOffset
          String[] parts = msg.split(":");
          long streamId = Long.parseLong(parts[1]);
          int chunkIndex = Integer.parseInt(parts[2]);
          int length = Integer.parseInt(parts[3]);
          long serverOffset = Long.parseLong(parts[4]);

          String key = streamId + "_" + chunkIndex;
          FetchTask task = pendingTasks.remove(key);
          if (task != null) {
            logger.info("pollNotifications: Dispatched chunk {}_{} (len: {}, serverOffset: {}, localOffset: {}) to transfer thread.", 
                streamId, chunkIndex, length, serverOffset, task.localOffset);
            transferExecutor.submit(() -> executeRdmaRead(task, length, serverOffset));
          } else {
            logger.warn("pollNotifications: Received CHUNK_READY for unknown task: {}_{}", streamId, chunkIndex);
            // We should notify server to free the slot immediately since we won't read it
            comms.notify(serverPeerName, "CHUNK_DONE:" + serverOffset);
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
            logger.info("pollNotifications: PUSH_COMPLETE received for slotOffset {}", slotOffset);
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
    
    logger.info("executeRdmaRead: Starting JNI postTransfer (Read) for chunk {}_{} (len: {}, serverOffset: {}, localOffset: {})...", 
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
        do {
          status = req.getStatus();
          if (status.state == CommsWrapper.State.InProgress) {
            if (spinCount < 100) {
              Thread.onSpinWait();
              spinCount++;
            } else {
              Thread.yield();
            }
          }
        } while (status.state == CommsWrapper.State.InProgress && running.get());

        long duration = System.currentTimeMillis() - startTime;
        if (status.state != CommsWrapper.State.Done) {
          throw new IOException("RDMA Read failed with state: " + status.state + " after " + duration + "ms");
        }
        logger.info("executeRdmaRead: JNI transfer complete for chunk {}_{} in {}ms.", task.streamId, task.chunkIndex, duration);
      }

      logger.info("executeRdmaRead: Completed processing for chunk {}_{}.", task.streamId, task.chunkIndex);

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
            logger.info("CustomRDMAByteBuf release hook: Triggered for slot offset {} for chunk {}_{}", 
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

}
