package rdma_comms;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

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
              (int) conf.rdmaFetchSlotSize()
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

  // Persistent objects
  private CommsWrapper comms;
  private CommsWrapper.MemToken localToken;
  private CommsWrapper.MemToken remoteToken;
  private ByteBuffer localBuffer;
  private long remoteSize;
  private final AtomicBoolean running = new AtomicBoolean(false);

  private static class FetchReq {
      long streamId; int chunkIndex; ChunkReceivedCallback cb;
      FetchReq(long s, int c, ChunkReceivedCallback cb) { this.streamId = s; this.chunkIndex = c; this.cb = cb; }
  }
  private static class PushReq {
      byte[] body; String shuffleKey; String pid; RpcResponseCallback cb;
      PushReq(byte[] b, String sk, String p, RpcResponseCallback c) { body = b; shuffleKey = sk; pid = p; cb = c; }
  }
  private static class PushMergedReq {
      byte[] body; String shuffleKey; String[] pids; int[] offsets; RpcResponseCallback cb;
      PushMergedReq(byte[] b, String sk, String[] p, int[] o, RpcResponseCallback c) { body = b; shuffleKey = sk; pids = p; offsets = o; cb = c; }
  }

  private final List<FetchReq> fetchBatch = new ArrayList<>(12);
  private final List<PushReq> pushBatch = new ArrayList<>(12);
  private final List<PushMergedReq> pushMergedBatch = new ArrayList<>(12);

  private final ReentrantLock lock = new ReentrantLock();
  private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "RDMA-Comms-Scheduler");
      t.setDaemon(true);
      return t;
  });
  private ScheduledFuture<?> currentTimer = null;

  private static final int MAX_BATCH = 12;

  public CommsClient(String transportType, String localPeerName, String localIp, String serverIp, 
                int oobPort, String serverPeerName, int slotSize,
                int pushSlotsCount, int fetchSlotsCount, int pushSlotSize, int fetchSlotSize) {
    this.transportType = transportType;
    this.localPeerName = localPeerName;
    this.localIp = (localIp == null || localIp.isEmpty()) ? org.apache.celeborn.common.util.JavaUtils.getLocalHost() : localIp;
    this.serverIp = serverIp;
    this.oobPort = oobPort;
    this.serverPeerName = serverPeerName;
    this.slotSize = slotSize;
    this.pushSlotsCount = pushSlotsCount;
    this.fetchSlotsCount = fetchSlotsCount;
    this.pushSlotSize = pushSlotSize;
    this.fetchSlotSize = fetchSlotSize == 0 ? slotSize : fetchSlotSize;
  }

  public void setup() {
    long t0_tracker = rdmaTrackerEnabled ? System.nanoTime() : 0;
    try {
      String transport = "RDMA".equalsIgnoreCase(transportType) ? "1" : "0";
      try {
        this.comms = new CommsWrapper();
        Map<String, String> params = new HashMap<>();
        params.put("AP_TRANSPORT", transport);
        params.put("AP_LOCAL_PEER_NAME", localPeerName);
        params.put("AP_BOOTSTRAP_IP", localIp);
        params.put("AP_BOOTSTRAP_PORT", "0");
        params.put("AP_RDMA_TRACKER_ENABLED", String.valueOf(rdmaTrackerEnabled));

        comms.init(params);
        logger.info("Comms library initialized.");

        logger.info("Connecting to OOB server {}:{}...", serverIp, oobPort);
        Socket socket = null;
        int retries = 0;
        int maxRetries = 10;
        while (retries < maxRetries) {
          try {
            socket = new Socket(serverIp, oobPort);
            break;
          } catch (IOException e) {
            retries++;
            logger.warn("Connection failed, retrying in 500ms ({}/{})...", retries, maxRetries);
            try { Thread.sleep(500); } catch (InterruptedException ie) { throw new IOException("Connection interrupted", ie); }
          }
        }
        if (socket == null) throw new IOException("Failed to connect to OOB server");

        try (Socket finalSocket = socket;
          DataOutputStream out = new DataOutputStream(finalSocket.getOutputStream());
          DataInputStream in = new DataInputStream(finalSocket.getInputStream())) {
          
          byte[] clientHandleOpaque = comms.getEndpointInfo();
          out.writeUTF(localPeerName);
          out.writeLong(clientHandleOpaque.length);
          out.write(clientHandleOpaque);
          out.flush();

          long handleSize = in.readLong();
          byte[] serverHandleOpaque = new byte[(int) handleSize];
          in.readFully(serverHandleOpaque);

          long remoteBaseAddress = in.readLong();
          this.remoteSize = in.readLong();
          byte[] remoteTokenOpaque = new byte[4096];
          in.readFully(remoteTokenOpaque);
          logger.info("Received all handles and memory metadata.");

          comms.addRemoteEndpoint(serverPeerName, serverHandleOpaque, true);
          comms.connect(serverPeerName);
          logger.info("Connected. Confirming to OOB server...");
          out.writeByte(1);
          out.flush();

          CommsWrapper.NativeBuffer nativeBuffer = comms.allocateAndRegMem(remoteSize, CommsWrapper.MemoryType.Dram);
          this.localBuffer = nativeBuffer.buffer;
          this.localToken = nativeBuffer.token;
          this.remoteToken = comms.getMemToken(remoteTokenOpaque);

          comms.initClientPool(this.pushSlotsCount, this.pushSlotSize, this.fetchSlotsCount, this.fetchSlotSize, this.localBuffer, this.localToken, this.remoteToken, this.serverPeerName);
          this.running.set(true);
          logger.info("Control plane setup complete. Ready for RDMA transfer via JNI.");
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

  public void fetchChunk(long streamId, int chunkIndex, ChunkReceivedCallback callback) {
    if (isRunning()) {
      enqueueFetch(new FetchReq(streamId, chunkIndex, callback));
    } else {
      callback.onFailure(chunkIndex, new IllegalStateException("CommsClient is not setup"));
    }
  }

  public void pushData(byte[] body, String shuffleKey, String partitionUniqueId, RpcResponseCallback callback) {
    if (isRunning()) {
      enqueuePush(new PushReq(body, shuffleKey, partitionUniqueId, callback));
    } else {
      callback.onFailure(new IllegalStateException("CommsClient is not setup"));
    }
  }

  public void pushMergedData(byte[] body, String shuffleKey, String[] partitionUniqueIds, int[] offsets, RpcResponseCallback callback) {
    if (isRunning()) {
      enqueuePushMerged(new PushMergedReq(body, shuffleKey, partitionUniqueIds, offsets, callback));
    } else {
      callback.onFailure(new IllegalStateException("CommsClient is not setup"));
    }
  }

  private void enqueueFetch(FetchReq req) {
      List<FetchReq> readyToFlush = null;
      lock.lock();
      try {
          fetchBatch.add(req);
          scheduleTimerIfNeeded();
          if (fetchBatch.size() >= MAX_BATCH) {
              if (currentTimer != null && pushBatch.isEmpty() && pushMergedBatch.isEmpty()) {
                  currentTimer.cancel(false);
                  currentTimer = null;
              }
              readyToFlush = new ArrayList<>(fetchBatch);
              fetchBatch.clear();
          }
      } finally {
          lock.unlock();
      }
      if (readyToFlush != null) flushFetchBatched(readyToFlush);
  }

  private void enqueuePush(PushReq req) {
      List<PushReq> readyToFlush = null;
      lock.lock();
      try {
          pushBatch.add(req);
          scheduleTimerIfNeeded();
          if (pushBatch.size() >= MAX_BATCH) {
              if (currentTimer != null && fetchBatch.isEmpty() && pushMergedBatch.isEmpty()) {
                  currentTimer.cancel(false);
                  currentTimer = null;
              }
              readyToFlush = new ArrayList<>(pushBatch);
              pushBatch.clear();
          }
      } finally {
          lock.unlock();
      }
      if (readyToFlush != null) flushPushBatched(readyToFlush);
  }

  private void enqueuePushMerged(PushMergedReq req) {
      List<PushMergedReq> readyToFlush = null;
      lock.lock();
      try {
          pushMergedBatch.add(req);
          scheduleTimerIfNeeded();
          if (pushMergedBatch.size() >= MAX_BATCH) {
              if (currentTimer != null && fetchBatch.isEmpty() && pushBatch.isEmpty()) {
                  currentTimer.cancel(false);
                  currentTimer = null;
              }
              readyToFlush = new ArrayList<>(pushMergedBatch);
              pushMergedBatch.clear();
          }
      } finally {
          lock.unlock();
      }
      if (readyToFlush != null) flushPushMergedBatched(readyToFlush);
  }

  private void scheduleTimerIfNeeded() {
      if (currentTimer == null || currentTimer.isDone() || currentTimer.isCancelled()) {
          currentTimer = scheduler.schedule(this::flushFromTimer, 200, TimeUnit.MICROSECONDS);
      }
  }

  private void flushFromTimer() {
      List<FetchReq> fBatch = null;
      List<PushReq> pBatch = null;
      List<PushMergedReq> pmBatch = null;
      lock.lock();
      try {
          if (!fetchBatch.isEmpty()) { fBatch = new ArrayList<>(fetchBatch); fetchBatch.clear(); }
          if (!pushBatch.isEmpty()) { pBatch = new ArrayList<>(pushBatch); pushBatch.clear(); }
          if (!pushMergedBatch.isEmpty()) { pmBatch = new ArrayList<>(pushMergedBatch); pushMergedBatch.clear(); }
          currentTimer = null;
      } finally {
          lock.unlock();
      }
      if (fBatch != null) flushFetchBatched(fBatch);
      if (pBatch != null) flushPushBatched(pBatch);
      if (pmBatch != null) flushPushMergedBatched(pmBatch);
  }

  private void flushFetchBatched(List<FetchReq> items) {
      if (items.isEmpty()) return;
      long t0 = rdmaTrackerEnabled ? System.nanoTime() : 0;
      logger.info("Flushed FETCH batch of size: {}", items.size());
      int n = items.size();
      long[] streamIds = new long[n];
      int[] chunkIndices = new int[n];
      ChunkReceivedCallback[] cbs = new ChunkReceivedCallback[n];
      for(int i = 0; i < n; i++) {
          FetchReq r = items.get(i);
          streamIds[i] = r.streamId;
          chunkIndices[i] = r.chunkIndex;
          cbs[i] = r.cb;
      }
      comms.fetchChunksBatched(serverPeerName, streamIds, chunkIndices, cbs);
      if (rdmaTrackerEnabled) {
          rdma_comms.RDMATracker.record(rdma_comms.RDMATracker.CallType.CLIENT_FETCH_CHUNK, System.nanoTime() - t0);
          rdma_comms.RDMATracker.recordBatchSize(rdma_comms.RDMATracker.BatchType.CLIENT_FETCH, items.size());
      }
  }

  private void flushPushBatched(List<PushReq> items) {
      if (items.isEmpty()) return;
      long t0 = rdmaTrackerEnabled ? System.nanoTime() : 0;
      logger.info("Flushed PUSH batch of size: {}", items.size());
      int n = items.size();
      byte[][] bodies = new byte[n][];
      String[] keys = new String[n];
      String[] pids = new String[n];
      RpcResponseCallback[] cbs = new RpcResponseCallback[n];
      for (int i=0; i<n; i++) {
          PushReq r = items.get(i);
          bodies[i] = r.body;
          keys[i] = r.shuffleKey;
          pids[i] = r.pid;
          cbs[i] = r.cb;
      }
      comms.pushDataBatched(serverPeerName, bodies, keys, pids, cbs);
      if (rdmaTrackerEnabled) {
          rdma_comms.RDMATracker.record(rdma_comms.RDMATracker.CallType.CLIENT_PUSH_DATA, System.nanoTime() - t0);
          rdma_comms.RDMATracker.recordBatchSize(rdma_comms.RDMATracker.BatchType.CLIENT_PUSH, items.size());
      }
  }

  private void flushPushMergedBatched(List<PushMergedReq> items) {
      if (items.isEmpty()) return;
      long t0 = rdmaTrackerEnabled ? System.nanoTime() : 0;
      logger.info("Flushed PUSH_MERGED batch of size: {}", items.size());
      int n = items.size();
      byte[][] bodies = new byte[n][];
      String[] keys = new String[n];
      String[][] pids = new String[n][];
      int[][] offsets = new int[n][];
      RpcResponseCallback[] cbs = new RpcResponseCallback[n];
      for (int i=0; i<n; i++) {
          PushMergedReq r = items.get(i);
          bodies[i] = r.body;
          keys[i] = r.shuffleKey;
          pids[i] = r.pids;
          offsets[i] = r.offsets;
          cbs[i] = r.cb;
      }
      comms.pushMergedDataBatched(serverPeerName, bodies, keys, pids, offsets, cbs);
      if (rdmaTrackerEnabled) {
          rdma_comms.RDMATracker.record(rdma_comms.RDMATracker.CallType.CLIENT_PUSH_MERGED_DATA, System.nanoTime() - t0);
          rdma_comms.RDMATracker.recordBatchSize(rdma_comms.RDMATracker.BatchType.CLIENT_PUSH, items.size());
      }
  }

  public void shutdown() {
    running.set(false);
    scheduler.shutdownNow();
    if (localToken != null && localBuffer != null) {
      try {
        comms.deregAndFreeMem(localBuffer, localToken);
      } catch (Exception e) {}
      localToken = null;
    }
    if (remoteToken != null) {
      remoteToken.close();
      remoteToken = null;
    }
    if (comms != null) {
      try { comms.close(); } catch (Exception e) {}
      comms = null;
    }
  }

  boolean isRunning() { return running.get(); }
  public long getRemoteSize() { return remoteSize; } 
}
