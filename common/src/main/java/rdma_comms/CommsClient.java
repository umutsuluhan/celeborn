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

  private final java.util.concurrent.CountDownLatch readyLatch = new java.util.concurrent.CountDownLatch(1);

  public static CommsClient getOrCreate(org.apache.celeborn.common.CelebornConf conf) {
    CommsClient client = null;
    if (_instance == null) {
      synchronized (CommsClient.class) {
        if (_instance == null) {
          String baseName = conf.rdmaClientLocalPeerName();
          String uniqueClientName = baseName + "_" + java.util.UUID.randomUUID().toString().substring(0, 8);
          long chunkSize = conf.shuffleChunkSize();
          int slotSize = (int) chunkSize + 4 * 1024 * 1024; // 4MB headroom for record boundary overflow
          client = new CommsClient(
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
          client.setupAsync();
          _instance = client;
          logger.info("EAGER BOOT GATING: JNI CommsClient setup dispatched asynchronously!");
        }
      }
    }
    try {
      _instance.readyLatch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
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
  private int serverPeerId = -1;
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
  private final java.util.concurrent.locks.Condition notEmptyCondition = lock.newCondition();
  private Thread pollerThread;
  private long firstStrandedTimeNanos = 0;

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

  public void setupAsync() {
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

        logger.info("Connecting to OOB server via JNI async {}:{}...", serverIp, oobPort);
        
        comms.connectAndRegisterOOBAsync(
            serverIp, oobPort, localPeerName, serverPeerName,
            pushSlotsCount, pushSlotSize, fetchSlotsCount, fetchSlotSize,
            new RpcResponseCallback() {
                @Override
                public void onSuccess(ByteBuffer response) {
                    localBuffer = response;
                    running.set(true);
                    serverPeerId = comms.getPeerId(serverPeerName);
                    pollerThread = new Thread(CommsClient.this::runBatchPoller, "RDMA-Comms-Poller");
                    pollerThread.setDaemon(true);
                    pollerThread.start();
                    logger.info("Control plane async setup complete. Ready for RDMA transfer via JNI.");
                    readyLatch.countDown();
                }

                @Override
                public void onFailure(Throwable e) {
                    logger.error("Control plane async setup failed via JNI: {}", e.getMessage(), e);
                    shutdown();
                    readyLatch.countDown(); // Unblock waiters, but running is false, so next calls will fail normally
                }
            }
        );

      } catch (Exception e) {
        logger.error("Control setup failed", e);
        shutdown();
        readyLatch.countDown();
      }
    } finally {
      if (rdmaTrackerEnabled) {
        rdma_comms.RDMATracker.record(rdma_comms.RDMATracker.CallType.CLIENT_SETUP, System.nanoTime() - t0_tracker);
      }
    }
  }

  private void runBatchPoller() {
      while (isRunning()) {
          List<FetchReq> fBatch = null;
          List<PushReq> pBatch = null;
          List<PushMergedReq> pmBatch = null;
          lock.lock();
          try {
              while (isRunning() && fetchBatch.isEmpty() && pushBatch.isEmpty() && pushMergedBatch.isEmpty()) {
                  firstStrandedTimeNanos = 0;
                  try { notEmptyCondition.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
              }
              if (!isRunning()) break;

              long now = System.nanoTime();
              if (firstStrandedTimeNanos == 0) firstStrandedTimeNanos = now;

              if (now - firstStrandedTimeNanos >= 200_000L) {
                  if (!fetchBatch.isEmpty()) { fBatch = new ArrayList<>(fetchBatch); fetchBatch.clear(); }
                  if (!pushBatch.isEmpty()) { pBatch = new ArrayList<>(pushBatch); pushBatch.clear(); }
                  if (!pushMergedBatch.isEmpty()) { pmBatch = new ArrayList<>(pushMergedBatch); pushMergedBatch.clear(); }
                  firstStrandedTimeNanos = 0;
              }
          } finally {
              lock.unlock();
          }

          if (fBatch != null || pBatch != null || pmBatch != null) {
              if (fBatch != null) flushFetchBatched(fBatch);
              if (pBatch != null) flushPushBatched(pBatch);
              if (pmBatch != null) flushPushMergedBatched(pmBatch);
          }
      }
  }

  public void fetchChunk(long streamId, int chunkIndex, ChunkReceivedCallback callback) {
    if (isRunning()) {
      enqueueFetch(new FetchReq(streamId, chunkIndex, callback));
    } else {
      callback.onFailure(chunkIndex, new IllegalStateException("CommsClient is not setup (setup failed)"));
    }
  }

  public void pushData(byte[] body, String shuffleKey, String partitionUniqueId, RpcResponseCallback callback) {
    if (isRunning()) {
      enqueuePush(new PushReq(body, shuffleKey, partitionUniqueId, callback));
    } else {
      callback.onFailure(new IllegalStateException("CommsClient is not setup (setup failed)"));
    }
  }

  public void pushMergedData(byte[] body, String shuffleKey, String[] partitionUniqueIds, int[] offsets, RpcResponseCallback callback) {
    if (isRunning()) {
      enqueuePushMerged(new PushMergedReq(body, shuffleKey, partitionUniqueIds, offsets, callback));
    } else {
      callback.onFailure(new IllegalStateException("CommsClient is not setup (setup failed)"));
    }
  }

  private void enqueueFetch(FetchReq req) {
      List<FetchReq> readyToFlush = null;
      lock.lock();
      try {
          boolean wasEmpty = fetchBatch.isEmpty() && pushBatch.isEmpty() && pushMergedBatch.isEmpty();
          fetchBatch.add(req);
          if (wasEmpty) notEmptyCondition.signal();
          
          if (fetchBatch.size() >= MAX_BATCH) {
              if (pushBatch.isEmpty() && pushMergedBatch.isEmpty()) firstStrandedTimeNanos = 0;
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
          boolean wasEmpty = fetchBatch.isEmpty() && pushBatch.isEmpty() && pushMergedBatch.isEmpty();
          pushBatch.add(req);
          if (wasEmpty) notEmptyCondition.signal();
          
          if (pushBatch.size() >= MAX_BATCH) {
              if (fetchBatch.isEmpty() && pushMergedBatch.isEmpty()) firstStrandedTimeNanos = 0;
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
          boolean wasEmpty = fetchBatch.isEmpty() && pushBatch.isEmpty() && pushMergedBatch.isEmpty();
          pushMergedBatch.add(req);
          if (wasEmpty) notEmptyCondition.signal();
          
          if (pushMergedBatch.size() >= MAX_BATCH) {
              if (fetchBatch.isEmpty() && pushBatch.isEmpty()) firstStrandedTimeNanos = 0;
              readyToFlush = new ArrayList<>(pushMergedBatch);
              pushMergedBatch.clear();
          }
      } finally {
          lock.unlock();
      }
      if (readyToFlush != null) flushPushMergedBatched(readyToFlush);
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
      comms.fetchChunksBatched(serverPeerId, streamIds, chunkIndices, cbs);
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
      comms.pushDataBatched(serverPeerId, bodies, keys, pids, cbs);
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
      comms.pushMergedDataBatched(serverPeerId, bodies, keys, pids, offsets, cbs);
      if (rdmaTrackerEnabled) {
          rdma_comms.RDMATracker.record(rdma_comms.RDMATracker.CallType.CLIENT_PUSH_MERGED_DATA, System.nanoTime() - t0);
          rdma_comms.RDMATracker.recordBatchSize(rdma_comms.RDMATracker.BatchType.CLIENT_PUSH, items.size());
      }
  }

  public void shutdown() {
    running.set(false);
    lock.lock();
    try { notEmptyCondition.signalAll(); } finally { lock.unlock(); }
    if (pollerThread != null) pollerThread.interrupt();
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
