package rdma_comms;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import io.netty.buffer.ByteBuf;
import org.apache.celeborn.common.network.buffer.ManagedBuffer;
import org.apache.celeborn.common.network.buffer.NettyManagedBuffer;
import org.apache.celeborn.common.network.client.ChunkReceivedCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RdmaFetcher {
  private static final Logger logger = LoggerFactory.getLogger(RdmaFetcher.class);

  private final CommsClient client;
  private final BlockingQueue<Integer> fetchFreeSlots;
  private final Map<String, FetchTask> pendingTasks = new ConcurrentHashMap<>();
  private final java.util.Queue<FetchRequest> waitingQueue = new java.util.concurrent.ConcurrentLinkedQueue<>();

  public RdmaFetcher(CommsClient client, BlockingQueue<Integer> fetchFreeSlots) {
    this.client = client;
    this.fetchFreeSlots = fetchFreeSlots;
  }

  public void fetchChunk(long streamId, int chunkIndex, ChunkReceivedCallback callback) {
    long t0_tracker = client.isRdmaTrackerEnabled() ? System.nanoTime() : 0;
    try {
      if (!client.isRunning()) {
        callback.onFailure(chunkIndex, new IllegalStateException("CommsClient is not running"));
        return;
      }

      Integer slot = fetchFreeSlots.poll();
      if (slot != null) {
        dispatchFetchDirect(streamId, chunkIndex, callback, slot);
      } else {
        waitingQueue.offer(new FetchRequest(streamId, chunkIndex, callback));
      }
    } finally {
      if (client.isRdmaTrackerEnabled()) {
        rdma_comms.RDMATracker.record(rdma_comms.RDMATracker.CallType.CLIENT_FETCH_CHUNK, System.nanoTime() - t0_tracker);
      }
    }
  }

  private void dispatchFetchDirect(long streamId, int chunkIndex, ChunkReceivedCallback callback, int slot) {
    try {
      FetchTask task = new FetchTask(streamId, chunkIndex, callback, slot);
      String key = streamId + "_" + chunkIndex;
      pendingTasks.put(key, task);
      client.getComms().notify(client.getServerPeerName(), "FETCH_REQUEST:" + streamId + ":" + chunkIndex + ":" + slot);
    } catch (Exception e) {
      callback.onFailure(chunkIndex, e);
      client.releaseSlot(slot);
    }
  }

  public void processChunkReady(long streamId, int chunkIndex, int length, long serverOffset) {
    String key = streamId + "_" + chunkIndex;
    FetchTask task = pendingTasks.get(key); // DO NOT remove yet, we need it in processNetworkFetchDone
    if (task != null) {
      task.length = length;
      task.serverOffset = serverOffset;
      logger.info("processChunkReady: Received CHUNK_READY for {}_{} (len: {}, serverOffset: {})", 
          streamId, chunkIndex, length, serverOffset);
      executeRdmaReadAsync(task, length, serverOffset);
    } else {
      logger.warn("processChunkReady: Received CHUNK_READY for unknown task: {}_{}", streamId, chunkIndex);
      try {
        client.getComms().notify(client.getServerPeerName(), "CHUNK_DONE:" + serverOffset);
      } catch (Exception e) {
        logger.error("Failed to notify CHUNK_DONE for unknown task", e);
      }
    }
  }

  public void processChunkFailed(long streamId, int chunkIndex, String errorMsg) {
    String key = streamId + "_" + chunkIndex;
    FetchTask task = pendingTasks.remove(key);
    if (task != null) {
      logger.error("Server failed to fetch chunk {}_{}: {}", streamId, chunkIndex, errorMsg);
      task.callback.onFailure(chunkIndex, new java.io.IOException("Server failed to fetch chunk: " + errorMsg));
      client.releaseSlot(task.localOffset);
    } else {
      logger.warn("processChunkFailed: Received CHUNK_FAILED for unknown task: {}_{}", streamId, chunkIndex);
    }
  }

  private void executeRdmaReadAsync(FetchTask task, int length, long serverOffset) {
    long localAddr = client.getLocalBaseAddress() + task.localOffset;
    long remoteAddr = client.getRemoteBaseAddress() + serverOffset;
    
    logger.debug("executeRdmaReadAsync: Starting JNI asyncFetch (Read) for chunk {}_{} (len: {}, serverOffset: {}, localOffset: {})...", 
        task.streamId, task.chunkIndex, length, serverOffset, task.localOffset);

    try {
      // payload will be processed by CommsServer, which frees the slot and echoes back fetching success.
      String bouncyMsg = "CHUNK_DONE:" + serverOffset + ":" + task.streamId + ":" + task.chunkIndex;
      client.getComms().asyncFetch(client.getServerPeerName(), localAddr, remoteAddr, length, client.getLocalToken(), client.getRemoteToken(), bouncyMsg);
    } catch (Exception e) {
      logger.error("executeRdmaReadAsync: asyncFetch failed for chunk {}_{}", task.streamId, task.chunkIndex, e);
      task.callback.onFailure(task.chunkIndex, e);
      client.releaseSlot(task.localOffset);
      pendingTasks.remove(task.streamId + "_" + task.chunkIndex);
      try {
        client.getComms().notify(client.getServerPeerName(), "CHUNK_DONE:" + serverOffset + ":" + task.streamId + ":" + task.chunkIndex);
      } catch (Exception ne) {}
    }
  }

  public void processNetworkFetchDone(long streamId, int chunkIndex) {
    String key = streamId + "_" + chunkIndex;
    FetchTask task = pendingTasks.remove(key);
    if (task == null) {
      logger.warn("processNetworkFetchDone: Received bounce for unknown task: {}_{}", streamId, chunkIndex);
      return;
    }

    if (CommsWrapper.RDMA_TRACKER_ENABLED) {
      RDMATracker.recordTransfer(true, task.length);
    }

    ByteBuffer sliced;
    ByteBuffer duplicate = client.getLocalBuffer().duplicate();
    duplicate.position(task.localOffset);
    duplicate.limit(task.localOffset + task.length);
    sliced = duplicate.slice();

    ByteBuf customBuf = new CustomRDMAByteBuf(
        io.netty.buffer.UnpooledByteBufAllocator.DEFAULT,
        sliced,
        task.length,
        () -> {
          logger.debug("CustomRDMAByteBuf release hook: Triggered for slot offset {} for chunk {}_{}", 
              task.localOffset, task.streamId, task.chunkIndex);
          client.releaseSlot(task.localOffset);
          // Note: We don't send CHUNK_DONE to the server here, because the C++ asyncFetch poller already sent it!
        }
    );

    ManagedBuffer managedBuffer = new NettyManagedBuffer(customBuf);
    task.callback.onSuccess(task.chunkIndex, managedBuffer);
    managedBuffer.release();
  }

  void releaseSlot(int slotOffset) {
    fetchFreeSlots.offer(slotOffset);
    FetchRequest req = waitingQueue.poll();
    if (req != null) {
      Integer newSlot = fetchFreeSlots.poll();
      if (newSlot != null) {
        dispatchFetchDirect(req.streamId, req.chunkIndex, req.callback, newSlot);
      } else {
        waitingQueue.offer(req);
      }
    }
  }

  public void failPendingTasks(Throwable cause) {
    for (Map.Entry<String, FetchTask> entry : pendingTasks.entrySet()) {
      FetchTask task = entry.getValue();
      try {
        task.callback.onFailure(task.chunkIndex, cause);
      } catch (Exception e) {
        logger.error("Failed to trigger onFailure callback for {}_{}", task.streamId, task.chunkIndex, e);
      }
      client.releaseSlot(task.localOffset);
    }
    pendingTasks.clear();
  }

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
}
