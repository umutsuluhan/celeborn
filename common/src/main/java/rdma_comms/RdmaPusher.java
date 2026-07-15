package rdma_comms;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.celeborn.common.network.client.RpcResponseCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RdmaPusher {
  private static final Logger logger = LoggerFactory.getLogger(RdmaPusher.class);

  private final CommsClient client;
  private final BlockingQueue<Integer> pushFreeSlots;
  private final Map<Integer, RpcResponseCallback> pendingPushes = new ConcurrentHashMap<>();

  public RdmaPusher(CommsClient client, BlockingQueue<Integer> pushFreeSlots) {
    this.client = client;
    this.pushFreeSlots = pushFreeSlots;
  }

  public void pushData(byte[] body, String shuffleKey, String partitionUniqueId, RpcResponseCallback callback) {
    long t0_tracker = client.isRdmaTrackerEnabled() ? System.nanoTime() : 0;
    try {
      if (!client.isRunning()) {
        callback.onFailure(new IllegalStateException("CommsClient is not running"));
        return;
      }

      Integer slot;
      try {
        slot = pushFreeSlots.take();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        callback.onFailure(e);
        return;
      }

      client.getTransferExecutor().submit(() -> {
        try {
          ByteBuffer duplicate;
          synchronized (client.getLocalBuffer()) {
              duplicate = client.getLocalBuffer().duplicate();
          }
          duplicate.position(slot);
          duplicate.put(body);
          pendingPushes.put(slot, callback);

          long localAddr = client.getLocalBaseAddress() + slot;
          long remoteAddr = client.getRemoteBaseAddress() + slot;
          int length = body.length;

          try (CommsWrapper.TransferIov localIov = new CommsWrapper.TransferIov(false);
               CommsWrapper.TransferIov remoteIov = new CommsWrapper.TransferIov(true)) {
            
            localIov.addSegment(localAddr, length, client.getLocalToken());
            remoteIov.addSegment(remoteAddr, length, client.getRemoteToken());

            try (CommsWrapper.Request req = client.getComms().postTransfer(client.getServerPeerName(), CommsWrapper.TransferOpType.Write, localIov, remoteIov, "")) {
              CommsWrapper.TransferStatus status = req.waitCompletion();
              if (status.state != CommsWrapper.State.Done) {
                throw new java.io.IOException("RDMA Write failed with state: " + status.state);
              }
              if (CommsWrapper.RDMA_TRACKER_ENABLED) {
                rdma_comms.RDMATracker.recordTransfer(false, length);
              }
            }
          }

          String msg = "PUSH_DATA:" + slot + ":" + length + ":" + shuffleKey + ":" + partitionUniqueId;
          client.getComms().notify(client.getServerPeerName(), msg);

        } catch (Exception e) {
          logger.error("dispatchPushData: Failed for slot {}", slot, e);
          pendingPushes.remove(slot);
          callback.onFailure(e);
          client.releaseSlot(slot);
        }
      });
    } finally {
      if (client.isRdmaTrackerEnabled()) {
        rdma_comms.RDMATracker.record(rdma_comms.RDMATracker.CallType.CLIENT_PUSH_DATA, System.nanoTime() - t0_tracker);
      }
    }
  }

  public void pushMergedData(byte[] body, String shuffleKey, String[] partitionUniqueIds, int[] offsets, RpcResponseCallback callback) {
    long t0_tracker = client.isRdmaTrackerEnabled() ? System.nanoTime() : 0;
    try {
      if (!client.isRunning()) {
        callback.onFailure(new IllegalStateException("CommsClient is not running"));
        return;
      }

      Integer slot;
      try {
        slot = pushFreeSlots.take();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        callback.onFailure(e);
        return;
      }

      client.getTransferExecutor().submit(() -> {
        try {
          ByteBuffer duplicate;
          synchronized (client.getLocalBuffer()) {
              duplicate = client.getLocalBuffer().duplicate();
          }
          duplicate.position(slot);
          duplicate.put(body);
          pendingPushes.put(slot, callback);

          long localAddr = client.getLocalBaseAddress() + slot;
          long remoteAddr = client.getRemoteBaseAddress() + slot;
          int length = body.length;

          try (CommsWrapper.TransferIov localIov = new CommsWrapper.TransferIov(false);
               CommsWrapper.TransferIov remoteIov = new CommsWrapper.TransferIov(true)) {
            
            localIov.addSegment(localAddr, length, client.getLocalToken());
            remoteIov.addSegment(remoteAddr, length, client.getRemoteToken());

            try (CommsWrapper.Request req = client.getComms().postTransfer(client.getServerPeerName(), CommsWrapper.TransferOpType.Write, localIov, remoteIov, "")) {
              CommsWrapper.TransferStatus status = req.waitCompletion();
              if (status.state != CommsWrapper.State.Done) {
                throw new java.io.IOException("RDMA Merged Write failed with state: " + status.state);
              }
              if (CommsWrapper.RDMA_TRACKER_ENABLED) {
                rdma_comms.RDMATracker.recordTransfer(false, length);
              }
            }
          }

          String pIds = String.join(",", partitionUniqueIds);
          String offs = java.util.Arrays.stream(offsets).mapToObj(String::valueOf).collect(java.util.stream.Collectors.joining(","));
          String msg = "PUSH_MERGED_DATA:" + slot + ":" + length + ":" + shuffleKey + ":" + pIds + ";" + offs;
          client.getComms().notify(client.getServerPeerName(), msg);

        } catch (Exception e) {
          logger.error("dispatchPushMergedData: Failed for slot {}", slot, e);
          pendingPushes.remove(slot);
          callback.onFailure(e);
          client.releaseSlot(slot);
        }
      });
    } finally {
      if (client.isRdmaTrackerEnabled()) {
        rdma_comms.RDMATracker.record(rdma_comms.RDMATracker.CallType.CLIENT_PUSH_MERGED_DATA, System.nanoTime() - t0_tracker);
      }
    }
  }

  public void processPushComplete(int slotOffset, byte statusCode) {
    RpcResponseCallback callback = pendingPushes.remove(slotOffset);
    if (callback != null) {
      logger.debug("processPushComplete: PUSH_COMPLETE received for slotOffset {} with status {}", slotOffset, statusCode);
      callback.onSuccess(ByteBuffer.wrap(new byte[] { statusCode }));
    } else {
      logger.warn("processPushComplete: PUSH_COMPLETE received for unknown slotOffset {}", slotOffset);
    }
    client.releaseSlot(slotOffset);
  }

  public void processPushFailed(int slotOffset, String errorMsg) {
    RpcResponseCallback callback = pendingPushes.remove(slotOffset);
    if (callback != null) {
      logger.error("processPushFailed: PUSH_FAILED received for slotOffset {}: {}", slotOffset, errorMsg);
      callback.onFailure(new IOException("RDMA push failed: " + errorMsg));
    } else {
      logger.warn("processPushFailed: PUSH_FAILED received for unknown slotOffset {}", slotOffset);
    }
    client.releaseSlot(slotOffset);
  }

  public void failPendingTasks(Throwable cause) {
    for (Map.Entry<Integer, RpcResponseCallback> entry : pendingPushes.entrySet()) {
      int slot = entry.getKey();
      RpcResponseCallback callback = entry.getValue();
      try {
        callback.onFailure(cause);
      } catch (Exception e) {
        logger.error("Failed to trigger onFailure callback for push slot {}", slot, e);
      }
      client.releaseSlot(slot);
    }
    pendingPushes.clear();
  }

  void releaseSlot(int slotOffset) {
    pushFreeSlots.offer(slotOffset);
  }
}
