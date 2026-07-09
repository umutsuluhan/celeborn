package rdma_comms;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RDMATracker {
  private static final Logger logger = LoggerFactory.getLogger(RDMATracker.class);

  public enum CallType {
    INIT,
    GET_ENDPOINT_INFO,
    ADD_REMOTE_ENDPOINT,
    CONNECT,
    REG_MEM,
    DEREG_MEM,
    GET_MEM_TOKEN,
    SEMAPHORE_ACQUIRE,
    POST_TRANSFER,
    NOTIFY,
    GET_PEER_NOTIFICATION,
    REQUEST_GET_STATUS,
    REQUEST_DESTROY
  }

  public enum BatchType {
    CLIENT_PUSH,
    CLIENT_FETCH,
    SERVER_FETCH_REQUEST,
    SERVER_PUSH_REQUEST,
    SERVER_CHUNK_READY
  }

  private static class StatBucket {
    final LongAdder count = new LongAdder();
    final LongAdder totalNs = new LongAdder();
    final AtomicLong maxNs = new AtomicLong(0);

    void record(long ns) {
      count.increment();
      totalNs.add(ns);
      
      long currentMax;
      do {
        currentMax = maxNs.get();
        if (ns <= currentMax) {
          break;
        }
      } while (!maxNs.compareAndSet(currentMax, ns));
    }
  }

  private static final StatBucket[] buckets;
  private static final java.util.concurrent.ConcurrentHashMap<Integer, LongAdder> clientPushBatchSizes = new java.util.concurrent.ConcurrentHashMap<>();
  private static final java.util.concurrent.ConcurrentHashMap<Integer, LongAdder> clientFetchBatchSizes = new java.util.concurrent.ConcurrentHashMap<>();
  private static final java.util.concurrent.ConcurrentHashMap<Integer, LongAdder> serverFetchReqBatchSizes = new java.util.concurrent.ConcurrentHashMap<>();
  private static final java.util.concurrent.ConcurrentHashMap<Integer, LongAdder> serverPushReqBatchSizes = new java.util.concurrent.ConcurrentHashMap<>();
  private static final java.util.concurrent.ConcurrentHashMap<Integer, LongAdder> serverChunkReadyBatchSizes = new java.util.concurrent.ConcurrentHashMap<>();

  private static class SizeStatBucket {
    final LongAdder count = new LongAdder();
    final LongAdder totalBytes = new LongAdder();
    final AtomicLong maxBytes = new AtomicLong(0);

    void record(long bytes) {
      count.increment();
      totalBytes.add(bytes);
      
      long currentMax;
      do {
        currentMax = maxBytes.get();
        if (bytes <= currentMax) {
          break;
        }
      } while (!maxBytes.compareAndSet(currentMax, bytes));
    }
  }

  private static final SizeStatBucket readTransfers = new SizeStatBucket();
  private static final SizeStatBucket writeTransfers = new SizeStatBucket();

  static {
    int numTypes = CallType.values().length;
    buckets = new StatBucket[numTypes];
    for (int i = 0; i < numTypes; i++) {
      buckets[i] = new StatBucket();
    }

    // Register JVM shutdown hook to write results before exiting
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      writeSnapshot();
    }, "RDMATracker-ShutdownHook"));
  }

  public static void record(CallType type, long latencyNs) {
    if (type != null) {
      buckets[type.ordinal()].record(latencyNs);
    }
  }

  public static void recordBatchSize(BatchType type, int size) {
    java.util.concurrent.ConcurrentHashMap<Integer, LongAdder> map = null;
    switch (type) {
      case CLIENT_PUSH: map = clientPushBatchSizes; break;
      case CLIENT_FETCH: map = clientFetchBatchSizes; break;
      case SERVER_FETCH_REQUEST: map = serverFetchReqBatchSizes; break;
      case SERVER_PUSH_REQUEST: map = serverPushReqBatchSizes; break;
      case SERVER_CHUNK_READY: map = serverChunkReadyBatchSizes; break;
    }
    if (map != null) {
      map.computeIfAbsent(size, k -> new LongAdder()).increment();
    }
  }

  public static void recordTransfer(boolean isRead, long bytes) {
    if (isRead) {
      readTransfers.record(bytes);
    } else {
      writeTransfers.record(bytes);
    }
  }

  public static void writeSnapshot() {
    String timestamp = new java.text.SimpleDateFormat("yyyyMMdd_HHmmss").format(new java.util.Date());
    String filename = String.format("/tmp/rdma_tracker_%s_%d.log", timestamp, ProcessHandle.current().pid());
    File logFile = new File(filename);
    
    try (PrintWriter writer = new PrintWriter(new FileWriter(logFile))) {
      writer.println("=====================================================================================");
      writer.println("RDMA JNI Call Latency Statistics Snapshot");
      writer.println("=====================================================================================");
      writer.printf("%-25s %12s %18s %18s %18s%n", "JNI Call Type", "Count", "Total Time (ms)", "Avg Latency (us)", "Max Latency (us)");
      writer.println("-------------------------------------------------------------------------------------");
      
      boolean hasStats = false;
      for (CallType type : CallType.values()) {
        StatBucket bucket = buckets[type.ordinal()];
        long count = bucket.count.sum();
        if (count > 0) {
          hasStats = true;
          double totalMs = bucket.totalNs.sum() / 1_000_000.0;
          double avgUs = (bucket.totalNs.sum() / (double) count) / 1_000.0;
          double maxUs = bucket.maxNs.get() / 1_000.0;
          writer.printf("%-25s %12d %18.3f %18.3f %18.3f%n", type.name(), count, totalMs, avgUs, maxUs);
        }
      }
      
      if (!hasStats) {
        writer.println("No JNI calls were recorded.");
      }
      writer.println("=====================================================================================");

      writer.println();
      writer.println("=====================================================================================");
      writer.println("RDMA Data Transfer Statistics Snapshot");
      writer.println("=====================================================================================");
      writer.printf("%-15s %12s %18s %18s %18s%n", "Transfer Type", "Count", "Total Data (MB)", "Avg Size (KB)", "Max Size (KB)");
      writer.println("-------------------------------------------------------------------------------------");
      printTransferStats(writer, "READ", readTransfers);
      printTransferStats(writer, "WRITE", writeTransfers);
      writer.println("=====================================================================================");

      writer.println();
      writer.println("=====================================================================================");
      writer.println("RDMA Batch Size Distributions");
      writer.println("=====================================================================================");
      printBatchSizes(writer, "CLIENT PUSH Batch Sizes", clientPushBatchSizes);
      printBatchSizes(writer, "CLIENT FETCH Batch Sizes", clientFetchBatchSizes);
      printBatchSizes(writer, "SERVER FETCH REQUEST Batch Sizes", serverFetchReqBatchSizes);
      printBatchSizes(writer, "SERVER PUSH REQUEST Batch Sizes", serverPushReqBatchSizes);
      printBatchSizes(writer, "SERVER CHUNK READY Batch Sizes", serverChunkReadyBatchSizes);
      writer.println("=====================================================================================");

      logger.info("RDMA Latency stats written to " + logFile.getAbsolutePath());
    } catch (IOException e) {
      logger.error("Failed to write RDMA stats snapshot to " + filename, e);
    }
  }

  private static void printBatchSizes(PrintWriter writer, String label, java.util.concurrent.ConcurrentHashMap<Integer, LongAdder> map) {
    writer.println(label + ":");
    if (map.isEmpty()) {
      writer.println("  No batches recorded.");
    } else {
      map.entrySet().stream()
          .sorted(java.util.Map.Entry.comparingByKey())
          .forEach(entry -> writer.printf("  Size %3d: %d times%n", entry.getKey(), entry.getValue().sum()));
    }
  }

  private static void printTransferStats(PrintWriter writer, String label, SizeStatBucket bucket) {
    long count = bucket.count.sum();
    if (count > 0) {
      double totalMB = bucket.totalBytes.sum() / (1024.0 * 1024.0);
      double avgKB = (bucket.totalBytes.sum() / (double) count) / 1024.0;
      double maxKB = bucket.maxBytes.get() / 1024.0;
      writer.printf("%-15s %12d %18.3f %18.3f %18.3f%n", label, count, totalMB, avgKB, maxKB);
    } else {
      writer.printf("%-15s %12d %18.3f %18.3f %18.3f%n", label, 0, 0.0, 0.0, 0.0);
    }
  }
}
