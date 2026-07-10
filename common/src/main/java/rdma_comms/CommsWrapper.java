package rdma_comms;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.Semaphore;

/**
 * Main entry point for the communication library.
 * Equivalent to C++ class 'comms::Comms' in 'comms/comms.h'.
 */
public class CommsWrapper implements AutoCloseable {
  static {
    try {
      // Extract and load native libraries directly from JAR resources
      java.io.File tempDir = createTempDir();
      java.io.File coreLib = extractResource(tempDir, "/natives/libml_transport.so");
      java.io.File jniLib = extractResource(tempDir, "/natives/libml_transport_jni.so");
      
      System.load(coreLib.getAbsolutePath());
      System.load(jniLib.getAbsolutePath());
    } catch (Exception ex) {
      throw new RuntimeException("Failed to load native libraries from JAR resources", ex);
    }
  }

  public static volatile boolean RDMA_TRACKER_ENABLED = true;

  // Holds the raw pointer to the C++ comms::Comms object.
  private final long nativePtr;
  private final Semaphore transferSemaphore = new Semaphore(128);

  /**
   * Constructor. Creates the underlying C++ Comms object.
   * Equivalent to 'comms::Comms::Comms()'.
   */
  public CommsWrapper() {
    this.nativePtr = nativeCreate();
  }

  public synchronized void init(Map<String, String> params) {
    long t0 = RDMA_TRACKER_ENABLED ? System.nanoTime() : 0;
    try {
      if (params.containsKey("AP_RDMA_TRACKER_ENABLED")) {
        RDMA_TRACKER_ENABLED = Boolean.parseBoolean(params.get("AP_RDMA_TRACKER_ENABLED"));
      }
      nativeInit(nativePtr, params);
    } finally {
      if (RDMA_TRACKER_ENABLED) {
        RDMATracker.record(RDMATracker.CallType.INIT, System.nanoTime() - t0);
      }
    }
  }

  /**
   * Retrieves serialized connection handle of the local instance (opaque bytes).
   * Equivalent to 'absl::StatusOr<size_t> comms::Comms::GetEndpointInfo(opaque_data_t& data)'.
   */
  public synchronized byte[] getEndpointInfo() {
    long t0 = RDMA_TRACKER_ENABLED ? System.nanoTime() : 0;
    try {
      return nativeGetEndpointInfo(nativePtr);
    } finally {
      if (RDMA_TRACKER_ENABLED) {
        RDMATracker.record(RDMATracker.CallType.GET_ENDPOINT_INFO, System.nanoTime() - t0);
      }
    }
  }

  /**
   * Adds a remote endpoint using its opaque handle bytes and connects to it.
   * Internally converts opaque bytes to ConnHandle and uses default ConnConfig in C++ JNI layer.
   * 
   * @param peerName Name of the remote peer.
   * @param opaqueHandleBytes Handle bytes received from the remote peer.
   * @param block If true, blocks until the connection is established.
   */
  public void addRemoteEndpoint(String peerName, byte[] opaqueHandleBytes, boolean block) {
    long t0 = RDMA_TRACKER_ENABLED ? System.nanoTime() : 0;
    try {
      nativeAddRemoteEndpoint(nativePtr, peerName, opaqueHandleBytes, block);
    } finally {
      if (RDMA_TRACKER_ENABLED) {
        RDMATracker.record(RDMATracker.CallType.ADD_REMOTE_ENDPOINT, System.nanoTime() - t0);
      }
    }
  }

  /**
   * Explicitly connects to a remote peer.
   * Equivalent to 'absl::Status comms::Comms::Connect(const std::string&)'.
   */
  public void connect(String peerName) {
    long t0 = RDMA_TRACKER_ENABLED ? System.nanoTime() : 0;
    try {
      nativeConnect(nativePtr, peerName);
    } finally {
      if (RDMA_TRACKER_ENABLED) {
        RDMATracker.record(RDMATracker.CallType.CONNECT, System.nanoTime() - t0);
      }
    }
  }

  /**
   * Registers a local direct ByteBuffer.
   * Equivalent to 'absl::StatusOr<unique_ptr<MemToken>> comms::Comms::RegMem(...)'.
   */
  public synchronized MemToken regMem(ByteBuffer buffer, long size, MemoryType type) {
    if (!buffer.isDirect()) {
      throw new IllegalArgumentException("Buffer must be direct");
    }
    long t0 = RDMA_TRACKER_ENABLED ? System.nanoTime() : 0;
    try {
      long tokenPtr = nativeRegMem(nativePtr, buffer, size, type.ordinal());
      return new MemToken(tokenPtr);
    } finally {
      if (RDMA_TRACKER_ENABLED) {
        RDMATracker.record(RDMATracker.CallType.REG_MEM, System.nanoTime() - t0);
      }
    }
  }

  /**
   * Deregisters a local memory token and closes it.
   * Equivalent to 'absl::Status comms::Comms::DeregMem(MemToken&)'
   */
  public synchronized void deregMem(MemToken token) {
    long t0 = RDMA_TRACKER_ENABLED ? System.nanoTime() : 0;
    try {
      nativeDeregMem(nativePtr, token.getNativePtr());
      token.close(); 
    } finally {
      if (RDMA_TRACKER_ENABLED) {
        RDMATracker.record(RDMATracker.CallType.DEREG_MEM, System.nanoTime() - t0);
      }
    }
  }

  /**
   * Deserializes a remote memory token from opaque bytes.
   * Equivalent to 'absl::StatusOr<unique_ptr<MemToken>> comms::Comms::GetMemToken(const opaque_data_t&)'.
   */
  public synchronized MemToken getMemToken(byte[] serTok) {
    long t0 = RDMA_TRACKER_ENABLED ? System.nanoTime() : 0;
    try {
      long tokenPtr = nativeGetMemToken(nativePtr, serTok);
      return new MemToken(tokenPtr);
    } finally {
      if (RDMA_TRACKER_ENABLED) {
        RDMATracker.record(RDMATracker.CallType.GET_MEM_TOKEN, System.nanoTime() - t0);
      }
    }
  }

  /**
   * Posts an asynchronous transfer operation.
   * Equivalent to 'absl::StatusOr<unique_ptr<Request>> comms::Comms::PostTransfer(...)'.
   */
  public Request postTransfer(String remotePeer, TransferOpType op, TransferIov local, TransferIov remote, String notificationMessage) {
    long tAcq = RDMA_TRACKER_ENABLED ? System.nanoTime() : 0;
    try {
      transferSemaphore.acquire();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CommsException("Interrupted while waiting for transfer semaphore: " + e.getMessage());
    } finally {
      if (RDMA_TRACKER_ENABLED) {
        RDMATracker.record(RDMATracker.CallType.SEMAPHORE_ACQUIRE, System.nanoTime() - tAcq);
      }
    }

    boolean success = false;
    long t0 = RDMA_TRACKER_ENABLED ? System.nanoTime() : 0;
    try {
      long reqPtr = nativePostTransfer(nativePtr, remotePeer, op.ordinal(), local.getNativePtr(), remote.getNativePtr(), notificationMessage);
      success = true;
      return new Request(reqPtr, transferSemaphore);
    } finally {
      if (RDMA_TRACKER_ENABLED) {
        RDMATracker.record(RDMATracker.CallType.POST_TRANSFER, System.nanoTime() - t0);
      }
      if (!success) {
        transferSemaphore.release();
      }
    }
  }

  /**
   * Sends a standalone notification message.
   * Equivalent to 'absl::Status comms::Comms::Notify(const std::string&, const std::string&)'.
   */
  public void notify(String remotePeer, String message) {
    long t0 = RDMA_TRACKER_ENABLED ? System.nanoTime() : 0;
    try {
      nativeNotify(nativePtr, remotePeer, message);
    } finally {
      if (RDMA_TRACKER_ENABLED) {
        RDMATracker.record(RDMATracker.CallType.NOTIFY, System.nanoTime() - t0);
      }
    }
  }

  /**
   * Retrieves the raw message bytes of the earliest unprocessed notification for a peer.
   * Internally extracts the message from 'NotificationProto' in C++ JNI layer.
   * Returns null if no notification is pending.
   */
  public byte[] getPeerNotification(String remotePeer) {
    long t0 = RDMA_TRACKER_ENABLED ? System.nanoTime() : 0;
    try {
      return nativeGetPeerNotification(nativePtr, remotePeer);
    } finally {
      if (RDMA_TRACKER_ENABLED) {
        RDMATracker.record(RDMATracker.CallType.GET_PEER_NOTIFICATION, System.nanoTime() - t0);
      }
    }
  }

  /**
   * Destructor. Frees the underlying C++ Comms object.
   * Equivalent to 'comms::Comms::~Comms()'.
   */
  @Override
  public synchronized void close() {
    nativeDestroy(nativePtr);
  }

  /**
   * JNI Helper to get the native memory address of a direct ByteBuffer.
   */
  public static native long getDirectBufferAddress(ByteBuffer buf);

  // =========================================================================
  // Nested Types
  // =========================================================================

  public enum MemoryType { Dram, Vram }
  public enum TransferOpType { Read, Write }
  public enum State { Unstarted, InProgress, Done, Error }

  public static class TransferStatus {
    public final State state;
    public final long bytesTransferred;

    public TransferStatus(State state, long bytesTransferred) {
      this.state = state;
      this.bytesTransferred = bytesTransferred;
    }
  }

  public static class CommsException extends RuntimeException {
    public CommsException(String message) { super(message); }
  }

  public static class MemToken implements AutoCloseable {
    private long nativePtr;
    MemToken(long nativePtr) { this.nativePtr = nativePtr; }
    long getNativePtr() { return nativePtr; }

    public byte[] serialize() {
      if (nativePtr == 0) throw new IllegalStateException("Closed");
      return nativeMemTokenSerialize(nativePtr);
    }
    public long getAddress() {
      if (nativePtr == 0) throw new IllegalStateException("Closed");
      return nativeMemTokenGetAddress(nativePtr);
    }
    public long getSize() {
      if (nativePtr == 0) throw new IllegalStateException("Closed");
      return nativeMemTokenGetSize(nativePtr);
    }
    @Override
    public void close() {
      if (nativePtr != 0) {
        nativeMemTokenDelete(nativePtr);
        nativePtr = 0;
      }
    }
  }

  public static class TransferIov implements AutoCloseable {
    private long nativePtr;
    public TransferIov(boolean remote) { this.nativePtr = nativeTransferIovCreate(remote); }
    long getNativePtr() { return nativePtr; }

    public void addSegment(long addr, long size, MemToken token) {
      if (nativePtr == 0) throw new IllegalStateException("Closed");
      nativeTransferIovAddSegment(nativePtr, addr, size, token.getNativePtr());
    }
    @Override
    public void close() {
      if (nativePtr != 0) {
        nativeTransferIovDestroy(nativePtr);
        nativePtr = 0;
      }
    }
  }

  public static class Request implements AutoCloseable {
    private static final State[] CACHED_STATES = State.values();
    private final long[] scratchStats = new long[1];
    private long nativePtr;
    private final Semaphore semaphore;

    Request(long nativePtr, Semaphore semaphore) { 
      this.nativePtr = nativePtr; 
      this.semaphore = semaphore;
    }
    long getNativePtr() { return nativePtr; }

    public boolean isInProgress() {
      if (nativePtr == 0) throw new IllegalStateException("Closed");
      long t0 = RDMA_TRACKER_ENABLED ? System.nanoTime() : 0;
      try {
        int state = nativeRequestGetStatus(nativePtr, scratchStats);
        return CACHED_STATES[state] == State.InProgress;
      } finally {
        if (RDMA_TRACKER_ENABLED) {
          RDMATracker.record(RDMATracker.CallType.REQUEST_GET_STATUS, System.nanoTime() - t0);
        }
      }
    }

    public TransferStatus getStatus() {
      if (nativePtr == 0) throw new IllegalStateException("Closed");
      long t0 = RDMA_TRACKER_ENABLED ? System.nanoTime() : 0;
      try {
        int state = nativeRequestGetStatus(nativePtr, scratchStats);
        return new TransferStatus(CACHED_STATES[state], scratchStats[0]);
      } finally {
        if (RDMA_TRACKER_ENABLED) {
          RDMATracker.record(RDMATracker.CallType.REQUEST_GET_STATUS, System.nanoTime() - t0);
        }
      }
    }

    @Override
    public void close() {
      if (nativePtr != 0) {
        long t0 = RDMA_TRACKER_ENABLED ? System.nanoTime() : 0;
        try {
          nativeRequestDestroy(nativePtr);
        } finally {
          if (RDMA_TRACKER_ENABLED) {
            RDMATracker.record(RDMATracker.CallType.REQUEST_DESTROY, System.nanoTime() - t0);
          }
        }
        nativePtr = 0;
        if (semaphore != null) {
          semaphore.release();
        }
      }
    }
  }

  // =========================================================================
  // JNI Native Declarations
  // =========================================================================

  private static native long nativeCreate();
  private static native void nativeDestroy(long nativePtr);
  private static native void nativeInit(long nativePtr, Map<String, String> params);
  private static native byte[] nativeGetEndpointInfo(long nativePtr);
  private static native void nativeAddRemoteEndpoint(long nativePtr, String peerName, byte[] opaqueHandleBytes, boolean block);
  private static native void nativeConnect(long nativePtr, String peerName);
  private static native long nativeRegMem(long nativePtr, ByteBuffer buffer, long size, int memoryType);
  private static native void nativeDeregMem(long nativePtr, long memTokenPtr);
  private static native long nativeGetMemToken(long nativePtr, byte[] serTok);
  private static native long nativePostTransfer(long nativePtr, String remotePeer, int op, long localIovPtr, long remoteIovPtr, String notificationMessage);
  private static native void nativeNotify(long nativePtr, String remotePeer, String message);
  private static native byte[] nativeGetPeerNotification(long nativePtr, String remotePeer);

  private static native byte[] nativeMemTokenSerialize(long tokenPtr);
  private static native long nativeMemTokenGetAddress(long tokenPtr);
  private static native long nativeMemTokenGetSize(long tokenPtr);
  private static native void nativeMemTokenDelete(long tokenPtr);

  private static native long nativeTransferIovCreate(boolean remote);
  private static native void nativeTransferIovDestroy(long iovPtr);
  private static native void nativeTransferIovAddSegment(long iovPtr, long addr, long size, long tokenPtr);

  private static native int nativeRequestGetStatus(long reqPtr, long[] stats);
  private static native void nativeRequestDestroy(long reqPtr);

  private static java.io.File createTempDir() throws java.io.IOException {
    java.io.File tempDir = java.io.File.createTempFile("ml_transport_natives-", "");
    if (!tempDir.delete() || !tempDir.mkdir()) {
      throw new java.io.IOException("Failed to create temp directory: " + tempDir.getAbsolutePath());
    }
    tempDir.deleteOnExit();
    return tempDir;
  }

  private static java.io.File extractResource(java.io.File destDir, String resourcePath) throws java.io.IOException {
    java.io.InputStream in = CommsWrapper.class.getResourceAsStream(resourcePath);
    if (in == null) {
      throw new java.io.FileNotFoundException("Resource not found in JAR: " + resourcePath);
    }
    
    String filename = resourcePath.substring(resourcePath.lastIndexOf('/') + 1);
    java.io.File destFile = new java.io.File(destDir, filename);
    destFile.deleteOnExit();
    
    try (java.io.FileOutputStream out = new java.io.FileOutputStream(destFile)) {
      byte[] buffer = new byte[8192];
      int read;
      while ((read = in.read(buffer)) != -1) {
        out.write(buffer, 0, read);
      }
    }
    return destFile;
  }
}
