package rdma_comms;

import java.nio.ByteBuffer;
import java.util.Map;

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

  // Holds the raw pointer to the C++ comms::Comms object.
  private final long nativePtr;

  /**
   * Constructor. Creates the underlying C++ Comms object.
   * Equivalent to 'comms::Comms::Comms()'.
   */
  public CommsWrapper() {
    this.nativePtr = nativeCreate();
  }

  /**
   * Initializes the comms instance.
   * Equivalent to 'absl::Status comms::Comms::Init(const Params& params)'.
   */
  public void init(Map<String, String> params) {
    nativeInit(nativePtr, params);
  }

  /**
   * Retrieves serialized connection handle of the local instance (opaque bytes).
   * Equivalent to 'absl::StatusOr<size_t> comms::Comms::GetEndpointInfo(opaque_data_t& data)'.
   */
  public byte[] getEndpointInfo() {
    return nativeGetEndpointInfo(nativePtr);
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
    nativeAddRemoteEndpoint(nativePtr, peerName, opaqueHandleBytes, block);
  }

  /**
   * Explicitly connects to a remote peer.
   * Equivalent to 'absl::Status comms::Comms::Connect(const std::string&)'.
   */
  public void connect(String peerName) {
    nativeConnect(nativePtr, peerName);
  }

  /**
   * Registers a local direct ByteBuffer.
   * Equivalent to 'absl::StatusOr<unique_ptr<MemToken>> comms::Comms::RegMem(...)'.
   */
  public MemToken regMem(ByteBuffer buffer, long size, MemoryType type) {
    if (!buffer.isDirect()) {
      throw new IllegalArgumentException("Buffer must be direct");
    }
    long tokenPtr = nativeRegMem(nativePtr, buffer, size, type.ordinal());
    return new MemToken(tokenPtr);
  }

  /**
   * Deregisters a local memory token and closes it.
   * Equivalent to 'absl::Status comms::Comms::DeregMem(MemToken&)'
   */
  public void deregMem(MemToken token) {
    nativeDeregMem(nativePtr, token.getNativePtr());
    token.close(); 
  }

  /**
   * Deserializes a remote memory token from opaque bytes.
   * Equivalent to 'absl::StatusOr<unique_ptr<MemToken>> comms::Comms::GetMemToken(const opaque_data_t&)'.
   */
  public MemToken getMemToken(byte[] serTok) {
    long tokenPtr = nativeGetMemToken(nativePtr, serTok);
    return new MemToken(tokenPtr);
  }

  /**
   * Posts an asynchronous transfer operation.
   * Equivalent to 'absl::StatusOr<unique_ptr<Request>> comms::Comms::PostTransfer(...)'.
   */
  public Request postTransfer(String remotePeer, TransferOpType op, TransferIov local, TransferIov remote, String notificationMessage) {
    long reqPtr = nativePostTransfer(nativePtr, remotePeer, op.ordinal(), local.getNativePtr(), remote.getNativePtr(), notificationMessage);
    return new Request(reqPtr);
  }

  /**
   * Sends a standalone notification message.
   * Equivalent to 'absl::Status comms::Comms::Notify(const std::string&, const std::string&)'.
   */
  public void notify(String remotePeer, String message) {
    nativeNotify(nativePtr, remotePeer, message);
  }

  /**
   * Retrieves the raw message bytes of the earliest unprocessed notification for a peer.
   * Internally extracts the message from 'NotificationProto' in C++ JNI layer.
   * Returns null if no notification is pending.
   */
  public byte[] getPeerNotification(String remotePeer) {
    return nativeGetPeerNotification(nativePtr, remotePeer);
  }

  /**
   * Destructor. Frees the underlying C++ Comms object.
   * Equivalent to 'comms::Comms::~Comms()'.
   */
  @Override
  public void close() {
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
    private long nativePtr;
    Request(long nativePtr) { this.nativePtr = nativePtr; }
    long getNativePtr() { return nativePtr; }

    public TransferStatus getStatus() {
      if (nativePtr == 0) throw new IllegalStateException("Closed");
      long[] stats = new long[1];
      int state = nativeRequestGetStatus(nativePtr, stats);
      return new TransferStatus(State.values()[state], stats[0]);
    }
    @Override
    public void close() {
      if (nativePtr != 0) {
        nativeRequestDestroy(nativePtr);
        nativePtr = 0;
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
