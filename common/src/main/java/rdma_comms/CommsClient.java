package rdma_comms;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommsClient {
  private static final Logger logger = LoggerFactory.getLogger(CommsClient.class);

  private final String transportType;
  private final String localPeerName;
  private final String serverIp;
  private final int oobPort;
  private final String serverPeerName;
  private final boolean testMode;

  // Persistent objects
  private CommsWrapper comms;
  private CommsWrapper.MemToken localToken;
  private CommsWrapper.MemToken remoteToken;
  private CommsWrapper.TransferIov localIov;
  private CommsWrapper.TransferIov remoteIov;
  private ByteBuffer localBuffer;

  private long remoteAddrVal;
  private long remoteSize;

  /**
   * Constructor.
   * @param transportType Transport to use: "TCP" or "RDMA".
   * @param localPeerName Local name for this client.
   * @param serverIp IP of the remote OOB server.
   * @param oobPort Port of the remote OOB server.
   * @param serverPeerName Name of the remote server.
   * @param testMode Whether to trigger read test.
   */
  public CommsClient(String transportType, String localPeerName, String serverIp, 
                int oobPort, String serverPeerName, boolean testMode) {
    this.transportType = transportType;
    this.localPeerName = localPeerName;
    this.serverIp = serverIp;
    this.oobPort = oobPort;
    this.serverPeerName = serverPeerName;
    this.testMode = testMode;
  }

  /**
   * Sets up the CommsClient Control Plane:
   * 1. Initializes Comms library.
   * 2. Connects to OOB TCP server and receives server handles/tokens.
   * 3. Establishes transport connection via addRemoteEndpoint.
   * 4. Allocates and registers local buffer.
   * 5. Prepares Transfer IOVs.
   * 
   * Once this method returns, the control connection is established and 
   * the client is ready to write data over the data plane.
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
      int maxRetries = 30; // Retry 30 times with 500ms sleep = 15 seconds total
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
        this.remoteAddrVal = in.readLong();
        logger.debug("Read remote memory address: 0x{}", Long.toHexString(remoteAddrVal));
        
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
        logger.info("Added remote endpoint and connected.");

        // 4. Allocate local buffer and register it
        this.localBuffer = ByteBuffer.allocateDirect((int) remoteSize);
        // Fill local buffer with default test sequence
        // for (int i = 0; i < remoteSize; i++) {
        //   localBuffer.put(i, (byte) (i & 0xFF));
        // }

        this.localToken = comms.regMem(localBuffer, remoteSize, CommsWrapper.MemoryType.Dram);
        this.remoteToken = comms.getMemToken(remoteTokenOpaque);

        // 5. Prepare IOVs
        this.localIov = new CommsWrapper.TransferIov(false); // Local is sender
        localIov.addSegment(CommsWrapper.getDirectBufferAddress(localBuffer), remoteSize, localToken);

        this.remoteIov = new CommsWrapper.TransferIov(true); // Remote is receiver
        remoteIov.addSegment(remoteAddrVal, remoteSize, remoteToken);

        logger.info("Control plane setup complete. Ready for RDMA transfer.");

        if (testMode) {
          logger.info("Test mode enabled. Running read test...");
          runReadTest();
        }
      }
    } catch (Exception e) {
      logger.error("Control setup failed", e);
      shutdown();
    }
  }

  /**
   * Triggers a synchronous RDMA Write transfer from local buffer to remote buffer.
   * Blocks until the hardware signals completion.
   */
  public void writeData() throws IOException {
    if (comms == null || localIov == null || remoteIov == null) {
      throw new IllegalStateException("CommsClient not setup");
    }

    try (CommsWrapper.Request req = comms.postTransfer(serverPeerName, CommsWrapper.TransferOpType.Write, localIov, remoteIov, "")) {
      CommsWrapper.TransferStatus status;
      do {
        status = req.getStatus();
        Thread.yield();
      } while (status.state == CommsWrapper.State.InProgress);

      if (status.state != CommsWrapper.State.Done) {
        throw new IOException("RDMA Transfer failed with state: " + status.state);
      }
      logger.info("RDMA Write transfer complete.");
    }
  }

  /**
   * Sends a "Done" notification to the server over the synchronization plane.
   */
  public void notifyDone() {
    if (comms != null) {
      comms.notify(serverPeerName, "Done");
      logger.info("Sent Done notification to server.");
    }
  }

  /**
   * Releases resources, memory registrations and IOVs.
   */
  public void shutdown() {
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
    if (localIov != null) {
      localIov.close();
      localIov = null;
    }
    if (remoteIov != null) {
      remoteIov.close();
      remoteIov = null;
    }
    if (comms != null) {
      comms.close();
      logger.info("Comms closed.");
      comms = null;
    }
  }

  // Getters for accessing components
  public CommsWrapper getComms() { return comms; }
  public ByteBuffer getLocalBuffer() { return localBuffer; }
  public long getRemoteSize() { return remoteSize; }

  public void runReadTest() {
    try {
      logger.info("Waiting for READY notification...");
      byte[] msgBytes = null;
      while (msgBytes == null) {
        Thread.sleep(100);
        msgBytes = comms.getPeerNotification(serverPeerName);
      }
      
      String msg = new String(msgBytes);
      logger.info("Received notification: {}", msg);
      
      if ("READY".equals(msg)) {
        logger.info("Triggering RDMA read pull...");
        try (CommsWrapper.Request req = comms.postTransfer(serverPeerName, CommsWrapper.TransferOpType.Read, localIov, remoteIov, "")) {
          CommsWrapper.TransferStatus status;
          do {
            status = req.getStatus();
            Thread.yield();
          } while (status.state == CommsWrapper.State.InProgress);
          
          if (status.state != CommsWrapper.State.Done) {
            throw new IOException("RDMA Read failed with state: " + status.state);
          }
          
          logger.info("RDMA Read complete. Verifying...");
          boolean verified = true;
          for (int i = 0; i < 100; i++) {
            byte got = localBuffer.get(i);
            byte expected = (byte) (i & 0xFF);
            if (got != expected) {
              logger.error("Mismatch at offset {}: got {}, expected {}", i, got, expected);
              verified = false;
            }
          }
          
          if (verified) {
            logger.info("SUCCESS! RDMA Read verified over {}!", transportType);
          } else {
            logger.error("FAILED! Memory verification mismatch.");
          }
        }
      }
    } catch (Exception e) {
      logger.error("Test failed", e);
    }
  }
}
