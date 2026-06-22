package rdma_comms;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommsServer {
  private static final Logger logger = LoggerFactory.getLogger(CommsServer.class);

  private final String transportType;
  private final String localPeerName;
  private final String localIp;
  private final int bootstrapPort;
  private final int bufferSize;
  private final int oobPort;
  private final boolean testMode;

  // Persistent fields kept alive after setup completes
  private CommsWrapper comms;
  private CommsWrapper.MemToken localToken;
  private ByteBuffer localBuffer;
  private ServerSocket listenSock;
  private final Map<String, String> clientPeerNames = new ConcurrentHashMap<>();

  /**
   * Constructor.
   * @param transportType Transport to use: "TCP" or "RDMA".
   * @param localPeerName Local name for this node.
   * @param localIp Routable local IP address to bind the bootstrap server to.
   * @param bootstrapPort Port for bootstrap TCP server.
   * @param bufferSize Size of the direct memory buffer to allocate and register.
   * @param oobPort Port for out-of-band metadata sharing TCP server.
   * @param testMode Whether to trigger read test.
   */
  public CommsServer(String transportType, String localPeerName, String localIp, 
                     int bootstrapPort, int bufferSize, int oobPort, boolean testMode) {
    this.transportType = transportType;
    this.localPeerName = localPeerName;
    this.localIp = localIp;
    this.bootstrapPort = bootstrapPort;
    this.bufferSize = bufferSize;
    this.oobPort = oobPort;
    this.testMode = testMode;
  }

  /**
   * Sets up the Control Plane:
   * 1. Initializes the Comms library.
   * 2. Allocates a direct memory buffer.
   * 3. Registers the buffer with the transport layer.
   * 4. Starts the OOB TCP server and shares handles/keys with the client.
   * 
   * Once this method returns, the server is ready for the client to perform
   * RDMA transfers into the buffer. Sockets are closed, but the Comms instance 
   * and Memory Registration remain active.
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
      params.put("AP_BOOTSTRAP_PORT", String.valueOf(bootstrapPort));
      params.put("AP_BOOTSTRAP_IP", localIp);

      comms.init(params);
      logger.info("Comms library initialized.");

      // 2. Allocate direct memory buffer (for registration)
      this.localBuffer = ByteBuffer.allocateDirect(bufferSize);
      for (int i = 0; i < bufferSize; i++) {
        localBuffer.put(i, (byte) 0);
      }

      // 3. Register memory region
      this.localToken = comms.regMem(localBuffer, bufferSize, CommsWrapper.MemoryType.Dram);
      logger.info("DRAM memory region registered with transport layer.");

      // Prepare exchange metadata
      byte[] serverHandleOpaque = comms.getEndpointInfo();
      long handleSize = serverHandleOpaque.length;
      byte[] serverTokenOpaque = localToken.serialize();
      long addrVal = CommsWrapper.getDirectBufferAddress(localBuffer);

      // 4. Start OOB TCP CommsServer to share handles in a loop
      final long finalHandleSize = handleSize;
      final byte[] finalServerHandleOpaque = serverHandleOpaque;
      final long finalAddrVal = addrVal;
      final byte[] finalServerTokenOpaque = serverTokenOpaque;

      new Thread(() -> {
        try {
          this.listenSock = new ServerSocket(oobPort);
          logger.info("Starting OOB server on port {}...", oobPort);

          while (!listenSock.isClosed()) {
            try {
              Socket clientSock = listenSock.accept();
              String clientIp = clientSock.getInetAddress().getHostAddress();
              logger.info("OOB Client connected from {}", clientSock.getRemoteSocketAddress());

              try (DataInputStream in = new DataInputStream(clientSock.getInputStream());
                  DataOutputStream out = new DataOutputStream(clientSock.getOutputStream())) {
                
                logger.debug("Reading clientPeerName (blocking readUTF)...");
                String clientPeerName = in.readUTF();
                logger.debug("Read clientPeerName: {}", clientPeerName);
                
                logger.debug("Reading clientHandleSize (blocking readLong)...");
                long clientHandleSize = in.readLong();
                logger.debug("Read clientHandleSize: {}", clientHandleSize);
                
                byte[] clientHandleOpaque = new byte[(int) clientHandleSize];
                logger.debug("Reading clientHandleOpaque bytes (blocking readFully, length: {})...", clientHandleSize);
                in.readFully(clientHandleOpaque);
                logger.debug("Read clientHandleOpaque bytes successfully.");
                
                clientPeerNames.put(clientIp, clientPeerName);
                logger.info("Registered Client: {} IP: {}", clientPeerName, clientIp);

                // Register Client remote endpoint in CommsServer's context
                logger.debug("Calling comms.addRemoteEndpoint (blocking=true)...");
                comms.addRemoteEndpoint(clientPeerName, clientHandleOpaque, true);
                logger.debug("comms.addRemoteEndpoint returned successfully.");

                logger.debug("Writing finalHandleSize: {}", finalHandleSize);
                out.writeLong(finalHandleSize);
                logger.debug("Writing finalServerHandleOpaque bytes (length: {})...", finalServerHandleOpaque.length);
                out.write(finalServerHandleOpaque);
                logger.debug("Writing finalAddrVal: 0x{}", Long.toHexString(finalAddrVal));
                out.writeLong(finalAddrVal);
                logger.debug("Writing bufferSize: {}", bufferSize);
                out.writeLong(bufferSize);
                logger.debug("Writing finalServerTokenOpaque bytes...");
                out.write(finalServerTokenOpaque);
                logger.debug("Flushing output stream...");
                out.flush();
                logger.info("Shared handles sent and flushed to client: {}", clientSock.getRemoteSocketAddress());

                if (testMode) {
                  logger.info("Test Mode enabled. Triggering Read test for: {}", clientPeerName);
                  runReadTest(clientPeerName);
                }
              } finally {
                clientSock.close();
              }
            } catch (IOException e) {
              if (!listenSock.isClosed()) {
                logger.error("Client connection handle error", e);
              }
            }
          }
        } catch (IOException e) {
          logger.error("CommsServer socket failed", e);
        }
      }, "OOB-CommsServer-Accept-Loop").start();

    } catch (Exception e) {
      logger.error("Comms initialization failed", e);
      // Clean up whatever was initialized if setup fails
      shutdown();
    }
  }

  /**
   * Call this when the server shuts down to release the registered memory
   * and close resources.
   */
  public void shutdown() {
    if (listenSock != null) {
      try {
        listenSock.close();
        logger.info("OOB ServerSocket closed.");
      } catch (Exception e) {
        logger.error("Failed to close OOB ServerSocket", e);
      }
    }
    if (localToken != null) {
      try {
        comms.deregMem(localToken);
        logger.info("Memory deregistered.");
        localToken = null;
      } catch (Exception e) {
        logger.error("Failed to clean up memory registration", e);
      }
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

  // Getters for external access
  public CommsWrapper getComms() {
    return comms;
  }

  public CommsWrapper.MemToken getLocalToken() {
    return localToken;
  }

  public ByteBuffer getLocalBuffer() {
    return localBuffer;
  }

  public String getClientPeerName(String clientIp) {
    return clientPeerNames.get(clientIp);
  }

  public void runReadTest(String clientPeerName) {
    new Thread(() -> {
      try {
        logger.info("Populating test sequence into DRAM...");
        ByteBuffer buf = getLocalBuffer();
        for (int i = 0; i < 100; i++) {
          buf.put(i, (byte) (i & 0xFF));
        }
        
        // Sleep briefly to ensure client is waiting
        Thread.sleep(1000);
        
        logger.info("Sending READY notification to {}...", clientPeerName);
        getComms().notify(clientPeerName, "READY");
      } catch (Exception e) {
        logger.error("Test CommsServer failed", e);
      }
    }, "CommsServer-Test-Thread").start();
  }
}
