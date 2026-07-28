package rdma_comms;

import org.apache.celeborn.common.CelebornConf;
import org.apache.spark.api.plugin.DriverPlugin;
import org.apache.spark.api.plugin.ExecutorPlugin;
import org.apache.spark.api.plugin.PluginContext;
import org.apache.spark.api.plugin.SparkPlugin;
import org.apache.spark.shuffle.celeborn.SparkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class RDMAPlugin implements SparkPlugin {
    @Override
    public DriverPlugin driverPlugin() {
        return null;
    }

    @Override
    public ExecutorPlugin executorPlugin() {
        return new CelebornExecutorPlugin();
    }

    public static class CelebornExecutorPlugin implements ExecutorPlugin {
        private static final Logger logger = LoggerFactory.getLogger(CelebornExecutorPlugin.class);

        @Override
        public void init(PluginContext ctx, Map<String, String> extraConf) {
            boolean rdmaEnabled = ctx.conf().getBoolean("spark.celeborn.rdma.enabled", false);
            if (!rdmaEnabled) {
                logger.debug("Celeborn RDMA is not enabled (spark.celeborn.rdma.enabled=false). Skipping eager CommsClient initialization.");
                return;
            }

            logger.info("Eagerly initializing Celeborn RDMA CommsClient inside Spark ExecutorPlugin...");
            CelebornConf celebornConf = SparkUtils.fromSparkConf(ctx.conf());
            
            Thread initThread = new Thread(() -> {
                try {
                    CommsClient.getOrCreate(celebornConf);
                    logger.info("Celeborn RDMA CommsClient eager initialization completed successfully.");
                } catch (Exception e) {
                    logger.error("Failed to eagerly initialize Celeborn RDMA CommsClient", e);
                }
            });
            initThread.setName("RDMA-Eager-Init");
            initThread.setDaemon(true);
            initThread.start();
            logger.info("Triggered eager RDMA CommsClient setup in the background.");
        }

        @Override
        public void shutdown() {
        }
    }
}
