package com.distributed.systems;

import com.distributed.systems.config.BrokerConfig;
import com.distributed.systems.server.BrokerServer;
import com.distributed.systems.util.Logger;

import java.nio.file.Paths;

public class Main {
    public static void main(String[] args) {
        // Default to 9001 if no port is provided
        int port = 9001;
        if (args.length >= 1) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                Logger.logError("Invalid port provided. Usage: java -cp ... Main <port> [dataDir]");
                System.exit(1);
            }
        }

        // Default data directory based on port to avoid conflicts
        String dataDir = (args.length >= 2) ? args[1] : "data/broker-" + port;

        Logger.logBanner();

        try {
            BrokerConfig config = new BrokerConfig();
            BrokerServer server = new BrokerServer(port, dataDir, config);

            Logger.logInfo("Node Identity: localhost:" + port);
            Logger.logInfo("Disk Path: " + Paths.get(dataDir).toAbsolutePath());
            Logger.logBootstrap("Broker is initializing storage and networking...");

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                Logger.logWarning("Shutdown signal received. Closing segments and socket...");
                server.stop();
                Logger.logInfo("Safe exit completed.");
            }));

            // blocking call that starts the socket listener
            server.start();

        } catch (Exception e) {
            Logger.logError("Critical start-up failure: " + e.getMessage());
            e.printStackTrace();
        }
    }
}