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
        String dataDir = "data/broker-" + port;
        int argIndex = 1;
        if (args.length >= 2 && !isRoleArg(args[1])) {
            dataDir = args[1];
            argIndex = 2;
        }

        Logger.logBanner();

        try {
            BrokerConfig config = new BrokerConfig();
            applyRoleArgs(config, args, argIndex);
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

    private static boolean isRoleArg(String arg) {
        return "leader".equalsIgnoreCase(arg) || "follower".equalsIgnoreCase(arg);
    }

    private static void applyRoleArgs(BrokerConfig config, String[] args, int argIndex) {
        if (args.length <= argIndex) {
            return;
        }

        String role = args[argIndex];
        if (!isRoleArg(role)) {
            return;
        }

        if ("leader".equalsIgnoreCase(role)) {
            config.setProperty("replication.is.leader", "true");
            return;
        }

        config.setProperty("replication.is.leader", "false");

        String leaderHost = (args.length > argIndex + 1) ? args[argIndex + 1] : "localhost";
        String leaderPort = (args.length > argIndex + 2) ? args[argIndex + 2] : "9001";

        if (args.length <= argIndex + 2) {
            Logger.logWarning("Follower started without leader host/port; defaulting to " + leaderHost + ":" + leaderPort);
        }

        config.setProperty("replication.leader.host", leaderHost);
        config.setProperty("replication.leader.port", leaderPort);
    }
}
