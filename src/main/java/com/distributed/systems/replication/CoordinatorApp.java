package com.distributed.systems.replication;

import com.distributed.systems.model.BrokerAddress;
import com.distributed.systems.util.Logger;
import com.distributed.systems.util.ShellColors;

import java.util.ArrayList;
import java.util.List;

public class CoordinatorApp {
    public static void main(String[] args) {
        ShellColors.printBanner();
        Logger.logBootstrap("Initializing Kafka-Lite Standalone Coordinator...");

        BrokerAddress primary = new BrokerAddress("localhost", 9001);

        List<BrokerAddress> followers = new ArrayList<>();
        followers.add(new BrokerAddress("localhost", 9002));
        followers.add(new BrokerAddress("localhost", 9003));

        ClusterController controller = new ClusterController(primary, followers);
        Thread controllerThread = new Thread(controller, "Coordinator-Main");
        controllerThread.start();

        Logger.logInfo("Coordinator is live. Monitoring " + (followers.size() + 1) + " nodes.");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Logger.logWarning("Shutdown signal received. Closing Coordinator...");
            controllerThread.interrupt();
        }));
    }
}