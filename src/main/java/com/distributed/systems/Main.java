package com.distributed.systems;

import com.distributed.systems.network.BrokerServer;
import com.distributed.systems.storage.LogSegment;

import java.nio.file.Paths;

public class Main {
    public static void main(String[] args) {
        try {
            // Define the port (9092 is the standard for Kafka)
            int port = 9092;

            String dataDir = "kafka-logs";

            System.out.println("\u001B[36m" + "=".repeat(40));
            System.out.println("       KAFKA-LITE BROKER v1.0");
            System.out.println("=".repeat(40) + "\u001B[0m");

            // Internally creating log manager and socket
            BrokerServer server = new BrokerServer(port, dataDir);

            System.out.printf("[BOOT] Port: %d%n", port);
            System.out.printf("[BOOT] Data Directory: %s%n", Paths.get(dataDir).toAbsolutePath());
            

            System.out.println("\u001B[32m[READY] Broker is online and awaiting connections...\u001B[0m");
            System.out.println("-".repeat(40));

            //stays alive w/ while(true) block
            server.start();


        } catch (Exception e) {
            System.err.println("Critical Failure: Could not start broker - " + e.getMessage());
            e.printStackTrace();
        }
    }
}
