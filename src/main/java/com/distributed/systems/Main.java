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

            System.out.println("Initializing Kafka-lite");

            // Internally creating log manager and socket
            BrokerServer server = new BrokerServer(port, dataDir);

            //stays alive w/ while(true) block
            server.start();


        } catch (Exception e) {
            System.err.println("Critical Failure: Could not start broker - " + e.getMessage());
            e.printStackTrace();
        }
    }
}
