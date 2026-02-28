package com.distributed.systems.replication;

import com.distributed.systems.config.BrokerConfig;
import com.distributed.systems.model.BrokerAddress;
import com.distributed.systems.server.BrokerServer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/*Test class used for Chaos testing. It mocks a cluster with several brokers.*/

public class ClusterMock {
    private final Map<Integer, BrokerServer> brokers = new HashMap<>();
    private final Map<Integer, Thread> threads = new HashMap<>();
    private final String baseDir;

    public ClusterMock(String baseDir) {
        this.baseDir = baseDir;
    }

    public BrokerAddress startBroker(int port, boolean isLeader) throws IOException {
        BrokerConfig config = new BrokerConfig();
        config.setProperty("replication.is.leader", String.valueOf(isLeader));

        // Ensure each broker has its own unique data sub-directory
        String dataPath = baseDir + "/broker-" + port;
        BrokerServer server = new BrokerServer(port, dataPath, config);

        brokers.put(port, server);
        Thread t = new Thread(server::start);
        t.start();
        threads.put(port, t);

        return new BrokerAddress("localhost", port);
    }

    public void stopBroker(int port) {
        if (brokers.containsKey(port)) {
            brokers.get(port).stop();
            threads.get(port).interrupt();
        }
    }

    public BrokerServer getBroker(int port) {
        return brokers.get(port);
    }

    public void shutdownAll() {
        brokers.values().forEach(BrokerServer::stop);
        threads.values().forEach(Thread::interrupt);
    }
}