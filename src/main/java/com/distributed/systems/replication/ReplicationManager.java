package com.distributed.systems.replication;


import com.distributed.systems.config.BrokerConfig;
import com.distributed.systems.storage.Log;
import com.distributed.systems.storage.TopicManager;
import com.distributed.systems.util.Logger;
import com.distributed.systems.util.Protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*This class will manage one topic per Replica Fetcher (thread). This creates simplicity for state management as each thread only needs
 * to worry about keeping up to date with one topic. */

public class ReplicationManager {

    private final TopicManager topicManager;
    private final BrokerConfig config;
    private final ExecutorService fetcherPool;

    // Keep track of which topics already have a running fetcher
    private final Map<String, ReplicaFetcher> activeFetchers = new ConcurrentHashMap<>();

    public ReplicationManager(TopicManager topicManager, BrokerConfig config) {
        this.topicManager = topicManager;
        this.config = config;
        this.fetcherPool = Executors.newCachedThreadPool();
    }

    public void start() {
        if (config.isLeader()) {
            Logger.logBootstrap("Broker is LEADER. ReplicationManager sitting idle.");
            return;
        }

        // Periodically check for new topics to replicate
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(
                this::refreshFetchers, 0, 5, java.util.concurrent.TimeUnit.SECONDS
        );
    }


    /* Checks all topics of leader to check if those exist yet in follower server. If not, create before we catch up to speed*/
    private void refreshFetchers() {
        try (Socket socket = new Socket(config.getLeaderHost(), config.getLeaderPort());
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {

            out.writeUTF(Protocol.CMD_LIST_TOPICS);
            out.flush();

            int topicCount = in.readInt();

            for (int i = 0; i < topicCount; i++) {

                String topic = in.readUTF();

                if (!activeFetchers.containsKey(topic)) {
                    Logger.logBootstrap("Discovered new topic on leader: " + topic);

                    Log localLog = topicManager.getOrCreateLog(topic);

                    ReplicaFetcher fetcher = new ReplicaFetcher(
                            topic,
                            localLog,
                            config.getLeaderHost(),
                            config.getLeaderPort()
                    );

                    activeFetchers.put(topic, fetcher);
                    fetcherPool.submit(fetcher);
                }
            }

            out.writeUTF(Protocol.CMD_QUIT);
            out.flush();

        } catch (IOException e) {
            Logger.logError("Failed to discover topics from leader: " + e.getMessage());
        }
    }

    public void shutdown() {
        activeFetchers.values().forEach(ReplicaFetcher::stop);
        fetcherPool.shutdownNow();
    }


}
