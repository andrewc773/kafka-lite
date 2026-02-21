package com.distributed.systems.replication;


import com.distributed.systems.config.BrokerConfig;
import com.distributed.systems.storage.TopicManager;
import com.distributed.systems.util.Logger;

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

    private void refreshFetchers() {
        for (String topic : topicManager.getAllTopics()) {

            if (!activeFetchers.containsKey(topic)) {
                Logger.logBootstrap("Initializing new replica fetcher for topic: " + topic);

                ReplicaFetcher fetcher = new ReplicaFetcher(
                        topic,
                        topicManager.getLogIfExits(topic),
                        config.getLeaderHost(),
                        config.getLeaderPort()
                );

                activeFetchers.put(topic, fetcher);
                fetcherPool.submit(fetcher);

            }
        }
    }

    public void shutdown() {
        activeFetchers.values().forEach(ReplicaFetcher::stop);
        fetcherPool.shutdownNow();
    }


}
