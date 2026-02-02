package com.distributed.systems.storage;

import com.distributed.systems.config.BrokerConfig;
import com.distributed.systems.util.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

public class TopicManager {

    private final Path dataRootDir;
    private final BrokerConfig config;
    private final ConcurrentHashMap<String, Log> topicMap = new ConcurrentHashMap<>();

    public TopicManager(Path dataRootDir, BrokerConfig config) throws IOException {
        this.dataRootDir = dataRootDir;
        this.config = config;

        if (!Files.exists(dataRootDir)) {
            Files.createDirectories(dataRootDir);
        }
    }

    public Log getOrCreateLog(String topicName) {
        return topicMap.computeIfAbsent(topicName, name -> {
            try {
                Path topicDir = dataRootDir.resolve(name);
                Logger.logInfo("Initializing storage for topic: " + name);
                return new Log(topicDir, config);
            } catch (IOException e) {
                throw new RuntimeException("Could not initialize log for topic: " + name, e);
            }
        });
    }

    /**
     * Returns total disk usage across all topics.
     */
    public long getTotalDiskUsage() {
        return topicMap.values().stream()
                .mapToLong(Log::getTotalDiskUsage)
                .sum();
    }

}
