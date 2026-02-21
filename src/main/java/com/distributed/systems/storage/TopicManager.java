package com.distributed.systems.storage;

import com.distributed.systems.config.BrokerConfig;
import com.distributed.systems.util.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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
        } else {
            try (var stream = Files.list(dataRootDir)) {
                stream.filter(Files::isDirectory)
                        .forEach(path -> {
                            String topicName = path.getFileName().toString();
                            try {
                                // preload the log into our map
                                topicMap.put(topicName, new Log(path, config));
                                Logger.logInfo("Recovered topic: " + topicName);
                            } catch (IOException e) {
                                Logger.logError("Failed to recover topic " + topicName);
                            }
                        });
            }
        }
    }

    public Log getOrCreateLog(String topicName) {
        return topicMap.computeIfAbsent(topicName, name -> {
            try {
                validateTopicName(topicName);
                Path topicDir = dataRootDir.resolve(name);
                Logger.logInfo("Initializing storage for topic: " + name);
                return new Log(topicDir, config);
            } catch (IOException e) {
                throw new RuntimeException("Could not initialize log for topic: " + name, e);
            }
        });
    }

    public List<String> getAllTopics() {
        return new ArrayList<>(topicMap.keySet());
    }

    public Log getLogIfExits(String topicName) {
        return topicMap.get(topicName);
    }

    /*Shutdown used for graceful end once server comes to stop*/
    public void shutdown() {
        Logger.logInfo("Shutting down TopicManager and closing all logs...");
        topicMap.forEach((name, log) -> {
            try {
                log.close();
                Logger.logInfo("Closed topic: " + name);
            } catch (IOException e) {
                Logger.logError("Failed to close log for topic: " + name);
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

    /*
     * Validate topic name
     * */
    private void validateTopicName(String name) {
        if (name == null || !name.matches("^[a-zA-Z0-9_-]+$")) {
            throw new IllegalArgumentException("Invalid topic name: " + name);
        }
    }

}
