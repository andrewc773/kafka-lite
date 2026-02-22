package com.distributed.systems.config;

import java.io.InputStream;
import java.util.Properties;

public class BrokerConfig {
    private final Properties properties = new Properties();

    public BrokerConfig() {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("config.properties")) {
            if (input != null) {
                properties.load(input);
            }
        } catch (Exception e) {
            System.err.println("Could not load config.properties, using defaults.");
        }
    }

    // Constructor for Unit Tests
    public BrokerConfig(long maxSegmentSize, long retentionMs, long indexInterval, long cleanupInterval) {
        properties.setProperty("storage.max.segment.size", String.valueOf(maxSegmentSize));
        properties.setProperty("storage.retention.ms", String.valueOf(retentionMs));
        properties.setProperty("storage.index.interval.bytes", String.valueOf(indexInterval));
        properties.setProperty("storage.cleanup.interval.ms", String.valueOf(cleanupInterval));
    }

    public boolean isLeader() {
        // Default to true so a single broker works out of the box
        return Boolean.parseBoolean(properties.getProperty("replication.is.leader", "true"));
    }

    public String getLeaderHost() {
        return properties.getProperty("replication.leader.host", "localhost");
    }

    public int getLeaderPort() {
        return Integer.parseInt(properties.getProperty("replication.leader.port", "9092"));
    }

    public long getMaxSegmentSize() {
        return Long.parseLong(properties.getProperty("storage.max.segment.size", "2048"));
    }

    public long getIndexIntervalBytes() {
        return Long.parseLong(properties.getProperty("storage.index.interval.bytes", "2048"));
    }

    public long getRetentionMs() {
        return Long.parseLong(properties.getProperty("storage.retention.ms", "300000"));
    }

    public long getCleanupIntervalMs() {
        return Long.parseLong(properties.getProperty("storage.cleanup.interval.ms", "60000"));
    }

    public void setProperty(String key, String value) {
        properties.setProperty(key, value);
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }
}