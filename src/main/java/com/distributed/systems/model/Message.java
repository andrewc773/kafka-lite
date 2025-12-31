package com.distributed.systems.model;

import java.util.zip.CRC32;

public class Message {
    private final long timestamp;
    private final byte[] key;
    private final byte[] value;
    private final long checksum;

    /**
     * Constructor for creating a new message from a producer.
     * Automatically assigns current timestamp and calculates CRC.
     */
    public Message(byte[] key, byte[] value) {
        this(System.currentTimeMillis(), key, value);
    }

    /**
     * Constructor used when loading a message back from disk.
     */
    public Message(long timestamp, byte[] key, byte[] value) {
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
        this.checksum = calculateChecksum();
    }

    private long calculateChecksum() {
        CRC32 crc = new CRC32();
        if (key != null) crc.update(key);
        if (value != null) crc.update(value);
        return crc.getValue();
    }

    // --- Getters ---

    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Returns a copy of the key to maintain immutability.
     */
    public byte[] getKey() {
        return key == null ? null : key.clone();
    }

    /**
     * Returns a copy of the value to maintain immutability.
     */
    public byte[] getValue() {
        return value == null ? null : value.clone();
    }

    public long getChecksum() {
        return checksum;
    }

    public int getKeySize() {
        return key == null ? -1 : key.length;
    }

    public int getValueSize() {
        return value == null ? 0 : value.length;
    }
}