package com.distributed.systems.model;

import java.nio.ByteBuffer;
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

    /**
     * Converts the Message object into a structured binary array.
     */
    public byte[] serialize() {
        // Calculate total size
        // Magic(1) + CRC(4) + Timestamp(8) + KeyLen(4) + Key(N) + ValLen(4) + Val(N)
        int size = 1 + 4 + 8 + 4 + (key == null ? 0 : key.length) + 4 + value.length;

        ByteBuffer buffer = ByteBuffer.allocate(size);

        //  Pack headers
        buffer.put((byte) 1);              // Magic Byte (Version 1)
        buffer.putInt((int) checksum);     // CRC32
        buffer.putLong(timestamp);         // Timestamp

        // pack the key
        if (key == null) {
            buffer.putInt(-1);             // represents null in binary
        } else {
            buffer.putInt(key.length);
            buffer.put(key);
        }

        // pack the val
        buffer.putInt(value.length);
        buffer.put(value);

        return buffer.array();
    }

    /* Static helper to turn bytes back into a Message object.
     */
    public static Message deserialize(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        byte magic = buffer.get();
        int savedChecksum = buffer.getInt();
        long timestamp = buffer.getLong();

        // Read Key
        int keyLen = buffer.getInt();
        byte[] key = null;
        if (keyLen != -1) {
            key = new byte[keyLen];
            buffer.get(key);
        }

        // Read Value
        int valLen = buffer.getInt();
        byte[] value = new byte[valLen];
        buffer.get(value);

        return new Message(timestamp, key, value);
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