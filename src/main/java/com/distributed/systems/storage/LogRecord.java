package com.distributed.systems.storage;

/**
 * Represents a single message stored in the log.
 * Wraps the Timestamp, Key, and Value.
 */
public record LogRecord(long offset, long timestamp, byte[] key, byte[] value) {

    // calculate how many bytes this record takes up on disk
    public int sizeInBytes() {
        // Timestamp (8) + KeyLength (4) + Key (N) + ValLength (4) + Value (M)
        return 8 + 4 + key.length + 4 + value.length;
    }

    @Override
    public String toString() {
        return String.format("Record{offset=%d, ts=%d, key=%s, valSize=%d}",
                offset, timestamp, new String(key), value.length);
    }
}