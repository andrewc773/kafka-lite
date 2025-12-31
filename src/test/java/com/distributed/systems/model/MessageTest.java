package com.distributed.systems.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MessageTest {

    @Test
    void testSerializationRoundTrip() {

        byte[] key = "user-123".getBytes();
        byte[] value = "Hello, Kafka-Lite!".getBytes();
        Message original = new Message(key, value);

        // Serialize to binary
        byte[] serialized = original.serialize();
        assertNotNull(serialized);
        assertTrue(serialized.length > value.length); // Headers should add size

        //  Deserialize back to object
        Message recovered = Message.deserialize(serialized);

        // verify
        assertArrayEquals(original.getKey(), recovered.getKey(), "Key should match");
        assertArrayEquals(original.getValue(), recovered.getValue(), "Value should match");
        assertEquals(original.getTimestamp(), recovered.getTimestamp(), "Timestamp should match");
        assertEquals(original.getChecksum(), recovered.getChecksum(), "Checksum should match");
    }

    @Test
    void testNullKeySerialization() {
        // Test that "KeyLength = -1" logic works for null keys
        Message msg = new Message(null, "No key here".getBytes());
        byte[] serialized = msg.serialize();
        Message recovered = Message.deserialize(serialized);

        assertNull(recovered.getKey());
        assertArrayEquals("No key here".getBytes(), recovered.getValue());
    }
}