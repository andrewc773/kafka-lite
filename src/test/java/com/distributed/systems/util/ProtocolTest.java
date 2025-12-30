package com.distributed.systems.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ProtocolTest {

    @Test
    void testFormatSuccess() {
        long offset = 123L;
        String expected = "SUCCESS: Message stored at OFFSET 123";
        assertEquals(expected, Protocol.formatSuccess(offset),
                "Protocol should format the success message with the correct offset.");
    }

    @Test
    void testCommandPrefixes() {
        // Ensuring the space is handled correctly in logic elsewhere
        assertEquals("PRODUCE", Protocol.CMD_PRODUCE);
        assertEquals("CONSUME", Protocol.CMD_CONSUME);
        assertEquals("DATA: ", Protocol.RESP_DATA_PREFIX);
    }
}