package com.distributed.systems.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.io.IOException;

public class LogSegmentTest {

    @TempDir
    Path tempDir; // JUnit handles creating and deleting this folder for you

    @Test
    public void testAppendAndReadIntegrity() throws IOException {
        Path logPath = tempDir.resolve("test.data");
        LogSegment segment = new LogSegment(logPath);

        // Test with two different messages
        byte[] msg1 = "Apples-first".getBytes();
        byte[] msg2 = "Bananas-second".getBytes();

        long pos1 = segment.append(msg1);
        long pos2 = segment.append(msg2);

        // Validation 1: Read back message 1
        assertArrayEquals(msg1, segment.readAt(pos1), "First message should match");

        // Validation 2: Read back message 2
        assertArrayEquals(msg2, segment.readAt(pos2), "Second message should match");

        // Validation 3: Check that positions don't overlap (Framing check)
        assertTrue(pos2 >= pos1 + 4 + msg1.length, "Second message must start after first");

        segment.close();
    }
}