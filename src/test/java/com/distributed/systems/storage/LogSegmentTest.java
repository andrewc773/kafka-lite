package com.distributed.systems.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class LogSegmentTest {

    @TempDir
    Path tempDir; // JUnit handles creating and deleting this folder for you

    @Test
    public void testAppendAndReadIntegrity() throws IOException {
        Path logPath = tempDir.resolve("test.data");
        LogSegment segment = new LogSegment(logPath, 0);

        byte[] message1 = "Apples".getBytes();
        byte[] message2 = "Bananas".getBytes();

        long physicalPos1 = segment.getFileSize();
        long offset1 = segment.append(message1);

        long physicalPos2 = segment.getFileSize();
        long offset2 = segment.append(message2);

        assertArrayEquals(message1, segment.readAt(physicalPos1));
        assertArrayEquals(message2, segment.readAt(physicalPos2));

        assertEquals(0, offset1);
        assertEquals(1, offset2);
    }

    @Test
    public void testLargeMessageSequence() throws IOException {
        Path logPath = tempDir.resolve("large.data");
        LogSegment segment = new LogSegment(logPath, 0);

        for (int i = 0; i < 100; i++) {
            byte[] data = ("Message-Content-Number-" + i).getBytes();

            // Track the physical start BEFORE we append
            long physicalPos = segment.getFileSize();

            long logicalOffset = segment.append(data);

            // Verify the data using the physical address
            assertArrayEquals(
                    data, segment.readAt(physicalPos), "Data mismatch at logical offset " + logicalOffset);
        }
        segment.close();
    }

    @Test
    public void testReadBetweenBookmarks() throws IOException {
        Path logPath = tempDir.resolve("test.data");
        LogSegment segment = new LogSegment(logPath, 0);

        // Write 20 messages, each 1KB.
        // This creates ~20KB of data, meaning the index will have roughly 5 entries (every 4KB).
        for (int i = 0; i < 20; i++) {
            byte[] data = ("Content-" + i).getBytes();
            segment.append(data);
        }

        // Offset 7 is likely NOT a bookmark (it's at ~7KB, between the 4KB and 8KB marks).
        // This forces the code to jump to the 4KB bookmark and walk to #7.
        byte[] result = segment.read(7);
        assertEquals("Content-7", new String(result), "Should find message between bookmarks");

        segment.close();
    }

    @Test
    public void testReadNonExistentOffsetThrowsException() throws IOException {
        Path logPath = tempDir.resolve("bounds.data");
        LogSegment segment = new LogSegment(logPath, 0);

        segment.append("Message-0".getBytes());
        segment.append("Message-1".getBytes());

        // Message #10 doesn't exist.
        assertThrows(IOException.class, () -> {
            segment.read(10);
        }, "Reading a non-existent offset should throw IOException");

        segment.close();
    }

    @Test
    public void testBoundaryReads() throws IOException {
        Path logPath = tempDir.resolve("bounds.data");
        LogSegment segment = new LogSegment(logPath, 0);

        segment.append("First".getBytes());  // Offset 0
        segment.append("Middle".getBytes()); // Offset 1
        segment.append("Last".getBytes());   // Offset 2

        assertArrayEquals("First".getBytes(), segment.read(0), "Should read first message");
        assertArrayEquals("Last".getBytes(), segment.read(2), "Should read last message");

        segment.close();
    }

    @Test
    public void testDeepScanBetweenBookmarks() throws IOException {
        Path logPath = tempDir.resolve("deep_scan.data");
        LogSegment segment = new LogSegment(logPath, 0);

        // 1. Write 200 small messages (approx 20KB total)
        // This will create ~5 index entries.
        for (int i = 0; i < 200; i++) {
            segment.append(("Msg-" + i).getBytes());
        }

        // 2. Read a message that is deep in the middle of a scan
        // Message 150 is likely far from its closest bookmark.
        byte[] data = segment.read(150);
        assertEquals("Msg-150", new String(data), "Should successfully walk to message 150");

        // 3. Read the very last message
        byte[] lastData = segment.read(199);
        assertEquals("Msg-199", new String(lastData));

        segment.close();
    }
}
