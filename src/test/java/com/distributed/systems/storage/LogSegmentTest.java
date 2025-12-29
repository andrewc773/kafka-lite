package com.distributed.systems.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class LogSegmentTest {

    @TempDir
    Path tempDir; // JUnit handles creating and deleting this folder for you

    private static final long MAX_SEGMENT_SIZE = 2048;

    @Test
    public void testAppendAndReadIntegrity() throws IOException {
        Path logPath = tempDir.resolve("test.data");
        LogSegment segment = new LogSegment(logPath, 0, MAX_SEGMENT_SIZE);

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
        LogSegment segment = new LogSegment(logPath, 0, MAX_SEGMENT_SIZE);

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
        LogSegment segment = new LogSegment(logPath, 0, MAX_SEGMENT_SIZE);

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
        LogSegment segment = new LogSegment(logPath, 0, MAX_SEGMENT_SIZE);

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
        LogSegment segment = new LogSegment(logPath, 0, MAX_SEGMENT_SIZE);

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
        LogSegment segment = new LogSegment(logPath, 0, MAX_SEGMENT_SIZE);

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

    @Test
    public void testSegmentRecovery() throws IOException {
        Path segmentPath = tempDir.resolve("0000000000.data");

        // 1. Create a segment, write 2 messages, and close it
        LogSegment seg1 = new LogSegment(segmentPath, 0, MAX_SEGMENT_SIZE);
        seg1.append("Message 1".getBytes()); // Offset 0
        seg1.append("Message 2".getBytes()); // Offset 1
        seg1.close();

        // 2. Create a NEW instance pointing to the same file
        LogSegment seg2 = new LogSegment(segmentPath, 0, MAX_SEGMENT_SIZE);

        // 3. Verify it resumed at the correct offset
        // append() should return 2
        assertEquals(2, seg2.append("Message 3".getBytes()));

        // 4. Verify we can read the old data from the new instance
        assertArrayEquals("Message 1".getBytes(), seg2.read(0));
        seg2.close();
    }

    @Test
    public void testLogSegmentLastOffset() throws IOException {
        Path segmentPath = tempDir.resolve("0000000100.data");
        long baseOffset = 100;

        // Test New Segment
        LogSegment segment = new LogSegment(segmentPath, baseOffset, MAX_SEGMENT_SIZE);
        // Before any appends, the last offset is baseOffset - 1 (99)
        assertEquals(baseOffset - 1, segment.getLastOffset(),
                "New segment should report baseOffset - 1 as last offset");

        // Test After Appends
        segment.append("first".getBytes());  // Offset 100
        segment.append("second".getBytes()); // Offset 101
        segment.append("third".getBytes());  // Offset 102

        assertEquals(102, segment.getLastOffset(),
                "Last offset should match the last appended message ID");

        segment.close();

        // test Recovery Last Offset
        // Re-open the same file to simulate a restart
        LogSegment recoveredSegment = new LogSegment(segmentPath, baseOffset, MAX_SEGMENT_SIZE);
        assertEquals(102, recoveredSegment.getLastOffset(),
                "Recovered segment should correctly identify the last offset from disk");

        //  Test Append after Recovery
        long next = recoveredSegment.append("fourth".getBytes());
        assertEquals(103, next);
        assertEquals(103, recoveredSegment.getLastOffset());

        recoveredSegment.close();
    }
}
