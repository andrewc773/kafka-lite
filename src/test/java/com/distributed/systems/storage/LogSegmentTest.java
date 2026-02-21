package com.distributed.systems.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class LogSegmentTest {

    @TempDir
    Path tempDir;

    private static final long MAX_SEGMENT_SIZE = 2048;
    private final byte[] defaultKey = "test-key".getBytes();

    @Test
    public void testAppendAndReadIntegrity() throws IOException {
        Path logPath = tempDir.resolve("test.data");
        LogSegment segment = new LogSegment(logPath, 0, MAX_SEGMENT_SIZE);

        byte[] message1 = "Apples".getBytes();
        byte[] message2 = "Bananas".getBytes();

        long offset1 = segment.append(defaultKey, message1);
        long offset2 = segment.append(defaultKey, message2);

        // Updated to use logical read and LogRecord return type
        assertArrayEquals(message1, segment.read(offset1).value());
        assertArrayEquals(message2, segment.read(offset2).value());

        assertEquals(0, offset1);
        assertEquals(1, offset2);
    }

    @Test
    public void testLargeMessageSequence() throws IOException {
        Path logPath = tempDir.resolve("large.data");
        LogSegment segment = new LogSegment(logPath, 0, MAX_SEGMENT_SIZE);

        for (int i = 0; i < 100; i++) {
            byte[] data = ("Message-Content-Number-" + i).getBytes();
            long logicalOffset = segment.append(defaultKey, data);

            // Verify using logical offset
            assertArrayEquals(
                    data, segment.read(logicalOffset).value(), "Data mismatch at logical offset " + logicalOffset);
        }
        segment.close();
    }

    @Test
    public void testReadBetweenBookmarks() throws IOException {
        Path logPath = tempDir.resolve("test.data");
        // Using a small index interval to force bookmarks
        LogSegment segment = new LogSegment(logPath, 0, 100);

        for (int i = 0; i < 20; i++) {
            byte[] data = ("Content-" + i).getBytes();
            segment.append(defaultKey, data);
        }

        LogRecord result = segment.read(7);
        assertEquals("Content-7", new String(result.value()), "Should find message between bookmarks");

        segment.close();
    }

    @Test
    public void testReadNonExistentOffsetThrowsException() throws IOException {
        Path logPath = tempDir.resolve("bounds.data");
        LogSegment segment = new LogSegment(logPath, 0, MAX_SEGMENT_SIZE);

        segment.append(defaultKey, "Message-0".getBytes());
        segment.append(defaultKey, "Message-1".getBytes());

        assertNull(segment.read(10));
        segment.close();
    }

    @Test
    public void testBoundaryReads() throws IOException {
        Path logPath = tempDir.resolve("bounds.data");
        LogSegment segment = new LogSegment(logPath, 0, MAX_SEGMENT_SIZE);

        segment.append(defaultKey, "First".getBytes());  // Offset 0
        segment.append(defaultKey, "Middle".getBytes()); // Offset 1
        segment.append(defaultKey, "Last".getBytes());   // Offset 2

        assertArrayEquals("First".getBytes(), segment.read(0).value(), "Should read first message");
        assertArrayEquals("Last".getBytes(), segment.read(2).value(), "Should read last message");

        segment.close();
    }

    @Test
    public void testDeepScanBetweenBookmarks() throws IOException {
        Path logPath = tempDir.resolve("deep_scan.data");
        LogSegment segment = new LogSegment(logPath, 0, 128); // Force frequent indexing

        for (int i = 0; i < 200; i++) {
            segment.append(defaultKey, ("Msg-" + i).getBytes());
        }

        LogRecord data = segment.read(150);
        assertEquals("Msg-150", new String(data.value()), "Should successfully walk to message 150");

        LogRecord lastData = segment.read(199);
        assertEquals("Msg-199", new String(lastData.value()));

        segment.close();
    }

    @Test
    public void testSegmentRecovery() throws IOException {
        Path segmentPath = tempDir.resolve("0000000000.data");

        LogSegment seg1 = new LogSegment(segmentPath, 0, MAX_SEGMENT_SIZE);
        seg1.append(defaultKey, "Message 1".getBytes()); // Offset 0
        seg1.append(defaultKey, "Message 2".getBytes()); // Offset 1
        seg1.close();

        LogSegment seg2 = new LogSegment(segmentPath, 0, MAX_SEGMENT_SIZE);

        assertEquals(2, seg2.append(defaultKey, "Message 3".getBytes()));

        assertArrayEquals("Message 1".getBytes(), seg2.read(0).value());
        assertArrayEquals("Message 3".getBytes(), seg2.read(2).value());
        seg2.close();
    }

    @Test
    void testSegmentRecoveryWithoutIndex() throws IOException {
        Path dataPath = tempDir.resolve("0000000000.data");
        long baseOffset = 0;
        long interval = 4096;

        LogSegment segment = new LogSegment(dataPath, baseOffset, interval);
        segment.append("user".getBytes(), "Andrew".getBytes());
        segment.append("role".getBytes(), "Engineer".getBytes());
        segment.close();

        // Manually delete the index file to force linear scan recovery
        Path indexPath = tempDir.resolve("0000000000.index");
        Files.deleteIfExists(indexPath);

        //re-open and verify recovery
        LogSegment recoveredSegment = new LogSegment(dataPath, baseOffset, interval);

        // This should be 2 because we appended twice (0 and 1)
        assertEquals(2, recoveredSegment.getLastOffset() + 1,
                "Segment should have recovered nextOffset=2 via linear scan.");

        LogRecord record = recoveredSegment.read(1);
        assertEquals("Engineer", new String(record.value()));
    }

    @Test
    public void testLogSegmentLastOffset() throws IOException {
        Path segmentPath = tempDir.resolve("0000000100.data");
        long baseOffset = 100;

        LogSegment segment = new LogSegment(segmentPath, baseOffset, MAX_SEGMENT_SIZE);
        assertEquals(baseOffset - 1, segment.getLastOffset(),
                "New segment should report baseOffset - 1 as last offset");

        segment.append(defaultKey, "first".getBytes());  // Offset 100
        segment.append(defaultKey, "second".getBytes()); // Offset 101
        segment.append(defaultKey, "third".getBytes());  // Offset 102

        assertEquals(102, segment.getLastOffset());
        segment.close();

        LogSegment recoveredSegment = new LogSegment(segmentPath, baseOffset, MAX_SEGMENT_SIZE);
        assertEquals(102, recoveredSegment.getLastOffset());

        long next = recoveredSegment.append(defaultKey, "fourth".getBytes());
        assertEquals(103, next);
        assertEquals(103, recoveredSegment.getLastOffset());

        recoveredSegment.close();
    }

    @Test
    public void testGetDataPath() throws IOException {
        Path segmentPath = tempDir.resolve("0000000000.data");
        LogSegment segment = new LogSegment(segmentPath, 0, 4096);

        assertEquals(segmentPath, segment.getDataPath());

        segment.close();
    }
}