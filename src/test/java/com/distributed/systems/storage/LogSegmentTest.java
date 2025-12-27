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
        LogSegment segment = new LogSegment(logPath);

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
        LogSegment segment = new LogSegment(logPath);

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
    public void testConcurrentAppends() throws InterruptedException, IOException {
        Path logPath = tempDir.resolve("concurrent.data");
        LogSegment segment = new LogSegment(logPath);
        int threadCount = 10;
        int msgsPerThread = 50;

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threads[i] =
                    new Thread(
                            () -> {
                                try {
                                    for (int j = 0; j < msgsPerThread; j++) {
                                        segment.append("thread-data".getBytes());
                                    }
                                } catch (IOException e) {
                                    fail("Thread failed: " + e.getMessage());
                                }
                            });
            threads[i].start();
        }

        for (Thread t : threads) t.join();

        // Verify file size: (4-byte prefix + 11-byte string) * 500 total messages
        long expectedSize = (4 + 11) * threadCount * msgsPerThread;
        assertEquals(
                expectedSize, segment.getFileSize(), "File size mismatch indicates a race condition!");
        segment.close();
    }

    @Test
    public void testReadBetweenBookmarks() throws IOException {
        Path logPath = tempDir.resolve("test.data");
        LogSegment segment = new LogSegment(logPath);

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
        LogSegment segment = new LogSegment(logPath);

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
        LogSegment segment = new LogSegment(logPath);

        segment.append("First".getBytes());  // Offset 0
        segment.append("Middle".getBytes()); // Offset 1
        segment.append("Last".getBytes());   // Offset 2

        assertArrayEquals("First".getBytes(), segment.read(0), "Should read first message");
        assertArrayEquals("Last".getBytes(), segment.read(2), "Should read last message");

        segment.close();
    }
}
