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

        assertArrayEquals(msg1, segment.readAt(pos1), "First message should match");

        assertArrayEquals(msg2, segment.readAt(pos2), "Second message should match");

        // Check that positions don't overlap
        assertTrue(pos2 >= pos1 + 4 + msg1.length, "Second message must start after first");

        segment.close();
    }

    @Test
    public void testLargeMessageSequence() throws IOException {
        Path logPath = tempDir.resolve("large.data");
        LogSegment segment = new LogSegment(logPath);

        for (int i = 0; i < 100; i++) {
            byte[] data = ("Message-Content-Number-" + i).getBytes();
            long pos = segment.append(data);
            assertArrayEquals(data, segment.readAt(pos));
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
            threads[i] = new Thread(() -> {
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
        assertEquals(expectedSize, segment.getFileSize(), "File size mismatch indicates a race condition!");
        segment.close();
    }
}