package com.distributed.systems.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

public class LogTest {
    @TempDir
    Path tempDir;

    @Test
    public void testLogWrapper() throws IOException {
        Path logDir = tempDir.resolve("kafka-logs");
        Log log = new Log(logDir);

        long offset = log.append("Hello-Log-Manager".getBytes());
        assertArrayEquals("Hello-Log-Manager".getBytes(), log.read(offset));

        log.close();
    }

    @Test
    public void testLogRotation() throws IOException {
        Path logDir = tempDir.resolve("rotation-log");
        Log log = new Log(logDir);

        long expectedTotalBytes = 0;

        // Append enough data to trigger rotation multiple times
        // MAX_SEGMENT_SIZE is 2048
        for (int i = 0; i < 150; i++) {
            byte[] data = ("Message-number-" + i).getBytes();
            log.append(data);
            // 4 bytes for the length integer + the actual data length
            expectedTotalBytes += (4 + data.length);
        }

        // Count how many .data files exist
        long fileCount;
        try (Stream<Path> files = Files.list(logDir)) {
            fileCount = files.filter(p -> p.toString().endsWith(".data")).count();
        }

        assertTrue(fileCount > 1, "Should have rotated into multiple segments, found: " + fileCount);
        assertTrue(expectedTotalBytes > 3000, "expectedTotalBytes should be greater than 3000; found: " + expectedTotalBytes);

        // Verify we can still read from the very first message
        assertNotNull(log.read(0));
        log.close();
    }


    @Test
    public void testConcurrentAppendsInLog() throws InterruptedException, IOException {
        Path logDir = tempDir.resolve("concurrent-log");
        Log log = new Log(logDir);

        int threadCount = 10;
        int msgsPerThread = 50;
        byte[] payload = "thread-data".getBytes(); // 11 bytes

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                try {
                    for (int j = 0; j < msgsPerThread; j++) {
                        log.append(payload);
                    }
                } catch (IOException e) {
                    fail("Thread failed: " + e.getMessage());
                }
            });
            threads[i].start();
        }

        for (Thread t : threads) t.join();

        // Calculate total size across ALL files
        long totalSize = 0;
        try (Stream<Path> files = Files.list(logDir)) {
            totalSize = files.filter(p -> p.toString().endsWith(".data"))
                    .mapToLong(p -> {
                        try {
                            return Files.size(p);
                        } catch (IOException e) {
                            return 0;
                        }
                    }).sum();
        }

        // (4-byte length + 11-byte data) * 500 messages = 7500 bytes
        long expectedSize = (4 + payload.length) * (threadCount * msgsPerThread);

        assertEquals(expectedSize, totalSize, "Total size of all segments should match expected data size");
        log.close();
    }
}