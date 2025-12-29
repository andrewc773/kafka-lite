package com.distributed.systems.storage;

import com.distributed.systems.config.BrokerConfig;
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

    // Helper to create a standard config for tests
    private BrokerConfig createDefaultConfig() {
        return new BrokerConfig(2048, 600000, 4096, 30000);
    }

    @Test
    public void testJanitorRetention() throws IOException, InterruptedException {
        Path logDir = tempDir.resolve("janitor-test");
        // CONFIG: 100 byte segments, 500ms retention, 100ms cleanup interval
        BrokerConfig janitorConfig = new BrokerConfig(100, 500, 4096, 100);
        Log log = new Log(logDir, janitorConfig);

        // Create multiple segments
        log.append(new byte[60]); // Segment 0
        log.append(new byte[60]); // Rotates, Segment 1 starts
        log.append(new byte[60]); // Rotates, Segment 2 starts (Active)

        assertEquals(3, log.getSegmentCount(), "Should have 3 segments initially");

        //  Wait for retention (500ms) + buffer for cleanup interval (100ms)
        Thread.sleep(1000);

        // verify cleanup.
        // Segment 2 is the 'activeSegment', so it must NOT be deleted.
        // Segments 0 and 1 should be gone.
        assertTrue(log.getSegmentCount() < 3, "Janitor should have deleted expired segments");
        assertEquals(1, log.getSegmentCount(), "Only the active segment should remain");

        log.close();
    }

    @Test
    public void testRotationWithConfig() throws IOException {
        BrokerConfig testConfig = new BrokerConfig(100, 60000, 4096, 30000);
        Log log = new Log(tempDir, testConfig);

        log.append(new byte[60]);
        log.append(new byte[60]);

        assertEquals(2, log.getSegmentCount(), "Log should have rotated into 2 segments");
        log.close();
    }

    @Test
    public void testLogWrapper() throws IOException {
        Path logDir = tempDir.resolve("kafka-logs");
        Log log = new Log(logDir, createDefaultConfig());

        long offset = log.append("Hello-Log-Manager".getBytes());
        assertArrayEquals("Hello-Log-Manager".getBytes(), log.read(offset));

        log.close();
    }

    @Test
    public void testLogRotation() throws IOException {
        Path logDir = tempDir.resolve("rotation-log");
        Log log = new Log(logDir, createDefaultConfig());

        long expectedTotalBytes = 0;

        for (int i = 0; i < 150; i++) {
            byte[] data = ("Message-number-" + i).getBytes();
            log.append(data);
            expectedTotalBytes += (4 + data.length);
        }

        long fileCount;
        try (Stream<Path> files = Files.list(logDir)) {
            fileCount = files.filter(p -> p.toString().endsWith(".data")).count();
        }

        assertTrue(fileCount > 1, "Should have rotated into multiple segments");
        assertTrue(expectedTotalBytes > 3000, "Expected total bytes is greater than 3000; actual totaL: " + expectedTotalBytes);
        log.close();
    }

    @Test
    public void testConcurrentAppendsInLog() throws InterruptedException, IOException {
        Path logDir = tempDir.resolve("concurrent-log");
        Log log = new Log(logDir, createDefaultConfig());

        int threadCount = 10;
        int msgsPerThread = 50;
        byte[] payload = "thread-data".getBytes();

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

        long expectedSize = (4 + payload.length) * (threadCount * msgsPerThread);
        assertEquals(expectedSize, totalSize);
        log.close();
    }

    @Test
    public void testLogBootstrapWithMultipleSegments() throws IOException {
        Path logDir = tempDir.resolve("bootstrap-test");

        Log log1 = new Log(logDir, createDefaultConfig());
        for (int i = 0; i < 150; i++) {
            log1.append(("msg-" + i).getBytes());
        }
        log1.close();

        Log log2 = new Log(logDir, createDefaultConfig());
        assertEquals(150, log2.append("new-msg".getBytes()));
        assertArrayEquals("msg-0".getBytes(), log2.read(0));
        assertArrayEquals("new-msg".getBytes(), log2.read(150));

        log2.close();
    }
}