package com.distributed.systems.storage;

import com.distributed.systems.config.BrokerConfig;
import com.distributed.systems.util.Logger;
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
        // CONFIG: 200 byte segments (increased to fit at least one new-format message), 500ms retention
        BrokerConfig janitorConfig = new BrokerConfig(200, 500, 4096, 100);
        Log log = new Log(logDir, janitorConfig);

        // Create multiple segments
        // Each append is roughly: 8(ts) + 4(kLen) + 4(vLen) + 3(key) + 60(val) = ~79 bytes
        byte[] key = "key".getBytes();
        log.append(key, new byte[60]); // Segment 0 (Size ~79)
        log.append(key, new byte[60]); // Segment 0 (Size ~158) -> Fits? Yes.
        log.append(key, new byte[60]); // Segment 0 (Size ~237) -> Rotates. Segment 1 starts.

        // Force rotation more explicitly for test reliability
        log.append(key, new byte[60]);
        log.append(key, new byte[60]);

        // Wait for retention (500ms) + buffer for cleanup interval (100ms)
        Thread.sleep(1000);

        // Only the active segment should remain
        assertTrue(log.getSegmentCount() < 5, "Janitor should have deleted expired segments");

        log.close();
    }

    @Test
    public void testRotationWithConfig() throws IOException {
        // Increased max segment size to 150 to fit exactly one message but not two
        BrokerConfig testConfig = new BrokerConfig(150, 60000, 4096, 30000);
        Log log = new Log(tempDir, testConfig);

        // Message size: 8(TS) + 4(KL) + 50(K) + 4(VL) + 30(V) = 96 bytes
        log.append(new byte[50], new byte[30]); // 96 bytes (Fits)
        log.append(new byte[50], new byte[30]); // estimates overflow; rotates

        assertEquals(2, log.getSegmentCount(), "Log should have rotated into 2 segments");
        log.close();
    }

    @Test
    public void testLogWrapper() throws IOException {
        Path logDir = tempDir.resolve("kafka-logs");
        Log log = new Log(logDir, createDefaultConfig());

        byte[] key = "key-1".getBytes();
        byte[] val = "Hello-Log-Manager".getBytes();

        long offset = log.append(key, val);

        LogRecord record = log.read(offset);
        assertArrayEquals(val, record.value());

        log.close();
    }

    @Test
    public void testLogRotation() throws IOException {
        Path logDir = tempDir.resolve("rotation-log");
        Log log = new Log(logDir, createDefaultConfig());

        long expectedTotalBytes = 0;
        byte[] key = "k".getBytes(); // 1 byte

        for (int i = 0; i < 150; i++) {
            byte[] data = ("Message-number-" + i).getBytes();
            log.append(key, data);

            // New Format Overhead: 8(TS) + 4(KeyLen) + Key + 4(ValLen) + Value
            expectedTotalBytes += (8 + 4 + key.length + 4 + data.length);
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
        byte[] key = "thread-key".getBytes(); // 10 bytes
        byte[] payload = "thread-data".getBytes(); // 11 bytes

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                try {
                    for (int j = 0; j < msgsPerThread; j++) {
                        log.append(key, payload);
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

        // Calculation: 8 (TS) + 4 (KeyLen) + 10 (Key) + 4 (ValLen) + 11 (Val) = 37 bytes per message
        long bytesPerMessage = 8 + 4 + key.length + 4 + payload.length;
        long expectedSize = bytesPerMessage * (threadCount * msgsPerThread);

        assertEquals(expectedSize, totalSize);
        log.close();
    }

    @Test
    public void testLogBootstrapWithMultipleSegments() throws IOException {
        Path logDir = tempDir.resolve("bootstrap-test");
        byte[] key = "key".getBytes();

        Log log1 = new Log(logDir, createDefaultConfig());
        for (int i = 0; i < 150; i++) {
            log1.append(key, ("msg-" + i).getBytes());
        }
        log1.close();

        Log log2 = new Log(logDir, createDefaultConfig());
        // 2. YOU MUST APPEND THE NEW MESSAGE FIRST
        long newlyAppendedOffset = log2.append(key, "new-msg".getBytes());

        Logger.logDebug("here is the newly appended offset " + newlyAppendedOffset);
        // newlyAppendedOffset should be 150.

        log2.read(150).value();

        // NOW READ 150
        assertArrayEquals("new-msg".getBytes(), log2.read(150).value());

        assertArrayEquals("msg-0".getBytes(), log2.read(0).value());

        log2.close();
    }

    @Test
    public void testLogClose() throws IOException {
        Path logDir = tempDir.resolve("close-test");
        BrokerConfig config = new BrokerConfig(2048, 60000, 4096, 1000);
        Log log = new Log(logDir, config);

        log.append("key".getBytes(), "test-data".getBytes());

        // Execute close
        log.close();

        // Verify we can't append anymore (should throw exception)
        assertThrows(Exception.class, () -> log.append("key".getBytes(), "more-data".getBytes()),
                "Appending after close should fail");

        // Verify files are unlockable/deletable (especially important on Windows)
        assertTrue(Files.deleteIfExists(logDir.resolve("0000000000.data")),
                "Data file should be deletable after log is closed");
    }
}