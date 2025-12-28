package com.distributed.systems.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

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
    public void testConcurrentAppendsInLog() throws InterruptedException, IOException {
        Path logDir = tempDir.resolve("concurrent-log");
        Log log = new Log(logDir);

        int threadCount = 10;
        int msgsPerThread = 50;

        Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                try {
                    for (int j = 0; j < msgsPerThread; j++) {
                        log.append("thread-data".getBytes());
                    }
                } catch (IOException e) {
                    fail("Thread failed: " + e.getMessage());
                }
            });
            threads[i].start();
        }

        for (Thread t : threads) t.join();

        // 4-byte length + 11-byte "thread-data" = 15 bytes per message
        long expectedSize = (4 + 11) * (threadCount * msgsPerThread);

        // You might need a way to check the size from the Log class,
        // or just check the file on disk directly.
        long actualSize = Files.size(logDir.resolve("0.data"));

        assertEquals(expectedSize, actualSize, "File size mismatch indicates a race condition!");
        log.close();
    }
}
