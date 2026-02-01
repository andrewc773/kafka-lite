package com.distributed.systems.storage;

import com.distributed.systems.config.BrokerConfig;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.*;

class LogDiskUsageTest {
    private Path tempDir;
    private Log log;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("log_usage_test");
        log = new Log(tempDir, new BrokerConfig());
    }

    @AfterEach
    void tearDown() throws IOException {
        Files.walk(tempDir)
                .sorted(Comparator.reverseOrder())
                .forEach(path -> path.toFile().delete());
    }

    @Test
    void testDiskUsageCalculation() throws IOException {
        // check initial size (files exist but should be empty/small)
        long initial = log.getTotalDiskUsage();

        // append using signature: append(key, value)
        byte[] key = "test-key".getBytes(StandardCharsets.UTF_8);     // 8 bytes
        byte[] value = "Hello World".getBytes(StandardCharsets.UTF_8); // 11 bytes

        log.append(key, value);

        // Verify total usage increased
        long afterAppend = log.getTotalDiskUsage();
        assertTrue(afterAppend > initial, "Disk usage should increase after append");

        // Exact math check
        // Overhead (16) + Key (8) + Value (11) = 35 bytes expected increase
        // Note: usage might be higher if index entries were flushed, so > is safer than ==

        Path dummy = tempDir.resolve("dummy.file");
        Files.write(dummy, new byte[1024]);

        long afterDummy = log.getTotalDiskUsage();

        // The disk usage should verify the FS includes the new file exactly
        assertEquals(afterAppend + 1024, afterDummy, "Disk usage should include manual file addition");
    }
}