package com.distributed.systems.storage;

import com.distributed.systems.config.BrokerConfig;
import org.junit.jupiter.api.*;

import java.io.IOException;
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
        //  Initial usage should be 0 (or very small for active segment files)
        long initial = log.getTotalDiskUsage();

        String data = "Hello World"; // 11 bytes
        log.append(data.getBytes());

        //verify total usage increased
        long afterAppend = log.getTotalDiskUsage();
        assertTrue(afterAppend > initial, "Disk usage should increase after append");

        //  Manual Verification: Create a 1KB dummy file in the dir
        Path dummy = tempDir.resolve("dummy.file");
        Files.write(dummy, new byte[1024]);

        long afterDummy = log.getTotalDiskUsage();
        assertEquals(afterAppend + 1024, afterDummy, "Disk usage should include manual file addition");
    }
}