package com.distributed.systems.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class IndexManagerTest {

    @TempDir
    Path tempDir;

    @Test
    public void testFileInitialization() throws IOException {
        Path logPath = tempDir.resolve("broker.data");
        Path indexPath = tempDir.resolve("broker.index");

        // Initialize the segment
        LogSegment segment = new LogSegment(logPath);

        // Verify both files were created on disk
        assertTrue(java.nio.file.Files.exists(logPath), "Data file should exist");
        assertTrue(java.nio.file.Files.exists(indexPath), "Index file should exist");

        segment.close();
    }
}
