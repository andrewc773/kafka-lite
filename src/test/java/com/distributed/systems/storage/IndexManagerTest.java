package com.distributed.systems.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

    @Test
    public void testIndexLookup() throws IOException {
        Path indexPath = tempDir.resolve("test.index");
        IndexManager index = new IndexManager(indexPath);

        index.addEntry(0, 0);       // Msg 0 is at Byte 0
        index.addEntry(100, 5000);  // Msg 100 is at Byte 5000
        index.addEntry(200, 12000); // Msg 200 is at Byte 12000

        // we call .physicalPosition() because lookup returns an IndexEntry
        IndexEntry exactMatch = index.lookup(100);
        assertEquals(100, exactMatch.logicalOffset());
        assertEquals(5000, exactMatch.physicalPosition());

        // Sparse match; looking for Msg 150 should return the closest bookmark BEFORE it
        IndexEntry sparseMatch = index.lookup(150);
        assertEquals(100, sparseMatch.logicalOffset(), "Should jump to the nearest lower bookmark");
        assertEquals(5000, sparseMatch.physicalPosition());

        index.close();
    }
}
