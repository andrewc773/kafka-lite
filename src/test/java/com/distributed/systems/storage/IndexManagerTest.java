package com.distributed.systems.storage;

import java.io.IOException;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.*;

public class IndexManagerTest {

    @TempDir
    Path tempDir;

    private static final long MAX_SEGMENT_SIZE = 2048;


    @Test
    public void testFileInitialization() throws IOException {
        Path logPath = tempDir.resolve("broker.data");
        Path indexPath = tempDir.resolve("broker.index");

        // Initialize the segment
        LogSegment segment = new LogSegment(logPath, 0, MAX_SEGMENT_SIZE);

        // Verify both files were created on disk
        assertTrue(java.nio.file.Files.exists(logPath), "Data file should exist");
        assertTrue(java.nio.file.Files.exists(indexPath), "Index file should exist");

        segment.close();
    }

    @Test
    public void testIndexLookup() throws IOException {
        Path indexPath = tempDir.resolve("test.index");
        IndexManager index = new IndexManager(indexPath);

        long baseOffset = 0;

        index.addEntry(0, 0);       // Msg 0 is at Byte 0
        index.addEntry(100, 5000);  // Msg 100 is at Byte 5000
        index.addEntry(200, 12000); // Msg 200 is at Byte 12000

        // we call .physicalPosition() because lookup returns an IndexEntry
        IndexEntry exactMatch = index.lookup(100, baseOffset);
        assertEquals(100, exactMatch.logicalOffset());
        assertEquals(5000, exactMatch.physicalPosition());

        // Sparse match; looking for Msg 150 should return the closest bookmark BEFORE it
        IndexEntry sparseMatch = index.lookup(150, baseOffset);
        assertEquals(100, sparseMatch.logicalOffset(), "Should jump to the nearest lower bookmark");
        assertEquals(5000, sparseMatch.physicalPosition());

        index.close();
    }

    @Test
    public void testGetLastOffset() throws IOException {
        Path indexPath = tempDir.resolve("test.index");
        IndexManager index = new IndexManager(indexPath);

        // 1. Check that it throws or handles empty correctly
        assertTrue(index.isEmpty());

        // 2. Add some entries
        index.addEntry(100, 1024);
        index.addEntry(101, 2048);
        index.addEntry(105, 5000);

        // 3. Verify the last offset is 105
        assertEquals(105, index.getLastOffset());
        assertFalse(index.isEmpty());
    }

    @Test
    public void testTruncateToMidPoint() throws IOException {
        Path indexPath = tempDir.resolve("truncate_mid.index");
        IndexManager index = new IndexManager(indexPath);
        long baseOffset = 0;

        index.addEntry(10, 1000);
        index.addEntry(20, 2000);
        index.addEntry(30, 3000);
        index.addEntry(40, 4000);

        index.truncateTo(30);

        assertEquals(20, index.getLastOffset(), "Last offset should be the one right before the truncation point");

        IndexEntry entry = index.lookup(25, baseOffset);
        assertEquals(20, entry.logicalOffset());
        assertEquals(2000, entry.physicalPosition());

        index.close();
    }

    @Test
    public void testTruncateEverything() throws IOException {
        Path indexPath = tempDir.resolve("truncate_all.index");
        IndexManager index = new IndexManager(indexPath);

        index.addEntry(100, 1024);
        index.addEntry(200, 2048);

        // Truncate to an offset before the base (or the first entry)
        index.truncateTo(50);

        assertTrue(index.isEmpty(), "Index should be empty after truncating before the first entry");

        // Verify file size is physically 0
        assertEquals(0, java.nio.file.Files.size(indexPath));

        index.close();
    }

    @Test
    public void testTruncateNonExistentFutureOffset() throws IOException {
        Path indexPath = tempDir.resolve("truncate_future.index");
        IndexManager index = new IndexManager(indexPath);

        index.addEntry(10, 100);
        long originalSize = 16; // One entry

        index.truncateTo(500);

        // Should be a no-op
        assertEquals(10, index.getLastOffset());
        assertEquals(originalSize, java.nio.file.Files.size(indexPath), "File size should not change if offset is not found");

        index.close();
    }

}