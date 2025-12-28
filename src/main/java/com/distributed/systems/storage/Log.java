package com.distributed.systems.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Manages high-level storage operations across segments.
 */
public class Log {

    private static final long MAX_SEGMENT_SIZE = 2048;
    private final Path dataDir;

    private LogSegment activeSegment;
    // Maps startingOffset -> LogSegment
    private final ConcurrentSkipListMap<Long, LogSegment> segments = new ConcurrentSkipListMap<>();
    private long nextOffset = 0;

    public Log(Path dataDir) throws IOException {
        this.dataDir = dataDir;
        // ensure the folder exists
        if (!Files.exists(dataDir)) {
            Files.createDirectories(dataDir);
        }
        createNewSegment(0);
    }

    /*
     * Creates new segment w/ segment file, provided the offset base
     */
    private void createNewSegment(long baseOffset) throws IOException {
        String fileName = String.format("%010d.data", baseOffset);
        Path segmentPath = dataDir.resolve(fileName);

        LogSegment newSegment = new LogSegment(segmentPath);
        segments.put(baseOffset, newSegment);
        this.activeSegment = newSegment;
    }

    /*
     * Append data to the current active segment.
     * */
    public synchronized long append(byte[] data) throws IOException {
        // Check if we need to rotate BEFORE appending
        if (activeSegment.getFileSize() >= MAX_SEGMENT_SIZE) {
            rotate();
        }

        long offset = activeSegment.append(data);
        nextOffset = offset + 1;
        return offset;
    }

    /*Rotates logs*/
    private void rotate() throws IOException {
        System.out.println("Rotating log segment at offset: " + nextOffset);
        // We can consider marking old one as read-only here
        createNewSegment(nextOffset);
    }

    /*
     * Reads data based on a logical offset
     * */
    public byte[] read(long offset) throws IOException {
        // Find the segment that contains this offset
        // floorEntry finds the greatest key less than or equal to the given offset

        Map.Entry<Long, LogSegment> entry = segments.floorEntry(offset);

        if (entry == null) {
            throw new IOException("Offset " + offset + " is before the start of the log.");
        }

        return entry.getValue().read(offset);
    }

    /*
     * Cleanly close the active segment(s) and indexes
     * */
    public void close() throws IOException {
        for (LogSegment segment : segments.values()) {
            segment.close();
        }
    }
}
