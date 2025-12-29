package com.distributed.systems.storage;

import com.distributed.systems.config.BrokerConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Manages high-level storage operations across segments.
 */
public class Log {

    private final Path dataDir;
    private final BrokerConfig config;

    private LogSegment activeSegment;
    // Maps startingOffset -> LogSegment
    private final ConcurrentSkipListMap<Long, LogSegment> segments = new ConcurrentSkipListMap<>();
    private long nextOffset = 0;

    public Log(Path dataDir, BrokerConfig config) throws IOException {
        this.dataDir = dataDir;
        this.config = config;
        // ensure the folder exists
        if (!Files.exists(dataDir)) {
            Files.createDirectories(dataDir);
        }

        loadSegments();

        if (segments.isEmpty()) {
            createNewSegment(0);
            this.nextOffset = 0;
        } else {
            // Resume from where we left off
            // Find the segment with the highest starting offset
            Map.Entry<Long, LogSegment> lastEntry = segments.lastEntry();
            this.activeSegment = lastEntry.getValue();

            // The next offset for the WHOLE log is the last offset of the last segment + 1
            this.nextOffset = activeSegment.getLastOffset() + 1;

            System.out.println("Resuming log at offset: " + nextOffset);
        }

    }

    private void loadSegments() throws IOException {
        try (var files = Files.list(dataDir)) {
            files.filter(path -> path.toString().endsWith(".data"))
                    .forEach(path -> {
                        try {
                            // Parse offset from filename (e.g., "0000000123.data" -> 123)
                            String name = path.getFileName().toString();
                            long baseOffset = Long.parseLong(name.replace(".data", ""));
                            LogSegment segment = new LogSegment(path, baseOffset, config.getIndexIntervalBytes());

                            segments.put(baseOffset, segment);
                            System.out.println("Loaded existing segment: " + name);
                        } catch (Exception e) {
                            System.err.println("Failed to load segment: " + path);
                        }

                    });
        }
    }

    /*
     * Creates new segment w/ segment file, provided the offset base
     */
    private void createNewSegment(long baseOffset) throws IOException {
        String fileName = String.format("%010d.data", baseOffset);
        Path segmentPath = dataDir.resolve(fileName);

        LogSegment newSegment = new LogSegment(segmentPath, baseOffset, config.getIndexIntervalBytes());
        segments.put(baseOffset, newSegment);
        this.activeSegment = newSegment;
    }

    /*
     * Append data to the current active segment.
     * */
    public synchronized long append(byte[] data) throws IOException {
        // Calculate total size: Current size + 4 bytes (length prefix) + data length
        long estimatedSizeAfterAppend = activeSegment.getFileSize() + 4 + data.length;

        // Rotate if this append would push us over the limit
        if (estimatedSizeAfterAppend > config.getMaxSegmentSize()) {
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

    public long getSegmentCount() {
        return segments.size();
    }
}
