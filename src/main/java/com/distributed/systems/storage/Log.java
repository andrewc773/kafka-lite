package com.distributed.systems.storage;

import com.distributed.systems.config.BrokerConfig;
import com.distributed.systems.util.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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

    private final ScheduledExecutorService janitor = Executors.newSingleThreadScheduledExecutor();

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

            Logger.logBootstrap("Resuming log at offset: " + nextOffset);
        }

        long interval = config.getCleanupIntervalMs();
        janitor.scheduleAtFixedRate(() -> {
            try {
                cleanup();
            } catch (IOException e) {
                Logger.logError("Janitor failed to run cleanup: " + e.getMessage());
            }
        }, interval, interval, TimeUnit.MILLISECONDS);

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
                            Logger.logBootstrap("Loaded existing segment: " + name);
                        } catch (Exception e) {
                            Logger.logError("Failed to load segment: " + path);
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
    public synchronized long append(byte[] key, byte[] value) throws IOException {
        // Calculate total size: Current size + 4 bytes (length prefix) + data length
        long estimatedSizeAfterAppend = activeSegment.getFileSize() + 8 + 4 + key.length + 4 + value.length;

        // Rotate if this append would push us over the limit
        if (estimatedSizeAfterAppend > config.getMaxSegmentSize()) {
            rotate();
        }

        long offset = activeSegment.append(key, value);
        nextOffset = offset + 1;
        return offset;
    }

    /*Rotates logs*/
    private void rotate() throws IOException {
        Logger.logStorage("Rotating log segment at offset: " + nextOffset);
        // We can consider marking old one as read-only here
        createNewSegment(nextOffset);
    }

    /*
     * Reads data based on a logical offset
     * */
    public LogRecord read(long offset) throws IOException {

        // Find the segment that contains this offset
        // floorEntry finds the greatest key less than or equal to the given offset

        Map.Entry<Long, LogSegment> entry = segments.floorEntry(offset);

        Logger.logDebug("last offset " + entry.getValue().getLastOffset());

        if (entry == null) {
            throw new IOException("Offset " + offset + " is before the start of the log.");
        }

        return entry.getValue().read(offset);
    }

    public synchronized void cleanup() throws IOException {
        long now = System.currentTimeMillis();
        long retentionMs = config.getRetentionMs();

        Logger.logJanitor("Scanning for expired segments...");

        var iterator = segments.entrySet().iterator();

        while (iterator.hasNext()) {
            var entry = iterator.next();
            long baseOffset = entry.getKey();
            LogSegment segment = entry.getValue();

            //skip active segment
            if (segment == activeSegment) {
                continue;
            }

            try {

                long lastModified = Files.getLastModifiedTime(segment.getDataPath()).toMillis();

                if (now - lastModified > retentionMs) {
                    //evicting expired segment
                    Logger.logJanitor("Evicting expired segment: " + segment.getDataPath().getFileName());

                    //release file locks
                    segment.close();

                    //delete both files for the segment. index and data files
                    Files.deleteIfExists(segment.getDataPath());

                    Path indexPath = segment.getDataPath().resolveSibling(segment.getDataPath().getFileName().toString().replace(".data", ".index"));

                    Files.deleteIfExists(indexPath);

                    iterator.remove();
                }


            } catch (IOException e) {
                Logger.logError("Cleanup failed for segment " + entry.getKey() + ": " + e.getMessage());
            }

        }


    }

    /*
     * Cleanly close the active segment(s) and indexes
     * */
    public void close() throws IOException {

        //safely shutdown manager
        janitor.shutdown();
        try {
            if (!janitor.awaitTermination(5, TimeUnit.SECONDS)) {
                janitor.shutdownNow();
            }
        } catch (InterruptedException e) {
            janitor.shutdownNow();
        }

        for (LogSegment segment : segments.values()) {
            segment.close();
        }
    }

    public long getNextOffset() {
        return this.nextOffset;
    }

    public long getSegmentCount() {
        return segments.size();
    }

    /**
     * Calculates the total size of all log segments and indexes in bytes.
     */
    public long getTotalDiskUsage() {
        try (var stream = Files.walk(this.dataDir)) {
            return stream
                    .filter(Files::isRegularFile)
                    .mapToLong(path -> {
                        try {
                            return Files.size(path);
                        } catch (IOException e) {
                            return 0L;
                        }
                    })
                    .sum();
        } catch (IOException e) {
            Logger.logError("Failed to calculate disk usage: " + e.getMessage());
            return -1L;
        }
    }

}
