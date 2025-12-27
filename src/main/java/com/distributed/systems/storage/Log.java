package com.distributed.systems.storage;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Manages high-level storage operations across segments.
 */
public class Log {
    private final LogSegment activeSegment;
    private final Path dataDir;

    public Log(Path dataDir) throws IOException {
        this.dataDir = dataDir;

        //Ensure directory exists
        if (!Files.exists(dataDir)) {
            Files.createDirectories(dataDir);
        }

        // For now, initialize a single active segment
        Path firstSegmentPath = dataDir.resolve("0.data");

        this.activeSegment = new LogSegment(firstSegmentPath);
    }

    /*
     * Append data to the current active segment.
     * */
    public long append(byte[] data) throws IOException {
        return activeSegment.append(data);
    }

    /*
     * Reads data based on a logical offset
     * */
    public byte[] read(long offest) throws IOException {
        return activeSegment.read(offest);
    }

    /*
     * Cleanly close the active segment and its index
     * */
    public void close() throws IOException {
        activeSegment.close();
    }
}
