package com.distributed.systems.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class IndexManager {
    private final FileChannel indexChannel;
    private static final int ENTRY_SIZE = 16; // 8 bytes for offset, 8 for position

    public IndexManager(Path indexPath) throws IOException {
        // Open the index file for reading and writing
        this.indexChannel =
                FileChannel.open(
                        indexPath,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE);
    }

    // Appends a new bookmark to the index.
    public void addEntry(long offset, long position) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(ENTRY_SIZE);
        buffer.putLong(offset); // Store message ID
        buffer.putLong(position); // Store file location
        buffer.flip(); // Prepare for writing

        indexChannel.write(buffer, indexChannel.size());
        indexChannel.force(true); // Ensure index is flushed to disk
    }

    // Returns {logicalOffset, physicalPosition}
    public IndexEntry lookup(long targetOffset) throws IOException {
        long fileSize = indexChannel.size();
        if (fileSize == 0) return new IndexEntry(0, 0);

        long low = 0;
        long high = (fileSize / 16) - 1;

        // default starting point (the first bookmark)
        long bestOffset = 0;
        long bestPosition = 0;

        ByteBuffer buffer = ByteBuffer.allocate(16);

        while (low <= high) {
            long mid = low + (high - low) / 2;
            buffer.clear();
            indexChannel.read(buffer, mid * 16);
            buffer.flip();

            long offsetAtMid = buffer.getLong();
            long positionAtMid = buffer.getLong();

            if (offsetAtMid == targetOffset) {
                return new IndexEntry(offsetAtMid, positionAtMid);
            } else if (offsetAtMid < targetOffset) {
                bestOffset = offsetAtMid;
                bestPosition = positionAtMid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }
        }
        return new IndexEntry(bestOffset, bestPosition);
    }


    public void close() throws IOException {
        indexChannel.close();
    }

    public boolean isEmpty() throws IOException {
        return indexChannel.size() == 0;
    }

    public long getLastOffset() throws IOException {
        long size = indexChannel.size();

        // Integrity Check: Every entry is exactly 16 bytes.
        // If it's not a multiple of 16, or it's empty, the index is corrupted.
        if (size < 16) {
            throw new IOException("Index file is corrupted or empty. Size: " + size);
        }

        ByteBuffer buffer = ByteBuffer.allocate(16);
        // Jump to the start of the last 16-byte record
        indexChannel.read(buffer, size - 16);
        buffer.flip();

        // The first 8 bytes of the record is the logical offset
        return buffer.getLong();
    }
}
