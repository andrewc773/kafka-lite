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

    public long lookup(long targetOffset) throws IOException {

        long fileSize = indexChannel.size();

        if (fileSize == 0) return 0;

        long low = 0;
        long high = (fileSize / 16) - 1; // Number of 16 byte entries
        long bestPhysicalPosition = 0;

        ByteBuffer buffer = ByteBuffer.allocate(16);

        while (low <= high) {
            long mid = low + (high - low) / 2;

            // Read the 16-byte entry at the mid index
            buffer.clear();
            indexChannel.read(buffer, mid * 16);
            buffer.flip();

            long offsetAtMid = buffer.getLong(); // offset ID
            long positionAtMid = buffer.getLong(); //byte address

            if (offsetAtMid == targetOffset) {
                return positionAtMid;
            } else if (offsetAtMid < targetOffset) {
                bestPhysicalPosition = positionAtMid;
                low = mid + 1;
            } else {
                high = mid - 1;
            }

        }

        // If no exact match, return the best starting point for a linear scan
        return bestPhysicalPosition;
    }


    public void close() throws IOException {
        indexChannel.close();
    }
}
