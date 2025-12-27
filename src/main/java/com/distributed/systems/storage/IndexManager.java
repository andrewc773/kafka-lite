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
        this.indexChannel = FileChannel.open(indexPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
    }

    // Appends a new bookmark to the index.
    public void addEntry(long offset, long position) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(ENTRY_SIZE);
        buffer.putLong(offset);    // Store message ID
        buffer.putLong(position);  // Store file location
        buffer.flip();             // Prepare for writing

        indexChannel.write(buffer, indexChannel.size());
        indexChannel.force(true);  // Ensure index is flushed to disk
    }

    public void close() throws  IOException {
        indexChannel.close();
    }

}
