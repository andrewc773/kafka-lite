package com.distributed.systems.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class LogSegment {
    private final FileChannel channel;
    private long currentPosition;

    private final IndexManager indexManager;
    private int bytesSinceLastIndexEntry = 0;
    private static final int INDEX_INTERVAL_BYTES = 4096; // 4KB Sparse Interval (normal page size)
    private long currentOffset = 0; // Tracks the logical message ID

    public LogSegment(Path dataPath) throws IOException {
        // Using FileChannel for high-performance I/O operations.
        this.channel =
                FileChannel.open(
                        dataPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);

        // Initialize the index file
        String indexFileName = dataPath.getFileName().toString().replace(".data", ".index");
        Path indexPath = dataPath.getParent().resolve(indexFileName);

        this.indexManager = new IndexManager(indexPath);

        // Ensure we append to the end if the file exists.
        this.currentPosition = channel.size();
    }

    /**
     * Appends a message to the log using a 4-byte length prefix. This framing allows the reader to
     * know exactly how much to read.
     */
    public long append(byte[] data) throws IOException {

        // Check if we need to add sparse index entry before writing
        if (bytesSinceLastIndexEntry >= INDEX_INTERVAL_BYTES) {
            indexManager.addEntry(currentOffset, currentPosition);
            bytesSinceLastIndexEntry = 0;
        }

        // [Length (4 bytes)] + [Payload (N bytes)]
        ByteBuffer buffer = ByteBuffer.allocate(4 + data.length);
        buffer.putInt(data.length);
        buffer.put(data);
        buffer.flip();

        // Write to channel using current position in retry manner
        int totalBytesWritten = 0;
        while (buffer.hasRemaining()) {
            totalBytesWritten += channel.write(buffer, currentPosition + totalBytesWritten);
        }

        currentPosition += totalBytesWritten;
        bytesSinceLastIndexEntry += totalBytesWritten;

        // Durability; Flush to physical hardware.
        channel.force(true);

        // Return the logical offset (0, 1, 2...) instead of the byte position
        return currentOffset++;
    }

    public byte[] readAt(long position) throws IOException {
        // Read the 4-bye length prefix
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        int bytesRead = channel.read(lengthBuffer, position);

        if (bytesRead < 4) {
            throw new IOException("Could not read message length at position: " + position);
        }

        // Flip the buffer so we can read the integer out of it
        lengthBuffer.flip();
        int length = lengthBuffer.getInt();

        // 2. Read the actual content (the message payload)
        ByteBuffer dataBuffer = ByteBuffer.allocate(length);
        int dataBytesRead = channel.read(dataBuffer, position + 4); // position + 4 skips the label

        if (dataBytesRead != length) {
            throw new IOException("Incomplete message read. Expected " + length + " bytes.");
        }

        return dataBuffer.array();
    }

    public byte[] read(long targetOffset) throws IOException {
        // jump point from the index
        IndexEntry entry = indexManager.lookup(targetOffset);
        long currentOffset = entry.logicalOffset();
        long physicalPosition = entry.physicalPosition();

        // Linear Scan: Walk from the bookmark to the target
        while (currentOffset < targetOffset) {
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            int bytesRead = channel.read(lenBuf, physicalPosition);

            // if we hit the end of the file unexpectedly; the log is corrupted/truncated
            if (bytesRead < 4) {
                throw new IOException("Unexpected EOF: Could not read message length at position "
                        + physicalPosition + ". Target offset " + targetOffset + " may not exist.");
            }

            lenBuf.flip();
            int msgLen = lenBuf.getInt();

            // Move to the next message
            physicalPosition += (4 + msgLen);
            currentOffset++;

            // Check if we've passed the end of the file while walking
            if (physicalPosition > channel.size()) {
                throw new IOException("Log truncation detected: Offset " + targetOffset
                        + " points beyond the current file end.");
            }
        }

        return readAt(physicalPosition);
    }

    public void close() throws IOException {
        channel.close();
    }

    public long getFileSize() throws IOException {
        return channel.size();
    }
}
