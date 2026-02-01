package com.distributed.systems.storage;

import com.distributed.systems.util.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class LogSegment {
    private final FileChannel channel;
    private long currentPosition;
    private final long baseOffset; // Identity of the segment

    private final IndexManager indexManager;
    private int bytesSinceLastIndexEntry = 0;
    private final long indexIntervalBytes; // 4KB Sparse Interval (normal page size)
    private long currentOffset; // Tracks the logical message ID
    private Path dataPath;

    public LogSegment(Path dataPath, long baseOffset, long indexIntervalBytes) throws IOException {

        this.baseOffset = baseOffset;
        this.dataPath = dataPath;
        this.indexIntervalBytes = indexIntervalBytes;
        // Using FileChannel for high-performance I/O operations.

        Logger.logBootstrap("Opening segment: " + dataPath.getFileName() + " (Base Offset: " + baseOffset + ")");

        this.channel =
                FileChannel.open(
                        dataPath, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);

        // Initialize the index file
        String indexFileName = dataPath.getFileName().toString().replace(".data", ".index");
        Path indexPath = dataPath.getParent().resolve(indexFileName);

        this.indexManager = new IndexManager(indexPath);

        if (!indexManager.isEmpty()) {
            this.currentOffset = indexManager.getLastOffset() + 1;
            Logger.logBootstrap("Segment " + baseOffset + " recovered offset from Index: " + currentOffset);
        } else if (channel.size() > 0) {
            // If we are resuming, the next offset to write is the one after the last one on disk
            this.currentOffset = recoverOffsetFromDataFile();
            Logger.logBootstrap("Segment " + baseOffset + " recovered offset via Linear Scan: " + currentOffset);
        } else {
            this.currentOffset = baseOffset;
        }

        // Ensure we append to the end if the file exists.
        this.currentPosition = channel.size();
    }

    private long recoverOffsetFromDataFile() throws IOException {
        long tempOffset = baseOffset;
        long tempPos = 0;
        long fileSize = channel.size();

        while (tempPos < fileSize) {
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            if (channel.read(lenBuf, tempPos) < 4) break;
            lenBuf.flip();
            int len = lenBuf.getInt();

            tempPos += (4 + len);
            tempOffset++;
        }
        return tempOffset;
    }


    /**
     * Appends a message to the log using a 4-byte length prefix. This framing allows the reader to
     * know exactly how much to read.
     */
    public long append(byte[] key, byte[] value) throws IOException {

        long timestamp = System.currentTimeMillis();

        if (key == null) key = new byte[0];
        if (value == null) value = new byte[0]; //should throw error

        // Size = Timestamp(8) + KeyLen(4) + Key(N) + ValLen(4) + Val(M)
        int totalSize = 8 + 4 + key.length + 4 + value.length;


        // Check if we need to add sparse index entry before writing
        if (bytesSinceLastIndexEntry >= indexIntervalBytes) {
            indexManager.addEntry(currentOffset, currentPosition);
            bytesSinceLastIndexEntry = 0;
        }

        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putLong(timestamp);
        buffer.putInt(key.length);
        buffer.put(key);
        buffer.putInt(value.length);
        buffer.put(value);
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

    public LogRecord read(long targetOffset) throws IOException {
        Logger.logStorage("Reading offset " + targetOffset + " from segment " + baseOffset);

        // jump-point from the index
        IndexEntry entry = indexManager.lookup(targetOffset);
        long currentOffset = entry.logicalOffset();
        long physicalPosition = entry.physicalPosition();

        // Linear Scan to find exact offset
        ByteBuffer headerBuf = ByteBuffer.allocate(12);
        ByteBuffer valLenBuf = ByteBuffer.allocate(4);

        while (currentOffset < targetOffset) {
            headerBuf.clear();
            int bytesRead = channel.read(headerBuf, physicalPosition);
            if (bytesRead < 12) throw new IOException("Corrupted log or EOF");

            headerBuf.flip();
            headerBuf.getLong(); // skip ts
            int keyLen = headerBuf.getInt();

            valLenBuf.clear();
            channel.read(valLenBuf, physicalPosition + 12 + keyLen);
            valLenBuf.flip();
            int valLen = valLenBuf.getInt();

            physicalPosition += (12 + keyLen + 4 + valLen);
            currentOffset++;

            if (physicalPosition > channel.size()) {
                throw new IOException("Offset " + targetOffset + " not found (Truncated).");
            }
        }

        return readRecordAt(physicalPosition, targetOffset);
    }

    private LogRecord readRecordAt(long position, long offset) throws IOException {
        // Read Timestamp(8) + KeyLen(4)
        ByteBuffer header = ByteBuffer.allocate(12);
        channel.read(header, position);
        header.flip();

        long timestamp = header.getLong();
        int keyLen = header.getInt();

        // Read key
        ByteBuffer keyBuf = ByteBuffer.allocate(keyLen);
        channel.read(keyBuf, position + 12);

        // Read val len
        ByteBuffer valLenBuf = ByteBuffer.allocate(4);
        channel.read(valLenBuf, position + 12 + keyLen);
        valLenBuf.flip();
        int valLen = valLenBuf.getInt();

        // Read val
        ByteBuffer valBuf = ByteBuffer.allocate(valLen);
        channel.read(valBuf, position + 12 + keyLen + 4);

        return new LogRecord(offset, timestamp, keyBuf.array(), valBuf.array());
    }

    public void close() throws IOException {
        Logger.logStorage("Closing file channel: " + dataPath.getFileName());
        channel.close();
        indexManager.close();
    }

    public long getFileSize() throws IOException {
        return channel.size();
    }

    public long getLastOffset() {
        // If the index is empty, the next offset is the base offset of the file
        // Otherwise, it's the last entry in our index + 1
        return currentOffset - 1;
    }

    public Path getDataPath() {
        return this.dataPath;
    }
}
