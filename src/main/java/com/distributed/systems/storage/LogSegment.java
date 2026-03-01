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
        this.channel.position(this.currentPosition);
    }

    private long recoverOffsetFromDataFile() throws IOException {
        long tempOffset = baseOffset;
        long tempPos = 0;
        long fileSize = channel.size();

        ByteBuffer headerBuf = ByteBuffer.allocate(12); // Timestamp (8) + KeyLen (4)
        ByteBuffer valLenBuf = ByteBuffer.allocate(4);


        while (tempPos + 12 <= fileSize) {
            // Read Timestamp + KeyLen
            headerBuf.clear();
            int read = channel.read(headerBuf, tempPos);
            if (read < 12) break; // Partial header at end of file, stop scan

            headerBuf.flip();
            headerBuf.getLong();
            int keyLen = headerBuf.getInt();

            long valLenPos = tempPos + 12 + keyLen;

            // FIX: Ensure we can read the 4-byte valLen and the val itself
            if (valLenPos + 4 > fileSize) break;

            valLenBuf.clear();
            if (channel.read(valLenBuf, valLenPos) < 4) break;
            valLenBuf.flip();
            int valLen = valLenBuf.getInt();

            if (valLenPos + 4 + valLen > fileSize) break; // Partial message

            tempPos = valLenPos + 4 + valLen;
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


        this.currentPosition += totalBytesWritten;
        this.bytesSinceLastIndexEntry += totalBytesWritten;

        // Durability; Flush to physical hardware.
        channel.force(true);

        // Return the logical offset (0, 1, 2...) instead of the byte position
        return currentOffset++;
    }

    public LogRecord read(long targetOffset) throws IOException {
        if (targetOffset >= currentOffset) {
            return null;
        }

        if (targetOffset < baseOffset) {
            throw new IOException("Offset " + targetOffset + " is before this segment's base offset " + baseOffset);
        }

        IndexEntry entry = indexManager.lookup(targetOffset, baseOffset);
        long logicalOffset = entry.logicalOffset();
        long physicalPos = entry.physicalPosition();

        ByteBuffer headerBuf = ByteBuffer.allocate(12);
        ByteBuffer valLenBuf = ByteBuffer.allocate(4);

        while (logicalOffset < targetOffset) {
            headerBuf.clear();
            // Read header at current physical position
            if (channel.read(headerBuf, physicalPos) < 12) {
                throw new IOException("Unexpected EOF at pos " + physicalPos + " while scanning for offset " + targetOffset);
            }

            headerBuf.flip();
            headerBuf.getLong(); // skip timestamp
            int keyLen = headerBuf.getInt();

            // Find value length
            valLenBuf.clear();
            if (channel.read(valLenBuf, physicalPos + 12 + keyLen) < 4) {
                throw new IOException("Unexpected EOF at valLen pos " + (physicalPos + 12 + keyLen));
            }
            valLenBuf.flip();
            int valLen = valLenBuf.getInt();

            // Advance to next record
            physicalPos += (12 + keyLen + 4 + valLen);
            logicalOffset++;
        }

        return readRecordAt(physicalPos, targetOffset);
    }

    private LogRecord readRecordAt(long position, long offset) throws IOException {
        ByteBuffer header = ByteBuffer.allocate(12);
        if (channel.read(header, position) < 12) throw new IOException("Read failed at header");
        header.flip();

        long timestamp = header.getLong();
        int keyLen = header.getInt();

        ByteBuffer keyBuf = ByteBuffer.allocate(keyLen);
        if (channel.read(keyBuf, position + 12) < keyLen) throw new IOException("Read failed at key");

        ByteBuffer valLenBuf = ByteBuffer.allocate(4);
        if (channel.read(valLenBuf, position + 12 + keyLen) < 4) throw new IOException("Read failed at valLen");
        valLenBuf.flip();
        int valLen = valLenBuf.getInt();

        ByteBuffer valBuf = ByteBuffer.allocate(valLen);
        if (channel.read(valBuf, position + 12 + keyLen + 4) < valLen) throw new IOException("Read failed at value");

        return new LogRecord(offset, timestamp, keyBuf.array(), valBuf.array());
    }

    /**
     * Scans the segment to find the physical byte position where 'targetOffset' begins.
     */
    private long findByteOffsetFor(long targetOffset) throws IOException {
        // If the target is at or before the very beginning of this segment, byte position is 0
        if (targetOffset <= baseOffset) return 0;

        long tempPos = 0;
        long tempOffset = baseOffset;
        long fileSize = channel.size();

        ByteBuffer headerBuf = ByteBuffer.allocate(12); // TS (8) + KeyLen (4)
        ByteBuffer valLenBuf = ByteBuffer.allocate(4);

        while (tempPos + 12 <= fileSize) {
            if (tempOffset == targetOffset) {
                return tempPos;
            }

            headerBuf.clear();
            channel.read(headerBuf, tempPos);
            headerBuf.flip();
            headerBuf.getLong(); // skip timestamp
            int keyLen = headerBuf.getInt();

            long valLenPos = tempPos + 12 + keyLen;
            if (valLenPos + 4 > fileSize) break;

            valLenBuf.clear();
            channel.read(valLenBuf, valLenPos);
            valLenBuf.flip();
            int valLen = valLenBuf.getInt();

            tempPos = valLenPos + 4 + valLen;
            tempOffset++;
        }
        return tempPos;
    }

    public synchronized void truncate(long targetOffset) throws IOException {
        // find where to cut physically
        long physicalPosition = findByteOffsetFor(targetOffset);
        
        channel.truncate(physicalPosition);
        channel.force(true);

        // clear the Index
        indexManager.truncateTo(targetOffset);

        // critical: if we don't update these, the next append will write to the old EOF
        this.currentPosition = physicalPosition;
        this.currentOffset = targetOffset;
        this.bytesSinceLastIndexEntry = 0; // Reset index counter

        channel.position(this.currentPosition);

        Logger.logStorage("Segment " + baseOffset + " physically truncated to " + physicalPosition + " bytes. Next offset: " + currentOffset);
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
