package com.distributed.systems.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class LogSegment {
    private final FileChannel channel;
    private long currentPosition;

    public LogSegment(Path file) throws IOException {
        // Using FileChannel for high-performance I/O operations.
        this.channel = FileChannel.open(file,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);

        // Ensure we append to the end if the file exists.
        this.currentPosition = channel.size();
    }

    /**
     * Appends a message to the log using a 4-byte length prefix.
     * This framing allows the reader to know exactly how much to read.
     */
    public synchronized long append(byte[] data) throws IOException {
        // [Length (4 bytes)] + [Payload (N bytes)]
        ByteBuffer buffer = ByteBuffer.allocate(4 + data.length);
        buffer.putInt(data.length);
        buffer.put(data);
        buffer.flip();

        long writeOffset = currentPosition;

        while (buffer.hasRemaining()) {
            currentPosition += channel.write(buffer, currentPosition);
        }

        // Durability; Flush to physical hardware.
        channel.force(true);

        return writeOffset;
    }

    public void close() throws IOException {
        channel.close();
    }
}