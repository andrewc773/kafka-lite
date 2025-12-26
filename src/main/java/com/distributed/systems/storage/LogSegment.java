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

    public void close() throws IOException {
        channel.close();
    }

    public long getFileSize() throws IOException {
        return channel.size();
    }
}