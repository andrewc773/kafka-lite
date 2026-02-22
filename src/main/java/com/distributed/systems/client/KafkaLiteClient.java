package com.distributed.systems.client;

import com.distributed.systems.util.Logger;
import com.distributed.systems.util.Protocol;

import java.io.*;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaLiteClient implements AutoCloseable {

    private final String host;
    private final int port;
    private Socket socket;
    private final String groupId;
    private DataOutputStream out;
    private DataInputStream in;
    private long currentOffset = 0;

    // Background scheduler for auto-committing
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public KafkaLiteClient(String host, int port, String groupId) throws IOException {
        this.host = host;
        this.port = port;
        this.groupId = groupId;
        connect();
    }

    private void connect() throws IOException {
        Logger.logNetwork("Connecting to broker at " + host + ":" + port + "...");

        this.socket = new Socket(host, port);
        // Using Data streams for binary record support
        this.out = new DataOutputStream(socket.getOutputStream());
        this.in = new DataInputStream(socket.getInputStream());
    }

    /**
     * Sends a message to the broker with a key.
     * Protocol: [String CMD][Int KeyLen][Bytes Key][Int ValLen][Bytes Val]
     */
    public long produce(String topic, String key, String value) throws IOException {
        return executeWithRetry(() -> {
            out.writeUTF(Protocol.CMD_PRODUCE); // "PRODUCE"
            out.writeUTF(topic); //Tell the server which topic we are writing to

            byte[] keyBytes = (key == null) ? new byte[0] : key.getBytes();
            out.writeInt(keyBytes.length);
            out.write(keyBytes);

            byte[] valBytes = value.getBytes();
            out.writeInt(valBytes.length);
            out.write(valBytes);
            out.flush();

            return in.readLong();
        });
    }

    /**
     * Retrieves a message from the broker by offset.
     * Protocol: [String CMD][Long Offset]
     */
    public void consume(String topic, long offset) throws IOException {
        executeWithRetry(() -> {
            out.writeUTF(Protocol.CMD_CONSUME);
            out.writeUTF(topic);
            out.writeLong(offset);
            out.flush();

            boolean found = in.readBoolean();
            if (found) {
                long resOffset = in.readLong();
                long timestamp = in.readLong();

                int keyLen = in.readInt();
                byte[] key = new byte[keyLen];
                in.readFully(key);

                int valLen = in.readInt();
                byte[] val = new byte[valLen];
                in.readFully(val);

                System.out.printf("[Offset: %d] [TS: %d] | Key: %s | Val: %s%n",
                        resOffset, timestamp, new String(key), new String(val));
            } else {
                String error = in.readUTF();
                throw new IOException("Server error: " + error);
            }
            return null; // For functional interface compatibility
        });
    }

    /**
     * Helper to find where this client left off.
     */
    public long fetchOffset(String topic) throws IOException {
        return executeWithRetry(() -> {
            out.writeUTF(Protocol.CMD_OFFSET_FETCH);
            out.writeUTF(this.groupId);
            out.writeUTF(topic);
            out.flush();
            return in.readLong(); // Returns -1 if group is new
        });
    }

    public void commitOffset(String topic, long offset) throws IOException {
        executeWithRetry(() -> {
            out.writeUTF(Protocol.CMD_OFFSET_COMMIT);
            out.writeUTF(this.groupId);
            out.writeUTF(topic);
            out.writeLong(offset);
            out.flush();
            return in.readBoolean();
        });
    }

    /**
     * Retrieves a health and performance report from the broker.
     */
    public String getStats() throws IOException {
        return executeWithRetry(() -> {
            out.writeUTF(Protocol.CMD_STATS);
            out.flush();
            return in.readUTF(); // Returns the report string
        });
    }

    @Override
    public void close() throws IOException {
        if (socket != null && !socket.isClosed()) {
            try {
                out.writeUTF(Protocol.CMD_QUIT);
                out.flush();
            } catch (IOException ignored) {
            }
            socket.close();
        }
    }

    @FunctionalInterface
    private interface CommandAction<T> {
        T execute() throws IOException;
    }

    /* Helper to execute network action and retry once if the connection is lost
     * */
    private <T> T executeWithRetry(CommandAction<T> action) throws IOException {
        int maxAttempts = 5;
        int attempt = 0;
        int backoffMs = 1000;

        while (true) {
            try {
                if (socket == null || socket.isClosed()) {
                    connect();
                }
                return action.execute();
            } catch (IOException e) {
                attempt++;
                Logger.logError("Socket error (Attempt " + attempt + "/" + maxAttempts + "): " + e.getMessage());

                //clean up dead socket
                closeQuietly();

                if (attempt >= maxAttempts) {
                    throw new IOException("Failed to execute command after " + maxAttempts + " attempts.", e);
                }

                Logger.logInfo("Retrying in " + backoffMs + "ms...");
                try {
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Retry interrupted", ie);
                }

                backoffMs *= 2;
            }

        }
    }

    private void closeQuietly() {
        try {
            if (socket != null) socket.close();
        } catch (IOException ignored) {
        }
        socket = null; // Forces connect() to create a new one next time
    }

    /**
     * Start the background offset-committer on the client side.
     * Every X ms, it tells the broker: "I am still at offset Y".
     */
    public void startAutoCommit(String topic, AtomicLong offsetTracker, long intervalMs) {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                long toCommit = offsetTracker.get();
                if (toCommit >= 0) {
                    commitOffset(topic, toCommit);
                    Logger.logInfo("Auto-committed offset " + toCommit + " for topic " + topic);
                }
            } catch (IOException e) {
                Logger.logError("Auto-commit failed: " + e.getMessage());
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }


}
