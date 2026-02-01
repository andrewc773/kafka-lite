package com.distributed.systems.client;

import com.distributed.systems.util.Logger;
import com.distributed.systems.util.Protocol;

import java.io.*;
import java.net.Socket;

public class KafkaLiteClient implements AutoCloseable {

    private final String host;
    private final int port;
    private Socket socket;

    private DataOutputStream out;
    private DataInputStream in;

    public KafkaLiteClient(String host, int port) throws IOException {
        this.host = host;
        this.port = port;
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
     * Sends a message to the broker.
     *
     * @return The offset assigned to the message.
     */
    public long produce(String topic, String key, String value) throws IOException {
        return executeWithRetry(() -> {
            out.writeUTF(Protocol.CMD_PRODUCE);
            out.writeUTF(topic);

            byte[] keyBytes = (key == null) ? new byte[0] : key.getBytes();
            out.writeInt(keyBytes.length);
            out.write(keyBytes);

            byte[] valBytes = value.getBytes();
            out.writeInt(valBytes.length);
            out.write(valBytes);
            out.flush();

            // Server returns the assigned offset as a long
            return in.readLong();
        });
    }

    /**
     * Retrieves a message from the broker by offset.
     */
    /**
     * Retrieves a message from the broker by topic and offset.
     * Protocol: [String CMD][String Topic][Long Offset]
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

                System.out.printf("[%s#%d] %d | Key: %s | Val: %s%n",
                        topic, resOffset, timestamp, new String(key), new String(val));
            } else {
                String error = in.readUTF();
                throw new IOException("Server error: " + error);
            }
            return null; // For functional interface compatibility
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
        try {
            return action.execute();
        } catch (IOException e) {
            Logger.logError("Socket error: " + e.getMessage());
            Logger.logInfo("Reconnecting...");

            // Cleanup
            try {
                if (socket != null) socket.close();
            } catch (Exception ignored) {
            }
            connect();

            return action.execute();
        }
    }


}
