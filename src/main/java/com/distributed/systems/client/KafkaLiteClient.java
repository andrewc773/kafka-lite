package com.distributed.systems.client;

import com.distributed.systems.util.Logger;
import com.distributed.systems.util.Protocol;

import java.io.*;
import java.net.Socket;

public class KafkaLiteClient implements AutoCloseable {

    private final String host;
    private final int port;
    private Socket socket;
    private PrintWriter out;
    private BufferedReader in;

    public KafkaLiteClient(String host, int port) throws IOException {
        this.host = host;
        this.port = port;
        connect();
    }

    private void connect() throws IOException {
        Logger.logNetwork("Connecting to broker at " + host + ":" + port + "...");

        this.socket = new Socket(host, port);
        this.out = new PrintWriter(socket.getOutputStream(), true);
        this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        // Skip the "Welcome" messages sent by the server on connect
        in.readLine(); // --- Welcome ---
        in.readLine(); // Commands: ...
    }

    /**
     * Sends a message to the broker.
     *
     * @return The offset assigned to the message.
     */
    public long produce(String data) throws IOException {
        
        return executeWithRetry(() -> {
            //sending to server
            out.println(Protocol.CMD_PRODUCE + " " + data);
            String response = in.readLine();

            if (response != null && response.startsWith(Protocol.RESP_SUCCESS_PREFIX)) {
                return Long.parseLong(response.substring(Protocol.RESP_SUCCESS_PREFIX.length()).trim());
            } else {
                throw new IOException("Server error: " + response);
            }
        });

    }

    /**
     * Retrieves a message from the broker by offset.
     */
    public String consume(long offset) throws IOException {

        return executeWithRetry(() -> {
            //sending to server
            out.println(Protocol.CMD_CONSUME + " " + offset);
            String response = in.readLine();

            if (response != null && response.startsWith(Protocol.RESP_DATA_PREFIX)) {
                return response.substring(Protocol.RESP_DATA_PREFIX.length());
            } else {
                throw new IOException("Server error: " + response);
            }
        });
    }

    @Override
    public void close() throws IOException {
        if (socket != null && !socket.isClosed()) {
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
            Logger.logError("Socket error detected: " + e.getMessage());
            Logger.logInfo("Attempting automatic reconnection...");

            close();   // Cleanup old resources
            connect(); // Re-establish the pipe

            Logger.logNetwork("Reconnected successfully. Retrying command...");
            return action.execute(); // Second attempt
        }
    }


}
