package com.distributed.systems.client;

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
        //sending to server
        out.println(Protocol.CMD_PRODUCE + " " + data);
        String response = in.readLine();

        if (response != null && response.startsWith(Protocol.RESP_SUCCESS_PREFIX)) {
            return Long.parseLong(response.substring(Protocol.RESP_SUCCESS_PREFIX.length()).trim());
        } else {
            throw new IOException("Server error: " + response);
        }
    }

    /**
     * Retrieves a message from the broker by offset.
     */
    public String consume(long offset) throws IOException {
        //sending to server
        out.println(Protocol.CMD_CONSUME + " " + offset);
        String response = in.readLine();

        if (response != null && response.startsWith(Protocol.RESP_DATA_PREFIX)) {
            return response.substring(Protocol.RESP_DATA_PREFIX.length());
        } else {
            throw new IOException("Server error: " + response);
        }
    }

    @Override
    public void close() throws IOException {
        if (socket != null) socket.close();
    }
}
