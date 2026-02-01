package com.distributed.systems.client;

import com.distributed.systems.util.Protocol;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaLiteClientTest {
    private static ServerSocket mockServer;
    private static int port;
    private static Thread serverThread;

    @BeforeAll
    static void startMockServer() throws IOException {
        mockServer = new ServerSocket(0);
        port = mockServer.getLocalPort();

        serverThread = new Thread(() -> {
            try {
                while (!mockServer.isClosed()) {
                    // Using try-with-resources inside the loop to handle each connection
                    try (Socket client = mockServer.accept();
                         DataInputStream in = new DataInputStream(client.getInputStream());
                         DataOutputStream out = new DataOutputStream(client.getOutputStream())) {

                        //  Binary only.
                        String command = in.readUTF();

                        if (Protocol.CMD_PRODUCE.equals(command)) {
                            // Protocol: [KeyLen][Key][ValLen][Value]
                            int kLen = in.readInt();
                            in.readFully(new byte[kLen]);
                            int vLen = in.readInt();
                            in.readFully(new byte[vLen]);

                            out.writeLong(999);
                        } else if (Protocol.CMD_CONSUME.equals(command)) {
                            // Protocol: [Offset]
                            in.readLong();

                            // Response: [Found][Offset][Timestamp][KeyLen][Key][ValLen][Value]
                            out.writeBoolean(true);
                            out.writeLong(0);
                            out.writeLong(System.currentTimeMillis());

                            byte[] key = "mock-key".getBytes();
                            out.writeInt(key.length);
                            out.write(key);

                            byte[] val = "MockData".getBytes();
                            out.writeInt(val.length);
                            out.write(val);
                        }
                        out.flush();
                    } catch (EOFException ignored) {
                        // Expected when client closes connection
                    }
                }
            } catch (IOException ignored) {
            }
        });
        serverThread.start();
    }

    @AfterAll
    static void stopMockServer() throws IOException {
        mockServer.close();
    }

    @Test
    void testProduceCommand() throws IOException {
        try (KafkaLiteClient client = new KafkaLiteClient("localhost", port)) {
            long offset = client.produce("my-key", "Hello Mock");
            assertEquals(999, offset);
        }
    }

    @Test
    void testConsumeCommand() throws IOException {
        try (KafkaLiteClient client = new KafkaLiteClient("localhost", port)) {
            client.consume(0);
        }
    }
}