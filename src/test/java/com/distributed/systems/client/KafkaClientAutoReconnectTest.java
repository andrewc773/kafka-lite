package com.distributed.systems.client;

import com.distributed.systems.util.Protocol;
import org.junit.jupiter.api.*;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class KafkaLiteClientAutoReconnectTest {

    private ServerSocket mockServer;
    private int port;
    private volatile boolean running = true;
    private AtomicInteger connectionCount = new AtomicInteger(0);

    @BeforeEach
    void setup() throws IOException {
        mockServer = new ServerSocket(0);
        port = mockServer.getLocalPort();
        running = true;
        connectionCount.set(0);
    }

    @AfterEach
    void tearDown() throws IOException {
        running = false;
        if (mockServer != null && !mockServer.isClosed()) {
            mockServer.close();
        }
    }

    @Test
    @DisplayName("Should reconnect and succeed if the first connection is severed")
    void testAutoReconnectSuccess() throws Exception {
        Thread serverThread = new Thread(() -> {
            try {
                while (running) {
                    // Use standard socket accept
                    Socket client = null;
                    try {
                        client = mockServer.accept();
                    } catch (IOException e) {
                        if (!running) break;
                        throw e;
                    }

                    int currentConn = connectionCount.incrementAndGet();

                    try (DataInputStream in = new DataInputStream(client.getInputStream());
                         DataOutputStream out = new DataOutputStream(client.getOutputStream())) {

                        if (currentConn == 1) {
                            // Hard close immediately on first attempt
                            client.close();
                            continue;
                        }

                        // SUCCESS PATH: Read binary command
                        String command = in.readUTF();
                        String topic = in.readUTF();
                        
                        if (Protocol.CMD_PRODUCE.equals(command)) {
                            // Drain the binary payload: KeyLen -> Key -> ValLen -> Val
                            int kLen = in.readInt();
                            in.readFully(new byte[kLen]);
                            int vLen = in.readInt();
                            in.readFully(new byte[vLen]);

                            // Write binary response
                            out.writeLong(555);
                            out.flush();
                        }
                    } catch (EOFException ignored) {
                        // Handle client disconnect
                    } finally {
                        if (client != null && !client.isClosed()) client.close();
                    }
                }
            } catch (IOException ignored) {
            }
        });
        serverThread.start();

        // Execute Client Logic
        try (KafkaLiteClient client = new KafkaLiteClient("localhost", port)) {
            // Updated call signature: Key="my-key", Value="Test Data"
            long offset = client.produce("my-topic", "my-key", "Test Data");

            assertEquals(555, offset, "Client should have successfully retried and received offset 555");
            assertEquals(2, connectionCount.get(), "Should have connected exactly twice (initial + retry)");
        }
    }
}