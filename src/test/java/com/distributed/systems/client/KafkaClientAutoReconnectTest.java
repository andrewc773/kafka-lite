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
        mockServer = new ServerSocket(0); // Random free port
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
        // start a "fragile" mock server
        Thread serverThread = new Thread(() -> {
            try {
                while (running) {
                    try (Socket client = mockServer.accept()) {
                        connectionCount.incrementAndGet();
                        PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                        BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));

                        // Protocol Welcome
                        out.println(Protocol.WELCOME_HEADER);
                        out.println(Protocol.WELCOME_HELP);

                        String line = in.readLine();
                        if (connectionCount.get() == 1) {
                            // SIMULATE FAILURE: Kill the connection on the first attempt
                            client.close();
                        } else {
                            // SUCCESS: Handle normally on second connection
                            if (line != null && line.startsWith(Protocol.CMD_PRODUCE)) {
                                out.println(Protocol.formatSuccess(555));
                            }
                        }
                    }
                }
            } catch (IOException ignored) {
            }
        });
        serverThread.start();

        // execute client Logic
        try (KafkaLiteClient client = new KafkaLiteClient("localhost", port)) {
            // This call should trigger the catch block, reconnect, and then return 555
            long offset = client.produce("Test Data");

            assertEquals(555, offset, "Client should have successfully retried and received offset 555");
            assertEquals(2, connectionCount.get(), "Should have connected exactly twice (initial + retry)");
        }
    }
}