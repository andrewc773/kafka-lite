package com.distributed.systems.network;

import com.distributed.systems.client.KafkaLiteClient;
import com.distributed.systems.util.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class BrokerServerTest {

    @TempDir
    Path tempDir;

    private BrokerServer server;
    private final int testPort = 9093; // different port than main app
    private Thread serverThread;

    @BeforeEach
    public void setup() throws IOException {
        server = new BrokerServer(testPort, tempDir.toString());
        // Run server in a background thread so the test isn't blocked
        serverThread = new Thread(() -> server.start());
        serverThread.start();

        // Give the server a moment to bind to the port
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
        }
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void testProduceAndConsumeOverNetwork() throws IOException {
        try (Socket socket = new Socket("localhost", testPort);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {

            out.writeUTF("PRODUCE");
            out.writeUTF("test-topic");

            byte[] key = "net-key".getBytes();
            out.writeInt(key.length);
            out.write(key);

            byte[] value = "Network-Message".getBytes();
            out.writeInt(value.length);
            out.write(value);
            out.flush(); // Ensure the command is sent!

            long offset = in.readLong();
            assertEquals(0, offset, "Should return offset 0 for first message");

            out.writeUTF("CONSUME");
            out.writeUTF("test-topic");
            out.writeLong(0);
            out.flush();

            boolean found = in.readBoolean();
            assertTrue(found, "Message should be found at offset 0");

            long resOffset = in.readLong();
            long timestamp = in.readLong();

            int resKeyLen = in.readInt();
            byte[] resKey = new byte[resKeyLen];
            in.readFully(resKey);

            int resValLen = in.readInt();
            byte[] resValue = new byte[resValLen];
            in.readFully(resValue);

            assertEquals("Network-Message", new String(resValue), "Should retrieve the correct data");
        }
    }

    @Test
    public void testInvalidCommands() throws IOException {
        try (Socket socket = new Socket("localhost", testPort);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {

            out.writeUTF("GARBAGE_COMMAND");
            out.flush();

            // 2. Read the response using binary readUTF
            // This matches the server's: out.writeUTF("ERROR: Unknown Command");
            String response = in.readUTF();

            assertTrue(response.contains("ERROR"), "Server should respond with error for unknown commands");
        }
    }

    @Test
    void testBrokerUnderHighContention() throws Exception {
        int port = 9093;
        BrokerServer server = new BrokerServer(port, Files.createTempDirectory("stress-test").toString());

        // Start server in its own thread
        Thread serverThread = new Thread(server::start);
        serverThread.start();
        Thread.sleep(500);

        int messageCount = 500;
        int threadCount = 20;
        ExecutorService clients = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(messageCount);

        for (int i = 0; i < messageCount; i++) {
            clients.submit(() -> {
                // Every task gets its own connection/client
                try (KafkaLiteClient client = new KafkaLiteClient("localhost", port)) {
                    client.produce("my-topic", "my-key", "Contention Test Message");
                    latch.countDown();
                } catch (IOException e) {
                    System.err.println("Client failed: " + e.getMessage());
                }
            });
        }

        boolean finished = latch.await(15, TimeUnit.SECONDS);
        assertTrue(finished, "Broker failed to process messages within the timeout");

        // Check final stats with a fresh connection
        try (KafkaLiteClient finalClient = new KafkaLiteClient("localhost", port)) {
            String stats = finalClient.getStats();
            Logger.logDebug(stats);
            assertTrue(stats.contains("MSG_COUNT=500"), "Stats should show 500 messages");
        } finally {
            clients.shutdown();
        }
    }
}