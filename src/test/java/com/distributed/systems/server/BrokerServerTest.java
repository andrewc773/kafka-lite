package com.distributed.systems.server;

import com.distributed.systems.client.KafkaLiteClient;
import com.distributed.systems.config.BrokerConfig;
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

        Path path = Path.of(tempDir.toString());

        // Clean up existing data from previous failed runs
        if (Files.exists(path)) {
            Files.walk(path)
                    .sorted(java.util.Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(java.io.File::delete);
        }

        // recreate the clean directory
        Files.createDirectories(path);

        server = new BrokerServer(testPort, tempDir.toString(), createDefaultConfig());
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

    // Helper to create a standard config for tests
    private BrokerConfig createDefaultConfig() {
        return new BrokerConfig(2048, 600000, 4096, 30000);
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
        BrokerServer server = new BrokerServer(port, Files.createTempDirectory("stress-test").toString(), createDefaultConfig());

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
                try (KafkaLiteClient client = new KafkaLiteClient("localhost", port, "my-group-id")) {
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
        try (KafkaLiteClient finalClient = new KafkaLiteClient("localhost", port, "my-group-id")) {
            String stats = finalClient.getStats();
            Logger.logDebug(stats);
            assertTrue(stats.contains("MSG_COUNT=500"), "Stats should show 500 messages");
        } finally {
            clients.shutdown();
        }
    }

    @Test
    public void testGracefulShutdownPersistence(@TempDir Path tempDir) throws IOException {
        int port = 9097;
        String dataPath = tempDir.toString();

        BrokerServer server = new BrokerServer(port, dataPath, createDefaultConfig());
        new Thread(server::start).start();

        try (KafkaLiteClient client = new KafkaLiteClient("localhost", port, "my-group-id")) {
            client.produce("shutdown-test", "key", "important-data");
        }

        server.stop();

        // Give a small buffer for OS file locks to release
        try {
            Thread.sleep(300);
        } catch (InterruptedException ignored) {
        }

        Path topicDir = tempDir.resolve("shutdown-test");
        assertTrue(Files.exists(topicDir), "Topic directory should exist");

        // Check if index and data files were created and flushed
        File[] files = topicDir.toFile().listFiles();
        assertNotNull(files);
        assertTrue(files.length >= 2, "Should have at least index and data files");
    }

    @Test
    public void testServerRecoveryAfterGracefulShutdown(@TempDir Path tempDir) throws IOException {
        int port = 9098;
        String dataPath = tempDir.toString();

        BrokerServer server1 = new BrokerServer(port, dataPath, createDefaultConfig());
        new Thread(server1::start).start();

        try (KafkaLiteClient client = new KafkaLiteClient("localhost", port, "my-group-id")) {
            client.produce("recovery-topic", "k1", "v1");
        }
        server1.stop();

        // restart a fresh server on the same path
        BrokerServer server2 = new BrokerServer(port, dataPath, createDefaultConfig());
        new Thread(server2::start).start();

        try (KafkaLiteClient client = new KafkaLiteClient("localhost", port, "my-group-id")) {


            // If recovery logic works, this topic should be 'discovered' on boot
            // and we can consume from offset 0 immediately.
            assertDoesNotThrow(() -> client.consume("recovery-topic", 0));
        }
        server2.stop();
    }

    @Test
    void testHandleGetOffset(@TempDir Path tempDir) throws IOException {

        String dataPath = tempDir.toString();

        // 1. Setup Broker
        BrokerConfig config = new BrokerConfig();
        config.setProperty("replication.is.leader", "true");
        BrokerServer server = new BrokerServer(9090, dataPath, config);

        // Create a topic and append data
        server.getTopicManager().getOrCreateLog("test-topic").append("key".getBytes(), "val".getBytes());

        // 2. PREPARE THE INPUT PROPERLY
        ByteArrayOutputStream inputPrepStream = new ByteArrayOutputStream();
        DataOutputStream inputPrepData = new DataOutputStream(inputPrepStream);
        inputPrepData.writeUTF("test-topic"); // This writes the 2-byte length + "test-topic"

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(inputPrepStream.toByteArray()));

        // Prepare the output capture
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(outStream);

        // 3. Trigger handler
        server.handleGetOffset(dis, dos);

        // 4. Verify Output
        DataInputStream resultIn = new DataInputStream(new ByteArrayInputStream(outStream.toByteArray()));
        assertEquals(1, resultIn.readLong(), "Should return offset 1 for the existing topic");
    }

    @Test
    void testHandleDemoteStateTransition() throws IOException {
        // 1. Start as a Leader
        BrokerConfig config = new BrokerConfig();
        config.setProperty("replication.is.leader", "true");
        BrokerServer server = new BrokerServer(9091, "data/test-demote", config);

        assertTrue(server.getConfig().isLeader(), "Should initially be leader");

        // 2. Mock the DEMOTE payload: [NewLeaderHost][NewLeaderPort]
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        dos.writeUTF("new-host");
        dos.writeInt(9999);

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bos.toByteArray()));
        DataOutputStream serverOut = new DataOutputStream(new ByteArrayOutputStream());

        // 3. Trigger Demotion
        server.handleDemote(dis, serverOut);

        // 4. Assertions
        assertFalse(server.getConfig().isLeader(), "Broker should no longer be leader");
        assertEquals("new-host", server.getConfig().getProperty("replication.leader.host"));
        assertEquals("9999", server.getConfig().getProperty("replication.leader.port"));

        // Verify ReplicationManager is active (as a follower)
        assertNotNull(server.getReplicationManager());
        assertFalse(server.getReplicationManager().isShutdown());
    }
}