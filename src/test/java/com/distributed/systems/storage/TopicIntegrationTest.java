package com.distributed.systems.storage;

import com.distributed.systems.client.KafkaLiteClient;
import com.distributed.systems.config.BrokerConfig;
import com.distributed.systems.server.BrokerServer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class TopicIntegrationTest {

    @TempDir
    Path tempDir;

    private BrokerServer server;
    private int port = 9095;

    @BeforeEach
    void setup() throws IOException {
        server = new BrokerServer(port, tempDir.toString(), new BrokerConfig());
        new Thread(server::start).start();
        // Small sleep to ensure server is bound to port
        try {
            Thread.sleep(100);
        } catch (InterruptedException ignored) {
        }
    }

    @AfterEach
    void tearDown() {
        server.stop();
    }

    @Test
    public void testTopicIsolationAndIndependentOffsets() throws IOException {
        try (KafkaLiteClient client = new KafkaLiteClient("localhost", port, "my-group-id")) {
            // 1. Produce to Topic A
            long offsetA0 = client.produce("orders", "user_1", "pizza");
            long offsetA1 = client.produce("orders", "user_2", "burger");

            // 2. Produce to Topic B
            long offsetB0 = client.produce("payments", "user_1", "$15.00");

            // ASSERT: Both topics should have started at offset 0
            assertEquals(0, offsetA0, "Topic A first offset should be 0");
            assertEquals(1, offsetA1, "Topic A second offset should be 1");
            assertEquals(0, offsetB0, "Topic B first offset should also be 0");

            // 3. Verify Isolation (Consume from Topic B)
            // We'll need a way to capture output, but for now, we verify it doesn't throw errors
            // and returns the correct metadata.
            assertDoesNotThrow(() -> client.consume("payments", 0));
            assertDoesNotThrow(() -> client.consume("orders", 1));
        }
    }

    @Test
    public void testConsumeNonExistentTopic() throws IOException {
        try (KafkaLiteClient client = new KafkaLiteClient("localhost", port, "my-group-id")) {
            // We expect the client to throw an exception because 'found' will be false
            IOException exception = assertThrows(IOException.class, () -> {
                client.consume("ghost-topic", 0);
            });

            assertTrue(exception.getMessage().contains("does not exist"),
                    "Error message should mention the topic doesn't exist");
        }
    }

    @Test
    public void testPersistenceAcrossRestarts(@TempDir Path tempDir) throws IOException {
        int port = 9099;
        String dataPath = tempDir.toString();

        BrokerServer server1 = new BrokerServer(port, dataPath, new BrokerConfig());
        new Thread(server1::start).start();

        try (KafkaLiteClient client = new KafkaLiteClient("localhost", port, "my-group-id")) {
            client.produce("persistence-test", "key1", "Permanent Data");
            client.produce("another-persistence-test", "key1", "Hello Again");
        }

        server1.stop();

        try {
            Thread.sleep(200);
        } catch (InterruptedException ignored) {
        }

        // --- The Resurrection ---
        // We point a completely fresh server at the same directory
        BrokerServer server2 = new BrokerServer(port, dataPath, new BrokerConfig());
        new Thread(server2::start).start();

        try (KafkaLiteClient client = new KafkaLiteClient("localhost", port, "my-group-id")) {
            // This will only work if TopicManager discovered the folder
            // and Log discovered the .data files!
            assertDoesNotThrow(() -> client.consume("persistence-test", 0));
            assertDoesNotThrow(() -> client.consume("another-persistence-test", 0));
        }
        server2.stop();
    }
}