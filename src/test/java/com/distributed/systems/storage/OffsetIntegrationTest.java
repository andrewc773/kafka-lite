package com.distributed.systems.storage;

import com.distributed.systems.client.KafkaLiteClient;
import com.distributed.systems.server.BrokerServer;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.Comparator;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

public class OffsetIntegrationTest {
    private static final String DATA_DIR = "integration_test_data";
    private static final int PORT = 9093;
    private static final String TOPIC = "orders";
    private static final String GROUP = "shipping-service";
    private BrokerServer server;

    @BeforeEach
    void setUp() throws IOException {
        // Clear old data
        if (Files.exists(Paths.get(DATA_DIR))) {
            Files.walk(Paths.get(DATA_DIR)).sorted(Comparator.reverseOrder())
                    .map(Path::toFile).forEach(java.io.File::delete);
        }
        server = new BrokerServer(PORT, DATA_DIR);
        new Thread(server::start).start();

        // Give the server a moment to bind to the port
        try {
            Thread.sleep(500);
        } catch (InterruptedException ignored) {
        }
    }

    @AfterEach
    void tearDown() {
        server.stop();
    }

    @Test
    @DisplayName("Consumer should resume from last committed offset after crash")
    void testConsumerResume() throws IOException {
        // produce 10 messages
        try (KafkaLiteClient producer = new KafkaLiteClient("localhost", PORT, "producer-group")) {
            for (int i = 0; i < 10; i++) {
                producer.produce(TOPIC, "key-" + i, "val-" + i);
            }
        }

        // first Consumer: process 5 messages and then crash
        System.out.println("--- Starting First Consumer (Processing 0-4) ---");
        try (KafkaLiteClient consumer1 = new KafkaLiteClient("localhost", PORT, GROUP)) {
            long startOffset = consumer1.fetchOffset(TOPIC);
            long current = (startOffset == -1) ? 0 : startOffset + 1;

            assertEquals(0, current, "New group should start at 0");

            for (int i = 0; i < 5; i++) {
                consumer1.consume(TOPIC, current);
                consumer1.commitOffset(TOPIC, current);
                current++;
            }
            // consumer1 closes here
        }

        // second Consumer: Start up and check where we are
        System.out.println("--- Starting Second Consumer (Resuming) ---");
        try (KafkaLiteClient consumer2 = new KafkaLiteClient("localhost", PORT, GROUP)) {
            long lastCommitted = consumer2.fetchOffset(TOPIC);
            long resumeOffset = lastCommitted + 1;

            assertEquals(5, resumeOffset, "Consumer should resume at offset 5");
            System.out.println("Success! Resumed at offset: " + resumeOffset);
        }
    }
}