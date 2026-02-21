package com.distributed.systems.replication;

import com.distributed.systems.client.KafkaLiteClient;
import com.distributed.systems.config.BrokerConfig;
import com.distributed.systems.server.BrokerServer;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.*;

public class ReplicationIntegrationTest {
    private BrokerServer leader;
    private BrokerServer follower;
    private final String TOPIC = "orders-topic";

    @BeforeEach
    void setup() throws IOException {
        // Clear old test data
        cleanDir(Paths.get("data/leader"));
        cleanDir(Paths.get("data/follower"));

        // 1. Start Leader on 9001
        BrokerConfig lConfig = new BrokerConfig();
        lConfig.setProperty("replication.is.leader", "true");
        leader = new BrokerServer(9001, "data/leader", lConfig);
        new Thread(leader::start).start();

        // 2. Start Follower on 9002
        BrokerConfig fConfig = new BrokerConfig();
        fConfig.setProperty("replication.is.leader", "false");
        fConfig.setProperty("replication.leader.port", "9001");
        follower = new BrokerServer(9002, "data/follower", fConfig);
        new Thread(follower::start).start();

        // Brief pause for port binding
        try {
            Thread.sleep(500);
        } catch (InterruptedException ignored) {
        }
    }

    @Test
    void testReplicationAndOffsetTracking() throws Exception {
        KafkaLiteClient leaderClient = new KafkaLiteClient("localhost", 9001, "test-group");

        leaderClient.produce(TOPIC, "order1", "iPhone");
        leaderClient.produce(TOPIC, "order2", "MacBook");


        System.out.println("Waiting for ReplicationManager to discover topic...");
        Thread.sleep(10000);

        KafkaLiteClient followerClient = new KafkaLiteClient("localhost", 9002, "test-group");


        assertDoesNotThrow(() -> followerClient.consume(TOPIC, 0));
        assertDoesNotThrow(() -> followerClient.consume(TOPIC, 1));

        followerClient.commitOffset(TOPIC, 1);
        long fetched = followerClient.fetchOffset(TOPIC);
        assertEquals(1, fetched, "Follower should track group offsets independently");
    }

    @Test
    void testBatchReplication() throws Exception {
        // Make these final or effectively final
        final String host = "localhost";
        final int leaderPort = 9001;
        final int followerPort = 9002;
        final String batchTopic = "batch-test";

        KafkaLiteClient leaderClient = new KafkaLiteClient(host, leaderPort, "batch-group");

        // Produce 150 messages
        for (int i = 0; i < 150; i++) {
            leaderClient.produce(batchTopic, "key-" + i, "data-" + i);
        }

        Thread.sleep(8000);

        KafkaLiteClient followerClient = new KafkaLiteClient(host, followerPort, "batch-group");

        for (int i = 0; i < 150; i++) {
            final int currentOffset = i; // Create a final copy for the lambda
            assertDoesNotThrow(() -> {
                // Use the final copy here
                followerClient.consume(batchTopic, currentOffset);
            }, "Follower missing record " + i);
        }
    }

    @AfterEach
    void tearDown() {
        leader.stop();
        follower.stop();
    }

    private void cleanDir(Path path) throws IOException {
        if (Files.exists(path)) {
            Files.walk(path).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(java.io.File::delete);
        }
    }
}