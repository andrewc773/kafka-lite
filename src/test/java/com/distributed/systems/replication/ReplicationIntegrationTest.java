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
        // Use Group ID "test-group"
        KafkaLiteClient leaderClient = new KafkaLiteClient("localhost", 9001, "test-group");

        // 1. Produce to Leader
        leaderClient.produce(TOPIC, "order1", "iPhone");
        leaderClient.produce(TOPIC, "order2", "MacBook");


        // 2. WAIT for Discovery (Manager polls every 5s) + Sync
        System.out.println("Waiting for ReplicationManager to discover topic...");
        Thread.sleep(10000);

        // 3. Consume from Follower (Client connects to 9002)
        KafkaLiteClient followerClient = new KafkaLiteClient("localhost", 9002, "test-group");

        // We expect to find the data there
        // Note: You might need to update your client.consume to return the record
        // instead of just printing it for the assertion to work.
        assertDoesNotThrow(() -> followerClient.consume(TOPIC, 0));
        assertDoesNotThrow(() -> followerClient.consume(TOPIC, 1));

        // 4. Verify Offset Management on Follower
        followerClient.commitOffset(TOPIC, 1);
        long fetched = followerClient.fetchOffset(TOPIC);
        assertEquals(1, fetched, "Follower should track group offsets independently");
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