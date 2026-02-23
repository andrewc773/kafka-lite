package com.distributed.systems.replication;

import com.distributed.systems.client.KafkaLiteClient;
import com.distributed.systems.config.BrokerConfig;
import com.distributed.systems.model.BrokerAddress;
import com.distributed.systems.server.BrokerServer;
import com.distributed.systems.util.Logger;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.*;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class ClusterControllerIntegrationTest {

    @TempDir
    Path tempDir;

    private BrokerServer leader;
    private BrokerServer follower;
    private Thread controllerThread;

    private final int LEADER_PORT = 9001;
    private final int FOLLOWER_PORT = 9002;
    private final String TOPIC = "failover-demo";

    @BeforeEach
    void setup() throws IOException {
        Path leaderPath = tempDir.resolve("leader");
        Path followerPath = tempDir.resolve("follower");

        Files.createDirectories(leaderPath);
        Files.createDirectories(followerPath);

        BrokerConfig leaderConfig = new BrokerConfig();
        leaderConfig.setProperty("replication.is.leader", "true");
        // use .toString() on the temp paths
        leader = new BrokerServer(LEADER_PORT, leaderPath.toString(), leaderConfig);
        new Thread(leader::start).start();

        // Follower Setup
        BrokerConfig followerConfig = new BrokerConfig();
        followerConfig.setProperty("replication.is.leader", "false");
        followerConfig.setProperty("replication.leader.host", "localhost");
        followerConfig.setProperty("replication.leader.port", String.valueOf(LEADER_PORT));
        follower = new BrokerServer(FOLLOWER_PORT, followerPath.toString(), followerConfig);
        new Thread(follower::start).start();

        BrokerAddress leaderAddress = new BrokerAddress("localhost", LEADER_PORT);
        List<BrokerAddress> followers = List.of(new BrokerAddress("localhost", FOLLOWER_PORT));

        ClusterController controller = new ClusterController(leaderAddress, followers);
        controllerThread = new Thread(controller);
        controllerThread.start();

        try {
            Thread.sleep(1500);
        } catch (InterruptedException ignored) {
        }
    }

    @Test
    void testAutomaticFailoverFlow() throws Exception {
        KafkaLiteClient producer = new KafkaLiteClient("localhost", LEADER_PORT, "p1");
        producer.produce(TOPIC, "msg1", "Hello from Leader");

        System.out.println("Waiting for replication sync...");
        Thread.sleep(7000);

        System.out.println(">>> SIMULATING CRASH: Stopping Leader on port " + LEADER_PORT);
        leader.stop();

        System.out.println("Waiting for ClusterController to detect failure...");
        Thread.sleep(8000);

        // Verify the Follower is now a Leader by producing to it
        // If promotion failed, this client will receive an ERR_NOT_LEADER or -1 offset
        KafkaLiteClient newLeaderClient = new KafkaLiteClient("localhost", FOLLOWER_PORT, "p1");

        long newOffset = assertDoesNotThrow(() ->
                        newLeaderClient.produce(TOPIC, "msg2", "Hello from New Leader"),
                "Follower should have been promoted and accepted the write"
        );

        assertEquals(1, newOffset, "New message should be at offset 1, following replicated offset 0");
        System.out.println("Successfully produced to new leader at offset: " + newOffset);
    }

    @Test
    void testBrokerStateTransition() throws IOException {
        BrokerConfig config = new BrokerConfig();
        config.setProperty("replication.is.leader", "false"); // Start as follower

        BrokerServer server = new BrokerServer(9005, "data/unit-test-promote", config);

        assertFalse(config.isLeader());

        server.promoteToLeader();

        assertTrue(config.isLeader());
        assertTrue(server.getReplicationManager().isShutdown());
    }

    @Test
    void testZombieLeaderScenario() throws Exception {
        KafkaLiteClient client = new KafkaLiteClient("localhost", 9001, "admin");
        client.produce("zombie-topic", "init", "data-0");
        Thread.sleep(6000); // Wait for Follower to sync

        System.out.println(">>> KILLING LEADER (9001) <<<");
        leader.stop();

        System.out.println("Waiting for Controller to confirm death and promote Follower...");
        Thread.sleep(8000);

        System.out.println(">>> RESTARTING ZOMBIE LEADER (9001) <<<");
        BrokerConfig zombieConfig = new BrokerConfig();
        zombieConfig.setProperty("replication.is.leader", "true"); // it still thinks it's the leader
        BrokerServer zombie = new BrokerServer(9001, "data/leader", zombieConfig);
        new Thread(zombie::start).start();
        Thread.sleep(2000); // Let it boot

        // clients should now be talking to the new Leader (9002)
        KafkaLiteClient newLeaderClient = new KafkaLiteClient("localhost", 9002, "admin");
        long offset = newLeaderClient.produce("zombie-topic", "new-boss", "data-1");

        assertEquals(1, offset, "The New Leader (9002) should accept writes at the correct next offset.");

        zombie.stop();
    }

    @Test
    public void testFullClusterFailoverAndFanOut() throws Exception {
        ClusterMock cluster = new ClusterMock(tempDir.toString());

        // start a 3-node cluster
        BrokerAddress addr1 = cluster.startBroker(10001, true);  // The Leader
        BrokerAddress addr2 = cluster.startBroker(10002, false); // Follower A (Target winner)
        BrokerAddress addr3 = cluster.startBroker(10003, false); // Follower B (To be redirected)

        // Give them a second to initialize
        Thread.sleep(500);

        List<BrokerAddress> followers = List.of(addr2, addr3);
        ClusterController controller = new ClusterController(addr1, followers);
        Thread controllerThread = new Thread(controller);
        controllerThread.start();

        // 3. THE CHAOS: Kill the Leader
        Logger.logWarning("TEST: Killing Leader on port 9001...");
        cluster.stopBroker(10001);

        // 4. WAIT: 3 failures * 2s sleep = 6 seconds. Let's wait 10s for safety.
        Thread.sleep(10000);

        // 5. VERIFY:
        // Did Follower A (9002) become the new active leader?
        assertTrue(cluster.getBroker(10002).getConfig().isLeader(), "Broker 10002 should have been promoted!");

        // Did Follower B (9003) redirect its ReplicationManager to 9002?
        String redirectedHost = cluster.getBroker(10003).getConfig().getProperty("replication.leader.host");
        String redirectedPort = cluster.getBroker(10003).getConfig().getProperty("replication.leader.port");

        assertEquals("localhost", redirectedHost);
        assertEquals("10002", redirectedPort, "Broker 10003 should now be tracking Broker 10002!");

        cluster.shutdownAll();
        controllerThread.interrupt();
    }


    @AfterEach
    void tearDown() {
        if (leader != null) leader.stop();
        if (follower != null) follower.stop();
        if (controllerThread != null) controllerThread.interrupt();
    }

    private void cleanDir(Path path) throws IOException {
        if (Files.exists(path)) {
            Files.walk(path)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(java.io.File::delete);
        }
    }
}