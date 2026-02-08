package com.distributed.systems.storage;

import com.distributed.systems.config.BrokerConfig;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.*;
import java.util.Comparator;

import static org.junit.jupiter.api.Assertions.*;

public class OffsetManagerTest {
    private static final String TEST_DIR = "test_data_offsets";
    private TopicManager topicManager;
    private OffsetManager offsetManager;

    @BeforeEach
    void setUp() throws IOException {
        if (Files.exists(Paths.get(TEST_DIR))) {
            Files.walk(Paths.get(TEST_DIR))
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(java.io.File::delete);
        }
        topicManager = new TopicManager(Paths.get(TEST_DIR), new BrokerConfig());
        offsetManager = new OffsetManager(topicManager);
    }

    @Test
    @DisplayName("Should commit and fetch offsets correctly")
    void testCommitAndFetch() throws IOException {
        offsetManager.commit("group-a", "topic-1", 100L);
        offsetManager.commit("group-b", "topic-1", 200L);

        assertEquals(100L, offsetManager.fetch("group-a", "topic-1"));
        assertEquals(200L, offsetManager.fetch("group-b", "topic-1"));
        assertEquals(-1L, offsetManager.fetch("new-group", "topic-1"));
    }

    @Test
    @DisplayName("Should recover offsets after a restart (Persistence Check)")
    void testRecoveryAfterRestart() throws IOException {
        offsetManager.commit("persistent-group", "orders", 500L);

        // Simulate shutdown
        topicManager.shutdown();

        // 3. Simulate restart by creating new managers on the same directory
        TopicManager newTopicManager = new TopicManager(Paths.get(TEST_DIR), new BrokerConfig());
        OffsetManager newOffsetManager = new OffsetManager(newTopicManager);

        // verify the bookmark survived
        assertEquals(500L, newOffsetManager.fetch("persistent-group", "orders"));
    }
}