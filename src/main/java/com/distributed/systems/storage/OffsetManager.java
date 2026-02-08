package com.distributed.systems.storage;

import com.distributed.systems.util.Logger;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class OffsetManager {
    private static final String OFFSET_TOPIC = "__consumer_offsets";
    private final TopicManager topicManager;

    // Memory Cache: "groupId:topicName" -> long offset
    private final ConcurrentHashMap<String, Long> offsetCache = new ConcurrentHashMap<>();

    public OffsetManager(TopicManager topicManager) {
        this.topicManager = topicManager;
//        loadExistingOffsets();
    }

    public void commit(String groupId, String topic, long offset) throws IOException {
        String key = groupId + ":" + topic;

        // write to the internal persistent OFFSET_TOPIC
        topicManager.getLogIfExits(OFFSET_TOPIC).append(
                key.getBytes(),
                String.valueOf(offset).getBytes()
        );

        // update the fast-lookup cache
        offsetCache.put(key, offset);
        Logger.logInfo("Offset committed: " + key + " -> " + offset);
    }


    /**
     * Retrieves the bookmark. Returns -1 if the group is new.
     */
    public long fetch(String groupId, String topic) {
        return offsetCache.getOrDefault(groupId + ":" + topic, -1L);
    }

}
