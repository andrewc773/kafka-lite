package com.distributed.systems.storage;

import java.nio.file.Paths;

public class Main {
    public static void main(String[] args) {
        try {
            // Initialize the log segment
            LogSegment segment = new LogSegment(Paths.get("kafka_lite.data"));

            // Append a test message
            String testMessage = "Log-structured storage test";
            long offset = segment.append(testMessage.getBytes());

            System.out.println("Success! Message stored at physical offset: " + offset);

            segment.close();
        } catch (Exception e) {
            System.err.println("Critical Failure: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
