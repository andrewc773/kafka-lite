package com.distributed.systems.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MetricsCollectorTest {

    @Test
    void testMessageRecording() {
        MetricsCollector metrics = new MetricsCollector();

        long start = System.nanoTime() - 5_500_000;
        metrics.recordMessage(start);

        String report = metrics.getStatsReport(1024);

        assertTrue(report.contains("MSG_COUNT=1"), "Message count should be 1");

        // We expect 5ms, but we'll accept 5 or 6 to account for the time the test took to run.
        boolean hasExpectedLatency = report.contains("LAST_LATENCY=5ms") || report.contains("LAST_LATENCY=6ms");

        assertTrue(hasExpectedLatency, "Report should show ~5ms latency. Full Report: " + report);
    }

    @Test
    void testZeroUptimeThroughput() {
        MetricsCollector metrics = new MetricsCollector();
        metrics.recordMessage(System.nanoTime());

        // should not throw ArithmeticException
        String report = metrics.getStatsReport(0);
        assertNotNull(report);
        assertTrue(report.contains("MSG_PER_SEC="));
    }

    @Test
    void testConcurrentUpdates() throws InterruptedException {
        MetricsCollector metrics = new MetricsCollector();
        int threadCount = 10;
        int iterations = 1000;
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.execute(() -> {
                for (int j = 0; j < iterations; j++) {
                    metrics.recordMessage(System.nanoTime());
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS);

        String report = metrics.getStatsReport(0);
        assertTrue(report.contains("MSG_COUNT=10000"), "Should accurately count concurrent updates");
    }

    @Test
    void testUptimeProgression() throws InterruptedException {
        MetricsCollector metrics = new MetricsCollector();

        // Wait 1.1 seconds to ensure the clock crosses the second threshold
        Thread.sleep(1100);

        String report = metrics.getStatsReport(0);
        // see if UPTIME is at least 1s
        assertTrue(report.contains("UPTIME=1s") || report.contains("UPTIME=2s"),
                "Uptime should reflect elapsed wall clock time. Report: " + report);
    }

    @Test
    void testThroughputAccuracy() throws InterruptedException {
        MetricsCollector metrics = new MetricsCollector();

        // Record 10 messages
        for (int i = 0; i < 10; i++) metrics.recordMessage(System.nanoTime());

        // Artificial delay to simulate time passing
        Thread.sleep(2000);

        String report = metrics.getStatsReport(0);
        // Expect roughly 5.00 msg/sec (10 messages / 2 seconds)
        // We check for "5.00" or similar depending on exact sleep timing
        assertTrue(report.contains("MSG_PER_SEC=5.00") || report.contains("MSG_PER_SEC=4.5"),
                "Throughput should be roughly 5.00. Report: " + report);
    }
}