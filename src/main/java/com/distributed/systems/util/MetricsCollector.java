package com.distributed.systems.util;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/*
 * Using Monotonic Clocks (nanoTime) for latency to ensure safety against NTP clock jumps,
 * and Time-of-Day Clocks (currentTimeMillis) for uptime and logging where absolute
 * calendar time is more relevant than nanosecond precision..
 * */
public class MetricsCollector {

    private final LongAdder totalMessages = new LongAdder();
    private final AtomicLong lastLatencyNano = new AtomicLong(0);
    // Wall clock is fine for the start time of the server
    private final long serverStartTimeMillis = System.currentTimeMillis();

    public void recordMessage(long startNano) {
        long durationNano = System.nanoTime() - startNano;
        totalMessages.increment();

        // Convert to ms for the report, but use double for precision if needed
        long latencyMs = durationNano / 1_000_000;
        lastLatencyNano.set(latencyMs);
    }

    public String getStatsReport(long diskUsageBytes) {
        // Wall clock used to calculate macro uptime
        long uptimeSec = (System.currentTimeMillis() - serverStartTimeMillis) / 1000;

        // Calculate throughput
        double msgPerSec = uptimeSec == 0 ? totalMessages.sum() : (double) totalMessages.sum() / uptimeSec;

        // Convert the precision nano latency back to ms for human reading
        long latencyMs = lastLatencyNano.get() / 1_000_000;

        return String.format("UPTIME=%ds, MSG_COUNT=%d, MSG_PER_SEC=%.2f, LAST_LATENCY=%dms, DISK_USAGE=%dKB",
                uptimeSec, totalMessages.sum(), msgPerSec, latencyMs, diskUsageBytes / 1024);
    }
}
