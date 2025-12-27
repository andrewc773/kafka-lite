package com.distributed.systems.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class LogTest {
    @TempDir
    Path tempDir;

    @Test
    public void testLogWrapper() throws IOException {
        Path logDir = tempDir.resolve("kafka-logs");
        Log log = new Log(logDir);

        long offset = log.append("Hello-Log-Manager".getBytes());
        assertArrayEquals("Hello-Log-Manager".getBytes(), log.read(offset));

        log.close();
    }
}
