package com.distributed.systems.storage;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class LogSegmentTest {

  @TempDir Path tempDir; // JUnit handles creating and deleting this folder for you

  @Test
  public void testAppendAndReadIntegrity() throws IOException {
    Path logPath = tempDir.resolve("test.data");
    LogSegment segment = new LogSegment(logPath);

    byte[] message1 = "Apples".getBytes();
    byte[] message2 = "Bananas".getBytes();

    long physicalPos1 = segment.getFileSize();
    long offset1 = segment.append(message1);

    long physicalPos2 = segment.getFileSize();
    long offset2 = segment.append(message2);

    assertArrayEquals(message1, segment.readAt(physicalPos1));
    assertArrayEquals(message2, segment.readAt(physicalPos2));

    assertEquals(0, offset1);
    assertEquals(1, offset2);
  }

  @Test
  public void testLargeMessageSequence() throws IOException {
    Path logPath = tempDir.resolve("large.data");
    LogSegment segment = new LogSegment(logPath);

    for (int i = 0; i < 100; i++) {
      byte[] data = ("Message-Content-Number-" + i).getBytes();

      // Track the physical start BEFORE we append
      long physicalPos = segment.getFileSize();

      long logicalOffset = segment.append(data);

      // Verify the data using the physical address
      assertArrayEquals(
          data, segment.readAt(physicalPos), "Data mismatch at logical offset " + logicalOffset);
    }
    segment.close();
  }

  @Test
  public void testConcurrentAppends() throws InterruptedException, IOException {
    Path logPath = tempDir.resolve("concurrent.data");
    LogSegment segment = new LogSegment(logPath);
    int threadCount = 10;
    int msgsPerThread = 50;

    Thread[] threads = new Thread[threadCount];
    for (int i = 0; i < threadCount; i++) {
      threads[i] =
          new Thread(
              () -> {
                try {
                  for (int j = 0; j < msgsPerThread; j++) {
                    segment.append("thread-data".getBytes());
                  }
                } catch (IOException e) {
                  fail("Thread failed: " + e.getMessage());
                }
              });
      threads[i].start();
    }

    for (Thread t : threads) t.join();

    // Verify file size: (4-byte prefix + 11-byte string) * 500 total messages
    long expectedSize = (4 + 11) * threadCount * msgsPerThread;
    assertEquals(
        expectedSize, segment.getFileSize(), "File size mismatch indicates a race condition!");
    segment.close();
  }
}
