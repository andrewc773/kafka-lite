package com.distributed.systems.replication;

import com.distributed.systems.storage.Log;
import com.distributed.systems.util.Logger;
import com.distributed.systems.util.Protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;

public class ReplicaFetcher implements Runnable {
    private final String topic;
    private final Log localLog;
    private final String leaderHost;
    private final int leaderPort;
    private volatile boolean running = true;

    public ReplicaFetcher(String topic, Log localLog, String leaderHost, int leaderPort) {
        this.topic = topic;
        this.localLog = localLog;
        this.leaderHost = leaderHost;
        this.leaderPort = leaderPort;
    }

    @Override
    public void run() {
        Logger.logBootstrap("Starting replication for topic: " + topic);
        while (running) {
            try (Socket socket = new Socket(leaderHost, leaderPort);
                 DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                 DataInputStream in = new DataInputStream(socket.getInputStream())) {

                // Ensure our local log isn't ahead of the leader
                long localNextOffset = localLog.getNextOffset();

                out.writeUTF(Protocol.CMD_GET_OFFSET);
                out.writeUTF(topic);
                out.flush();

                long leaderLastOffset = in.readLong();

                if (localNextOffset > (leaderLastOffset + 1)) {
                    Logger.logWarning("DIVERGENCE: Local log is at " + localNextOffset +
                            " but Leader is at " + leaderLastOffset + ". Truncating...");

                    // Rewind to exactly where the leader is
                    localLog.truncate(leaderLastOffset + 1);
                    continue; // Restart the loop with the corrected offset
                }

                // Ask the leader for a batch
                out.writeUTF(Protocol.CMD_REPLICA_FETCH);
                out.writeUTF(topic);
                out.writeLong(localLog.getNextOffset());
                out.flush();

                int count = in.readInt();

                // If we got 0 records, the leader has no new data.
                if (count == 0) {
                    Thread.sleep(1000); // Wait for new data on leader
                    continue;
                }

                // append records to local log
                for (int i = 0; i < count; i++) {
                    long offset = in.readLong(); // read, but we use localLog.append to keep it simple
                    long ts = in.readLong();
                    byte[] key = new byte[in.readInt()];
                    in.readFully(key);
                    byte[] val = new byte[in.readInt()];
                    in.readFully(val);

                    localLog.append(key, val);
                }

                Logger.logNetwork("Replicated " + count + " records for " + topic);

            } catch (Exception e) {
                Logger.logError("Replication failed for " + topic + ": " + e.getMessage());
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException ignored) {
                }
            }
        }
    }

    public void stop() {
        running = false;
    }
}
