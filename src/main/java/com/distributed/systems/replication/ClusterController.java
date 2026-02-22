package com.distributed.systems.replication;

import com.distributed.systems.util.Logger;
import com.distributed.systems.util.Protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class ClusterController implements Runnable {

    private final String leaderHost;
    private final int leaderPort;
    private final String followerHost;
    private final int followerPort;
    private final int FAILURE_THRESHOLD = 3;
    private int consecutiveFailures = 0;

    public ClusterController(String leaderHost, int leaderPort, String followerHost, int followerPort) {
        this.leaderHost = leaderHost;
        this.leaderPort = leaderPort;
        this.followerHost = followerHost;
        this.followerPort = followerPort;
    }

    @Override
    public void run() {
        Logger.logBootstrap("ClusterController started. Monitoring Leader: " + leaderHost + ":" + leaderPort);

        while (!Thread.currentThread().isInterrupted()) {
            if (!isAlive(leaderHost, leaderPort)) {
                consecutiveFailures++;
                Logger.logError("Heartbeat failed (" + consecutiveFailures + "/" + FAILURE_THRESHOLD + ")");

                if (consecutiveFailures >= FAILURE_THRESHOLD) {
                    Logger.logError("CRITICAL: Leader confirmed down. Initiating failover to " + followerHost);
                    promote(followerHost, followerPort);
                    break;
                }
            } else {
                if (consecutiveFailures > 0) {
                    Logger.logInfo("Leader recovered after " + consecutiveFailures + " failed attempt(s). Resetting counter.");
                }
                consecutiveFailures = 0;
            }
            try {
                Thread.sleep(2000); // check every 2 seconds
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private boolean isAlive(String host, int port) {
        try (Socket s = new Socket()) {
            // attempt to connect with a 2-second timeout
            s.connect(new java.net.InetSocketAddress(host, port), 2000);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private void promote(String host, int port) {
        try (Socket s = new Socket(host, port);
             DataOutputStream out = new DataOutputStream(s.getOutputStream());
             DataInputStream in = new java.io.DataInputStream(s.getInputStream())) {
            out.writeUTF(Protocol.CMD_PROMOTE);
            out.flush();

            String response = in.readUTF();
            Logger.logBootstrap("Promotion result from " + host + ":" + port + " -> " + response);

        } catch (IOException e) {
            Logger.logError("Failed to promote follower: " + e.getMessage());
        }
    }
}
