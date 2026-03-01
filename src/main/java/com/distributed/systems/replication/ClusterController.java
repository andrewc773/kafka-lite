package com.distributed.systems.replication;

import com.distributed.systems.model.BrokerAddress;
import com.distributed.systems.util.Logger;
import com.distributed.systems.util.Protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class ClusterController implements Runnable {
    private final BrokerAddress leaderAddress;
    private final List<BrokerAddress> followers;

    private final int FAILURE_THRESHOLD = 3;
    private int consecutiveFailures = 0;
    private BrokerAddress activeLeader;

    public ClusterController(BrokerAddress leader, List<BrokerAddress> followers) {
        this.leaderAddress = leader;
        this.activeLeader = leader;
        this.followers = new CopyOnWriteArrayList<>(followers);
    }

    @Override
    public void run() {
        Logger.logBootstrap("ClusterController started. Monitoring Leader: " + activeLeader.host() + ":" + activeLeader.port());

        while (!Thread.currentThread().isInterrupted()) {
            boolean leaderAlive = isAlive(activeLeader.host(), activeLeader.port());

            if (!leaderAlive) {
                consecutiveFailures++;
                Logger.logError("Heartbeat failed (" + consecutiveFailures + "/" + FAILURE_THRESHOLD + ")");

                if (consecutiveFailures >= FAILURE_THRESHOLD) {
                    Logger.logError("CRITICAL: Leader " + activeLeader + " confirmed down. Electing new leader...");
                    electNewLeader();
                    //reset failure count now that have a new active leader
                    consecutiveFailures = 0;
                }
            } else {
                //Fencing check: If the original leader comes back but is no longer the active leader, demote it
                if (!activeLeader.equals(leaderAddress)) {
                    checkAndDemoteOldLeader();
                }
                consecutiveFailures = 0;
            }
            printClusterStatus();

            try {
                Thread.sleep(2000); // check every 2 seconds
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                Logger.logInfo("ClusterController shutting down due to interruption.");
            }
        }
    }

    private long fetchOffsetFromBroker(BrokerAddress candidate, String topic) {
        try (Socket socket = new Socket(candidate.host(), candidate.port());
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {

            out.writeUTF(Protocol.CMD_GET_OFFSET);
            out.writeUTF(topic);
            out.flush();

            return in.readLong();

        } catch (IOException e) {
            Logger.logError("Failed to fetch offset from candidate" + candidate + ": " + e.getMessage());
        }

        return -1L;
    }

    private void electNewLeader() {
        BrokerAddress winner = null;
        long highestOffset = -1;

        // Try to find the node with the most data
        for (BrokerAddress candidate : followers) {
            long offset = fetchOffsetFromBroker(candidate, "p1");
            if (offset > highestOffset) {
                highestOffset = offset;
                winner = candidate;
            }
        }

        // SAFETY NET: If nobody has "p1" (everyone returned -1 or 0)
        // Just pick the first candidate that is actually online. For now, this is always
        // what we default to.
        if (winner == null) {
            for (BrokerAddress candidate : followers) {
                if (isAlive(candidate.host(), candidate.port())) {
                    winner = candidate;
                    Logger.logInfo("No topic data found. Picking " + winner + " as leader by availability.");
                    break;
                }
            }
        }

        if (winner != null) {
            sendPromoteCommand(winner);
            this.activeLeader = winner;
        }

        for (BrokerAddress f : followers) {
            if (!f.equals(winner)) {
                sendUpdateLeaderCommand(f, winner);
            }
        }
    }

    private void sendUpdateLeaderCommand(BrokerAddress targetFollower, BrokerAddress newLeader) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(targetFollower.host(), targetFollower.port()), 2000);

            try (DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {
                out.writeUTF(Protocol.CMD_UPDATE_LEADER);
                out.writeUTF(newLeader.host());
                out.writeInt(newLeader.port());
                out.flush();

                Logger.logInfo("Redirected follower " + targetFollower + " to new leader " + newLeader);
            }
        } catch (IOException e) {
            Logger.logWarning("Could not notify follower " + targetFollower + " of leader change: " + e.getMessage());
        }
    }

    private void sendPromoteCommand(BrokerAddress winner) {
        Logger.logBootstrap("Promoting broker " + winner + " to LEADER...");

        try (Socket socket = new Socket()) {
            socket.connect(new java.net.InetSocketAddress(winner.host(), winner.port()), 2000);

            try (DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                 DataInputStream in = new DataInputStream(socket.getInputStream())) {

                out.writeUTF(Protocol.CMD_PROMOTE);
                out.flush();

                String response = in.readUTF();
                if ("PROMOTED_SUCCESSFULLY".equals(response)) {
                    Logger.logNetwork("Broker " + winner + " confirmed promotion.");
                }
            }
        } catch (IOException e) {
            Logger.logError("CRITICAL: Failed to promote winner " + winner + ": " + e.getMessage());
            // Todo: Consider retry or pick the next best candidate here
        }
    }

    private void checkAndDemoteOldLeader() {
        if (isAlive(leaderAddress.host(), leaderAddress.port())) {
            Logger.logWarning("ZOMBIE DETECTED: Old leader " + leaderAddress + " is back online. Issuing demotion.");

            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(leaderAddress.host(), leaderAddress.port()), 2000);

                try (DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                     DataInputStream in = new DataInputStream(socket.getInputStream())) {

                    out.writeUTF(Protocol.CMD_DEMOTE);
                    out.writeUTF(activeLeader.host());
                    out.writeInt(activeLeader.port());
                    out.flush();

                    //confirmation before adding back to rotation
                    String response = in.readUTF();
                    if ("DEMOTED_SUCCESSFULLY".equals(response)) {
                        Logger.logInfo("Zombie " + leaderAddress + " successfully demoted and reintegrated.");

                        // Reintegration
                        if (!followers.contains(leaderAddress)) {
                            followers.add(leaderAddress);
                            Logger.logInfo("Added " + leaderAddress + " back to the active followers list.");
                        }
                    }
                }
            } catch (IOException e) {
                Logger.logError("Failed to demote zombie: " + e.getMessage());
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


    private void printClusterStatus() {
        System.out.println("\n" + Logger.BOLD_YELLOW + "--- Cluster Health Report ---" + Logger.RESET);
        System.out.printf("%-20s | %-10s | %-10s%n", "Broker", "Role", "Status");
        System.out.println("---------------------------------------------");

        // Check the leader
        String leaderStatus = isAlive(leaderAddress.host(), leaderAddress.port()) ? "ALIVE" : "DEAD";
        String leaderRole = activeLeader.equals(leaderAddress) ? "LEADER" : "ZOMBIE";
        System.out.printf("%-20s | %-10s | %-10s%n", leaderAddress, leaderRole, leaderStatus);

        // Check the followers
        for (BrokerAddress f : followers) {
            String fStatus = isAlive(f.host(), f.port()) ? "ALIVE" : "DEAD";
            String fRole = activeLeader.equals(f) ? "LEADER" : "FOLLOWER";
            System.out.printf("%-20s | %-10s | %-10s%n", f, fRole, fStatus);
        }
        System.out.println("---------------------------------------------\n");
    }
}
