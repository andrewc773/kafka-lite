package com.distributed.systems.network;

import com.distributed.systems.storage.Log;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Paths;

public class BrokerServer {

    private final Log log;
    private final int port;

    public BrokerServer(int port, String dataDir) throws IOException {
        this.port = port;
        this.log = new Log(Paths.get(dataDir));
    }

    public void start() {
        // Create ServerSocket to listen for incoming TCP connections
        try (ServerSocket serverSocket = new ServerSocket(port)) {

            System.out.println("Broker started on port " + port);
            System.out.println("Data Directory: " + serverSocket.getLocalSocketAddress());

            while (true) {
                // Wait for client to connect
                Socket clientSocket = serverSocket.accept();
                System.out.println("New client connected: " + clientSocket.getLocalSocketAddress());

                // For now, let's handle one client at a time for simplicity
                handleClient(clientSocket);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void handleClient(Socket socket) {
        try (
                BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        ) {
            out.println("--- Welcome to Kafka-Lite Broker ---");
            out.println("Commands: PRODUCE <data> | CONSUME <offset> | QUIT");

            String line;

            while ((line = in.readLine()) != null) {
                line = line.trim();

                if (line.equalsIgnoreCase("QUIT")) {
                    out.println("Goodbye!");
                    break;
                }

                if (line.startsWith("PRODUCE ")) {
                    // extract everything after "PRODUCE "
                    String payload = line.substring(8).trim();
                    if (payload.isEmpty()) {
                        out.println("ERROR: No data provided to produce..");
                        continue;
                    }
                    long offset = log.append(payload.getBytes());
                    out.println("SUCCESS: Message stored at OFFSET " + offset);
                } else if (line.startsWith("CONSUME ")) {
                    try {
                        // Parse the offset from the command
                        long offset = Long.parseLong(line.substring(8).trim());
                        byte[] data = log.read(offset);
                        out.println("DATA: " + new String(data));
                    } catch (NumberFormatException e) {
                        out.println("ERROR: Invalid offset format. Use CONSUME <number>.");
                    } catch (IOException e) {
                        out.println("ERROR: " + e.getMessage()); // Will catch your "Not Found" exception
                    }

                } else {
                    out.println("ERROR: Unknown Command. Try PRODUCE <data> or CONSUME <offset>");
                }

            }

        } catch (IOException e) {
            System.err.println("Connection with client lost: " + e.getMessage());
        } finally {
            try {
                socket.close(); // Ensure socket is closed
            } catch (IOException e) { /* Ignore */ }
        }
    }
}
