package com.distributed.systems.network;

import com.distributed.systems.config.BrokerConfig;
import com.distributed.systems.storage.Log;
import com.distributed.systems.util.Logger;

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
        this.log = new Log(Paths.get(dataDir), new BrokerConfig());
    }

    public void start() {

        printBanner();
        
        // Create ServerSocket to listen for incoming TCP connections
        try (ServerSocket serverSocket = new ServerSocket(port)) {

            Logger.logNetwork("Broker initialized and listening on port " + port);
            Logger.logInfo("Data Directory: " + serverSocket.getLocalSocketAddress());

            while (true) {
                // Wait for client to connect
                Socket clientSocket = serverSocket.accept();
                Logger.logNetwork("New client connected: " + clientSocket.getRemoteSocketAddress());

                Thread clientThread = new Thread(() -> {
                    handleClient(clientSocket);
                });

                clientThread.start();
            }
        } catch (IOException e) {
            Logger.logError("Server failed to start: " + e.getMessage());
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
                    Logger.logNetwork("PRODUCE request successful. Offset: " + offset);
                    out.println("SUCCESS: Message stored at OFFSET " + offset);
                } else if (line.startsWith("CONSUME ")) {
                    try {
                        // Parse the offset from the command
                        long offset = Long.parseLong(line.substring(8).trim());
                        byte[] data = log.read(offset);

                        Logger.logNetwork("CONSUME request for offset: " + offset);
                        out.println("DATA: " + new String(data));
                    } catch (NumberFormatException e) {
                        out.println("ERROR: Invalid offset format. Use CONSUME <number>.");
                    } catch (IOException e) {
                        Logger.logError("Failed consume at offset " + line.substring(8) + ": " + e.getMessage());
                        out.println("ERROR: " + e.getMessage()); // Will catch your "Not Found" exception
                    }

                } else {
                    out.println("ERROR: Unknown Command. Try PRODUCE <data> or CONSUME <offset>");
                }

            }

        } catch (IOException e) {
            Logger.logError("Connection lost with " + socket.getRemoteSocketAddress() + ": " + e.getMessage());
        } finally {
            try {
                socket.close(); // Ensure socket is closed
            } catch (IOException e) { /* Ignore */ }
        }
    }

    private void printBanner() {
        String banner = """
                  _  __      _  __        _      _ _       
                 | |/ /     | |/ _|      | |    (_) |      
                 | ' / __ _ |  _| | ____ | |     _| |_ ___ 
                 |  < / _` || |_| |/ / _`| |    | | __/ _ \\
                 | . \\ (_| ||  _|   < (_|| |____| | ||  __/
                 |_|\\_\\__,_||_| |_|\\_\\__,|______|__\\__\\___|
                
                 >> Kafka-Lite v1.0 | Storage Engine: Persistent Segmented Log
                """;
        System.out.println("\u001B[36m" + banner + "\u001B[0m"); // Cyan
    }
}
