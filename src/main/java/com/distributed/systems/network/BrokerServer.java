package com.distributed.systems.network;

import com.distributed.systems.config.BrokerConfig;
import com.distributed.systems.storage.Log;
import com.distributed.systems.storage.LogRecord;
import com.distributed.systems.util.Logger;
import com.distributed.systems.util.MetricsCollector;
import com.distributed.systems.util.Protocol;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BrokerServer {
    private final ExecutorService threadPool;
    private static final int MAX_THREADS = 10; // Only 10 clients at a time

    private final Log log;
    private final int port;

    private final MetricsCollector metrics = new MetricsCollector();


    public BrokerServer(int port, String dataDir) throws IOException {
        this.port = port;
        this.log = new Log(Paths.get(dataDir), new BrokerConfig());
        this.threadPool = Executors.newFixedThreadPool(MAX_THREADS);
    }

    public void start() {
        printBanner();
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            Logger.logNetwork("Broker listening on port " + port);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                threadPool.submit(() -> handleClient(clientSocket));
            }
        } catch (IOException e) {
            Logger.logError("Server failed: " + e.getMessage());
        } finally {
            threadPool.shutdown();
        }
    }

    private void handleProduce(DataInputStream in, DataOutputStream out) throws IOException {
        long startNano = System.nanoTime();

        // Protocol: [KeyLen] [Key] [ValLen] [Value]
        int keyLen = in.readInt();
        byte[] key = new byte[keyLen];
        in.readFully(key);

        int valLen = in.readInt();
        byte[] value = new byte[valLen];
        in.readFully(value);

        // Store in Log
        long offset = log.append(key, value);

        metrics.recordMessage(startNano);

        // Response: [Offset]
        out.writeLong(offset);
    }

    private void handleConsume(DataInputStream in, DataOutputStream out) throws IOException {
        // Protocol: [Offset]
        long offset = in.readLong();

        try {
            LogRecord record = log.read(offset);

            // Response: [Found=True] [Timestamp] [KeyLen] [Key] [ValLen] [Value]
            out.writeBoolean(true); // Status OK
            out.writeLong(record.offset());
            out.writeLong(record.timestamp());

            out.writeInt(record.key().length);
            out.write(record.key());

            out.writeInt(record.value().length);
            out.write(record.value());

            Logger.logNetwork("Served offset " + offset);

        } catch (IOException e) {
            // Response: [Found=False] [ErrorMessage]
            out.writeBoolean(false);
            out.writeUTF(e.getMessage());
        }
    }

    private void handleStats(DataOutputStream out) throws IOException {
        long diskUsage = log.getTotalDiskUsage();
        String report = metrics.getStatsReport(diskUsage);
        out.writeUTF(report);
    }

    private void handleClient(Socket socket) {
        try (
                DataInputStream in = new DataInputStream(socket.getInputStream());
                DataOutputStream out = new DataOutputStream(socket.getOutputStream())
        ) {

            Logger.logNetwork("Client connected: " + socket.getRemoteSocketAddress());

            while (true) {
                String command;
                try {
                    command = in.readUTF();
                } catch (EOFException e) {
                    break; //disconnect gracefully
                }

                if (command.equalsIgnoreCase("PRODUCE")) {
                    handleProduce(in, out);
                } else if (command.equalsIgnoreCase("CONSUME")) {
                    handleConsume(in, out);
                } else if (command.equalsIgnoreCase("STATS")) {
                    handleStats(out);
                } else if (command.equalsIgnoreCase("QUIT")) {
                    break;
                } else {
                    out.writeUTF("ERROR: Unknown Command");
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
