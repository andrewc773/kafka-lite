package com.distributed.systems.network;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

public class BrokerServerTest {

    @TempDir
    Path tempDir;

    private BrokerServer server;
    private final int testPort = 9093; // different port than main app
    private Thread serverThread;

    @BeforeEach
    public void setup() throws IOException {
        server = new BrokerServer(testPort, tempDir.toString());
        // Run server in a background thread so the test isn't blocked
        serverThread = new Thread(() -> server.start());
        serverThread.start();

        // Give the server a moment to bind to the port
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
        }
    }

    @Test
    public void testProduceAndConsumeOverNetwork() throws IOException {
        try (Socket socket = new Socket("localhost", testPort);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            // Read welcome messages
            in.readLine(); // Welcome banner
            in.readLine(); // Commands info

            // Test PRODUCE
            out.println("PRODUCE Network-Message");
            String response = in.readLine();
            assertTrue(response.contains("OFFSET 0"), "Should return offset 0 for first message");

            // Test CONSUME
            out.println("CONSUME 0");
            String dataResponse = in.readLine();
            assertEquals("DATA: Network-Message", dataResponse, "Should retrieve the correct data");
        }
    }

    @Test
    public void testInvalidCommands() throws IOException {
        try (Socket socket = new Socket("localhost", testPort);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            in.readLine();
            in.readLine(); // Skip welcome

            out.println("GARBAGE_COMMAND");
            String response = in.readLine();
            assertTrue(response.contains("ERROR"), "Server should respond with error for unknown commands");
        }
    }
}