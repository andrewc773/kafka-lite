package com.distributed.systems.client;

import com.distributed.systems.util.Protocol;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class KafkaLiteClientTest {
    private static ServerSocket mockServer;
    private static int port;
    private static Thread serverThread;

    @BeforeAll
    static void startMockServer() throws IOException {
        mockServer = new ServerSocket(0); // 0 finds a random free port
        port = mockServer.getLocalPort();


        serverThread = new Thread(() -> {
            try {

                while (!mockServer.isClosed()) {
                    Socket client = mockServer.accept();
                    PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                    BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));

                    out.println(Protocol.WELCOME_HEADER);
                    out.println(Protocol.WELCOME_HELP);

                    // simple Mock Logic
                    String line = in.readLine();
                    if (line != null && line.startsWith(Protocol.CMD_PRODUCE)) {
                        out.println(Protocol.formatSuccess(999));
                    } else if (line != null && line.startsWith(Protocol.CMD_CONSUME)) {
                        out.println(Protocol.RESP_DATA_PREFIX + "MockData");
                    }
                    client.close();
                }
            } catch (IOException ignored) {
            }
        });
        serverThread.start();
    }

    @AfterAll
    static void stopMockServer() throws IOException {
        mockServer.close();
    }

    @Test
    void testProduceCommand() throws IOException {
        try (KafkaLiteClient client = new KafkaLiteClient("localhost", port)) {
            long offset = client.produce("Hello Mock");
            assertEquals(999, offset, "Client should parse the offset correctly from mock response.");
        }
    }

    @Test
    void testConsumeCommand() throws IOException {
        try (KafkaLiteClient client = new KafkaLiteClient("localhost", port)) {
            String data = client.consume(0);
            assertEquals("MockData", data, "Client should strip the DATA prefix correctly.");
        }
    }
}
