package com.distributed.systems.client;

import com.distributed.systems.util.Logger;

import java.util.Scanner;

public class KafkaLiteShell {
    public static void main(String[] args) {
        String host = "localhost";
        int port = 9092;

        System.out.println("\u001B[36m--- Kafka-Lite Interactive Shell ---\u001B[0m");
        System.out.println("Usage:");
        System.out.println("  produce <key> <value>");
        System.out.println("  consume <offset>");
        System.out.println("  stats");

        try (KafkaLiteClient client = new KafkaLiteClient(host, port)) {
            Scanner scanner = new Scanner(System.in);

            while (true) {
                System.out.print("\n\u001B[32mkl-shell>\u001B[0m ");
                if (!scanner.hasNextLine()) break;
                String input = scanner.nextLine().trim();

                if (input.equalsIgnoreCase("exit") || input.equalsIgnoreCase("quit")) {
                    break;
                }

                if (input.isEmpty()) continue;

                try {
                    String[] parts = input.split("\\s+", 3);
                    String command = parts[0].toLowerCase();

                    switch (command) {
                        case "produce":
                            if (parts.length < 3) {
                                System.out.println("Usage: produce <key> <value>");
                                continue;
                            }
                            String key = parts[1];
                            String value = parts[2];
                            long offset = client.produce(key, value);
                            System.out.println("\u001B[32m✔\u001B[0m Stored at offset: " + offset);
                            break;

                        case "consume":
                            if (parts.length < 2) {
                                System.out.println("Usage: consume <offset>");
                                continue;
                            }
                            long consumeOffset = Long.parseLong(parts[1]);
                            // Client.consume now handles its own printing to match the binary record
                            client.consume(consumeOffset);
                            break;

                        case "stats":
                            String report = client.getStats();
                            System.out.println("\u001B[33m[BROKER STATS]\u001B[0m " + report);
                            break;

                        default:
                            System.out.println("Unknown command. Use: produce <key> <value>, consume <offset>, or stats");
                    }
                } catch (Exception e) {
                    System.err.println("Error: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            Logger.logError("Could not connect to broker: " + e.getMessage());
        }
    }
}