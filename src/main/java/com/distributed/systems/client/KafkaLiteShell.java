package com.distributed.systems.client;

import com.distributed.systems.util.Logger;

import java.util.Scanner;

public class KafkaLiteShell {
    public static void main(String[] args) {
        String host = "localhost";
        int port = 9092;
        if (args.length >= 1) {
            host = args[0];
        }
        if (args.length >= 2) {
            try {
                port = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port. Usage: KafkaLiteShell [host] [port]");
                return;
            }
        }

        System.out.println("\u001B[36m--- Kafka-Lite Interactive Shell ---\u001B[0m");
        System.out.println("Connected to: " + host + ":" + port);
        System.out.println("Commands:");
        System.out.println("  produce <topic> <key> <value>");
        System.out.println("    -> expects a numeric offset on success");
        System.out.println("  consume <topic> <offset>");
        System.out.println("    -> prints record payload if found");
        System.out.println("  stats");
        System.out.println("  quit");

        try (KafkaLiteClient client = new KafkaLiteClient(host, port, "my-group-id")) {
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
                    // Split into max 4 parts for 'produce topic key value'
                    String[] parts = input.split("\\s+", 4);
                    String command = parts[0].toLowerCase();

                    switch (command) {
                        case "produce":
                            if (parts.length < 4) {
                                System.out.println("Usage: produce <topic> <key> <value>");
                                continue;
                            }
                            String prodTopic = parts[1];
                            String key = parts[2];
                            String value = parts[3];
                            long offset = client.produce(prodTopic, key, value);
                            if (offset >= 0) {
                                System.out.println("\u001B[32m✔\u001B[0m Stored in [" + prodTopic + "] at offset: " + offset);
                            } else {
                                System.out.println("\u001B[31m✘\u001B[0m Produce rejected (not leader). Offset: " + offset);
                            }
                            break;

                        case "consume":
                            if (parts.length < 3) {
                                System.out.println("Usage: consume <topic> <offset>");
                                continue;
                            }
                            String consTopic = parts[1];
                            long consumeOffset = Long.parseLong(parts[2]);
                            System.out.println("Fetching [" + consTopic + "] at offset " + consumeOffset + "...");
                            client.consume(consTopic, consumeOffset);
                            break;

                        case "stats":
                            String report = client.getStats();
                            System.out.println("\u001B[33m[BROKER STATS]\u001B[0m " + report);
                            break;

                        default:
                            System.out.println("Unknown command. Use: produce, consume, or stats");
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
