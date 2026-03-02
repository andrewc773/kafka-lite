package com.distributed.systems.client;

import com.distributed.systems.util.Logger;

import java.util.Scanner;

public class ClientShell {
    public static void main(String[] args) {
        Logger.logBanner();

        String host = "localhost";
        int currentPort = 9001;
        if (args.length >= 1) {
            host = args[0];
        }
        if (args.length >= 2) {
            try {
                currentPort = Integer.parseInt(args[1]);
            } catch (NumberFormatException e) {
                Logger.logError("Invalid port. Usage: ClientShell [host] [port]");
                return;
            }
        }

        Scanner scanner = new Scanner(System.in);

        try (KafkaLiteClient client = new KafkaLiteClient(host, currentPort, "demo-group")) {
            Logger.logBootstrap("Shell session started. Connected to: " + host + ":" + currentPort);
            System.out.println("Commands:");
            System.out.println("  produce <topic> <key> <value>");
            System.out.println("    -> expects a numeric offset on success");
            System.out.println("  consume <topic> <offset>");
            System.out.println("    -> prints record payload if found");
            System.out.println("  stats");
            System.out.println("  help");
            System.out.println("  quit");

            while (true) {
                System.out.print("\n\u001B[32mkafka-lite>\u001B[0m ");
                String input = scanner.nextLine().trim();

                if (input.equalsIgnoreCase("exit") || input.equalsIgnoreCase("quit")) break;
                if (input.isEmpty()) continue;

                String[] parts = input.split("\\s+");
                String cmd = parts[0].toLowerCase();

                try {
                    switch (cmd) {
                        case "produce":
                            // Usage: produce <topic> <key> <value>
                            if (parts.length < 4) {
                                Logger.logError("Usage: produce <topic> <key> <value>");
                                break;
                            }
                            long offset = client.produce(parts[1], parts[2], parts[3]);
                            if (offset >= 0) {
                                Logger.logNetwork("Record stored at offset: " + offset);
                            } else {
                                Logger.logWarning("Produce rejected (not leader). Offset: " + offset);
                            }
                            break;

                        case "consume":
                            // Usage: consume <topic> <offset>
                            if (parts.length < 3) {
                                Logger.logError("Usage: consume <topic> <offset>");
                                break;
                            }
                            long consumeOffset = Long.parseLong(parts[2]);
                            Logger.logInfo("Fetching [" + parts[1] + "] at offset " + consumeOffset + "...");
                            client.consume(parts[1], consumeOffset);
                            break;

                        case "connect":
                            Logger.logWarning("Connect is not supported yet. Restart shell with host/port args.");
                            break;

                        case "stats":
                            Logger.logInfo("Broker stats: " + client.getStats());
                            break;

                        case "help":
                            System.out.println("Commands:");
                            System.out.println("  produce <topic> <key> <value>");
                            System.out.println("  consume <topic> <offset>");
                            System.out.println("  stats");
                            System.out.println("  quit");
                            break;

                        default:
                            Logger.logError("Unknown command. Try: produce, consume, exit");
                    }
                } catch (Exception e) {
                    Logger.logError("Execution error: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            Logger.logError("Failed to initialize shell: " + e.getMessage());
        }
    }
}
