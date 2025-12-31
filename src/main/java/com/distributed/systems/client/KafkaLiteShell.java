package com.distributed.systems.client;

import com.distributed.systems.util.Logger;

import java.util.Scanner;

public class KafkaLiteShell {
    public static void main(String[] args) {
        String host = "localhost";
        int port = 9092;

        System.out.println("\u001B[36m--- Kafka-Lite Interactive Shell ---\u001B[0m");

        try (KafkaLiteClient client = new KafkaLiteClient(host, port)) {
            Scanner scanner = new Scanner(System.in);

            while (true) {
                System.out.print("\n\u001B[32mkl-shell>\u001B[0m ");
                String input = scanner.nextLine().trim();

                if (input.equalsIgnoreCase("exit") || input.equalsIgnoreCase("quit")) {
                    break;
                }

                try {
                    if (input.startsWith("produce ")) {
                        String data = input.substring(8);
                        long offset = client.produce(data);
                        System.out.println("Stored at offset: " + offset);

                    } else if (input.startsWith("consume ")) {
                        long offset = Long.parseLong(input.substring(8));
                        String data = client.consume(offset);
                        System.out.println("Data: " + data);

                    } else if (input.equalsIgnoreCase("stats")) {
                        String report = client.getStats();
                        System.out.println("\u001B[33m[BROKER STATS]\u001B[0m " + report);
                        
                    } else {
                        System.out.println("Unknown command. Use: produce <msg> or consume <offset>");
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