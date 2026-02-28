package com.distributed.systems.util;

public class Logger {
    // ANSI Color Codes
    private static final String RESET = "\u001B[0m";
    private static final String GREEN = "\u001B[32m";  // Network
    private static final String YELLOW = "\u001B[33m"; // Janitor
    private static final String BLUE = "\u001B[34m";   // Storage/Segments
    private static final String PURPLE = "\u001B[35m"; // Bootstrap/Config
    private static final String CYAN = "\u001B[36m";   // Startup
    private static final String RED = "\u001B[31m";    // Error
    private static final String ORANGE = "\u001B[38;5;208m";
    public static final String BOLD_YELLOW = "\033[1;33m";

    public static void logBootstrap(String message) {
        System.out.println(PURPLE + "[BOOTSTRAP] " + RESET + message);
    }

    public static void logStorage(String message) {
        System.out.println(BLUE + "[STORAGE] " + RESET + message);
    }

    public static void logJanitor(String message) {
        System.out.println(YELLOW + "[JANITOR] " + RESET + message);
    }

    public static void logNetwork(String message) {
        System.out.println(GREEN + "[NETWORK] " + RESET + message);
    }

    public static void logError(String message) {
        System.err.println(RED + "[ERROR] " + RESET + message);
    }

    public static void logWarning(String message) {
        System.out.println(BOLD_YELLOW + "[WARNING] " + RESET + message);
    }

    public static void logInfo(String message) {
        System.out.println(CYAN + "[INFO] " + RESET + message);
    }

    public static void logDebug(String message) {
        System.out.println(ORANGE + "[DEBUG] " + RESET + message);
    }
}