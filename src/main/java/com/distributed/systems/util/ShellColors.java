package com.distributed.systems.util;

public class ShellColors {
    public static final String RESET = "\u001B[0m";
    public static final String RED = "\u001B[31m";
    public static final String GREEN = "\u001B[32m";
    public static final String YELLOW = "\u001B[33m";
    public static final String BLUE = "\u001B[34m";
    public static final String CYAN = "\u001B[36m";
    public static final String BOLD = "\u001B[1m";

    public static void printBanner() {
        System.out.println(CYAN + BOLD + "========================================");
        System.out.println("   _  __      ___ _             _      ");
        System.out.println("  | |/ /__ _ / _| |__ __ _     | |__   ");
        System.out.println("  | ' </ _` |  _| / / _` |_____| / /   ");
        System.out.println("  |_|\\_\\__,_|_| |_|\\_\\__,_|_____|_|\\_\\ ");
        System.out.println("      KAFKA-LITE DISTRIBUTED PLATFORM   ");
        System.out.println("========================================" + RESET);
    }
}