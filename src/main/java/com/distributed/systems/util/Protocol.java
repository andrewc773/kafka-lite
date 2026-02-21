package com.distributed.systems.util;

public class Protocol {
    // Commands
    public static final String CMD_PRODUCE = "PRODUCE";
    public static final String CMD_CONSUME = "CONSUME";
    public static final String CMD_QUIT = "QUIT";
    public static final String CMD_OFFSET_COMMIT = "OFFSET_COMMIT";
    public static final String CMD_OFFSET_FETCH = "OFFSET_FETCH";
    public static final String CMD_REPLICA_FETCH = "REPLICA_FETCH";

    // Responses
    public static final String RESP_SUCCESS_PREFIX = "SUCCESS: Message stored at OFFSET ";
    public static final String RESP_DATA_PREFIX = "DATA: ";
    public static final String RESP_ERROR_PREFIX = "ERROR: ";

    // Welcome Messages
    public static final String WELCOME_HEADER = "--- Welcome to Kafka-Lite Broker ---";
    public static final String WELCOME_HELP = "Commands: PRODUCE <data> | CONSUME <offset> | QUIT";

    // Statistics
    public static final String CMD_STATS = "STATS";
    public static final String RESP_STATS_PREFIX = "STATS_REPORT: ";

    // Helper for server-side formatting
    public static String formatSuccess(long offset) {
        return RESP_SUCCESS_PREFIX + offset;
    }
}