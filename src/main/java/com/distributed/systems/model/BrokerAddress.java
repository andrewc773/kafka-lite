package com.distributed.systems.model;

public record BrokerAddress(String host, int port) {
    @Override
    public String toString() {
        return host + ":" + port;
    }
}