package com.distributed.systems.util;

/**
 * Thrown when a request fails due to a logical error (e.g., Topic Not Found)
 * that should not be retried.
 */
public class FatalClientException extends RuntimeException {
    public FatalClientException(String message) {
        super(message);
    }
}