package com.distributed.systems.storage;

/**
 * A simple data carrier for index lookups.
 */
public record IndexEntry(long logicalOffset, long physicalPosition) {
}