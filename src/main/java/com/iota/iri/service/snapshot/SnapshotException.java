package com.iota.iri.service.snapshot;

public class SnapshotException extends Exception {
    public SnapshotException(String message) {
        super(message);
    }

    public SnapshotException(String message, Throwable cause) {
        super(message, cause);
    }
}
