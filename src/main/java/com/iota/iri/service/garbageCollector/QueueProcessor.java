package com.iota.iri.service.garbageCollector;

import java.util.ArrayDeque;

@FunctionalInterface
public interface QueueProcessor {
    public void processQueue(GarbageCollector garbageCollector, ArrayDeque<GarbageCollectorJob> jobQueue) throws GarbageCollectorException;
}
