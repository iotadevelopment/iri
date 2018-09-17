package com.iota.iri.service.garbageCollector;

import java.util.ArrayDeque;

@FunctionalInterface
public interface QueueConsolidator {
    public void consolidateQueue(GarbageCollector garbageCollector, ArrayDeque<GarbageCollectorJob> jobQueue) throws GarbageCollectorException;
}
