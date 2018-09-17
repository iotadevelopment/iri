package com.iota.iri.service.garbageCollector;

@FunctionalInterface
public interface JobParser {
    public GarbageCollectorJob parse(String input) throws GarbageCollectorException;
}
