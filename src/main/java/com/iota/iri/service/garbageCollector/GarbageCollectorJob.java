package com.iota.iri.service.garbageCollector;

import com.iota.iri.storage.Indexable;
import com.iota.iri.storage.Persistable;
import com.iota.iri.utils.Pair;

import java.util.ArrayDeque;
import java.util.List;

public abstract class GarbageCollectorJob {
    public static void consolidateQueue(GarbageCollector garbageCollector, ArrayDeque<GarbageCollectorJob> jobQueue) throws GarbageCollectorException {
        /* can be implemented (optional) */
    }

    public static void processQueue(GarbageCollector garbageCollector, ArrayDeque<GarbageCollectorJob> jobQueue) throws GarbageCollectorException {
        throw new GarbageCollectorException("\"processQueue\" has to be implemented by the child GarbageCollectorJob class");
    }

    public static GarbageCollectorJob parse(String input) throws GarbageCollectorException {
        throw new GarbageCollectorException("\"parse\" has to be implemented by the child GarbageCollectorJob class");
    }

    /**
     * Holds a reference to the {@link GarbageCollector} that this job belongs to.
     */
    protected GarbageCollector garbageCollector;

    public abstract List<Pair<Indexable, ? extends Class<? extends Persistable>>> getElementsToDelete() throws Exception;

    public abstract void process() throws GarbageCollectorException;

    @Override
    public abstract String toString();

    public void registerGarbageCollector(GarbageCollector garbageCollector) {
        this.garbageCollector = garbageCollector;
    }
}
