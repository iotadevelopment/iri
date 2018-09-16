package com.iota.iri.service.garbageCollector;

import com.iota.iri.model.Hash;
import com.iota.iri.storage.Indexable;
import com.iota.iri.storage.Persistable;
import com.iota.iri.utils.Pair;

import java.util.ArrayDeque;
import java.util.List;

public class OrphanedSubtanglePrunerJob extends GarbageCollectorJob {
    private Hash transactionHash;

    public static void processQueue(GarbageCollector garbageCollector, ArrayDeque<GarbageCollectorJob> jobQueue) throws GarbageCollectorException {
        while(jobQueue.size() >= 1) {
            jobQueue.getFirst().process();

            jobQueue.removeFirst();

            garbageCollector.persistChanges();
        }
    }

    public static OrphanedSubtanglePrunerJob parse(String input) throws GarbageCollectorException {
        return new OrphanedSubtanglePrunerJob(new Hash(input));
    }

    public OrphanedSubtanglePrunerJob(Hash transactionHash) {
        this.transactionHash = transactionHash;
    }

    @Override
    public List<Pair<Indexable, ? extends Class<? extends Persistable>>> getElementsToDelete() throws Exception {
        return null;
    }

    @Override
    public void process() throws GarbageCollectorException {
        System.out.println("EXECUTING CLEANUP TASK: " + transactionHash);
    }

    @Override
    public String toString() {
        return transactionHash.toString();
    }
}
