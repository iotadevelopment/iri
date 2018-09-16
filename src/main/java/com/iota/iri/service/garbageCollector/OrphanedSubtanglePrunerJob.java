package com.iota.iri.service.garbageCollector;

import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.model.IntegerIndex;
import com.iota.iri.model.Milestone;
import com.iota.iri.model.Transaction;
import com.iota.iri.storage.Indexable;
import com.iota.iri.storage.Persistable;
import com.iota.iri.utils.Pair;
import com.iota.iri.utils.dag.DAGHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

public class OrphanedSubtanglePrunerJob extends GarbageCollectorJob {
    /**
     * Logger for this class allowing us to dump debug and status messages.
     */
    protected static final Logger log = LoggerFactory.getLogger(OrphanedSubtanglePrunerJob.class);

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
        List<Pair<Indexable, ? extends Class<? extends Persistable>>> elementsToDelete = new ArrayList<>();

        try {
            // remove all orphaned transactions that are branching off of our deleted transactions
            DAGHelper.get(garbageCollector.tangle).traverseApprovers(
                transactionHash,
                approverTransaction -> approverTransaction.snapshotIndex() == 0,
                approverTransaction -> {
                    System.out.println("DELETING OPRHANED " + approverTransaction);
                    elementsToDelete.add(new Pair<>(approverTransaction.getHash(), Transaction.class));
                }
            );
        } catch(Exception e) {
            log.error("failed to clean up the orphaned approvers of transaction " + transactionHash, e);
        }

        return elementsToDelete;
    }

    @Override
    public void process() throws GarbageCollectorException {
        try {
            List<Pair<Indexable, ? extends Class<? extends Persistable>>> elementsToDelete = getElementsToDelete();

            // clean database entries
            garbageCollector.tangle.deleteBatch(elementsToDelete);

            // clean runtime caches
            elementsToDelete.stream().forEach(element -> {
                if(Transaction.class.equals(element.hi)) {
                    garbageCollector.tipsViewModel.removeTipHash((Hash) element.low);
                } else if(Milestone.class.equals(element.hi)) {
                    MilestoneViewModel.clear(((IntegerIndex) element.low).getValue());
                }
            });
        } catch (Exception e) {
            throw new GarbageCollectorException("failed to cleanup orphaned approvers of transaction " + transactionHash, e);
        }
    }

    @Override
    public String toString() {
        return transactionHash.toString();
    }
}
