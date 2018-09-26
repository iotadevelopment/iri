package com.iota.iri.service.transactionpruning;

import com.iota.iri.model.Hash;
import com.iota.iri.model.Transaction;
import com.iota.iri.storage.Indexable;
import com.iota.iri.storage.Persistable;
import com.iota.iri.utils.Pair;
import com.iota.iri.utils.dag.DAGHelper;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

/**
 * This class represents a job that cleans up all unconfirmed transactions approving a certain transaction.
 *
 * It is used to clean up orphaned subtangles when they become irrelevant for the ledger.
 */
public class UnconfirmedSubtanglePrunerJob extends TransactionPrunerJob {
    /**
     * Holds the hash of the transaction that shall have its unconfirmed approvers cleaned.
     */
    private Hash transactionHash;

    /**
     * This method registers this job type in a {@link TransactionPruner}.
     *
     * It registers the {@link JobParser} and the {@link QueueProcessor} belonging to this job type.
     *
     * @param transactionPruner {@link TransactionPruner} that shall be able to process {@link UnconfirmedSubtanglePrunerJob}s
     */
    static void registerInGarbageCollector(TransactionPruner transactionPruner) {
        transactionPruner.registerParser(UnconfirmedSubtanglePrunerJob.class, UnconfirmedSubtanglePrunerJob::parse);
        transactionPruner.registerQueueProcessor(UnconfirmedSubtanglePrunerJob.class, UnconfirmedSubtanglePrunerJob::processQueue);
    }

    /**
     * This method processes the entire queue of this job type.
     *
     * While it finds jobs, it retrieves the first job, processes it and then removes it from the queue, before
     * persisting the changes to keep track of the progress.
     *
     * @param transactionPruner {@link TransactionPruner} that this job belongs to
     * @param jobQueue queue of {@link MilestonePrunerJob} jobs that shall get processed
     * @throws TransactionPruningException if anything goes wrong while processing the jobs
     */
    private static void processQueue(TransactionPruner transactionPruner, ArrayDeque<TransactionPrunerJob> jobQueue) throws TransactionPruningException {
        while(jobQueue.size() >= 1) {
            jobQueue.getFirst().process();

            jobQueue.removeFirst();

            transactionPruner.persistChanges();
        }
    }

    /**
     * This method parses the serialized representation of a {@link UnconfirmedSubtanglePrunerJob} and creates the
     * corresponding object.
     *
     * It simply creates a hash from the input and passes it on to the {@link #UnconfirmedSubtanglePrunerJob(Hash)}.
     *
     * @param input serialized String representation of a {@link MilestonePrunerJob}
     * @return a new {@link UnconfirmedSubtanglePrunerJob} with the provided hash
     */
    private static UnconfirmedSubtanglePrunerJob parse(String input) throws TransactionPruningException {
        try {
            return new UnconfirmedSubtanglePrunerJob(new Hash(input));
        } catch(Exception e) {
            throw new TransactionPruningException(e);
        }
    }

    /**
     * Constructor of the job receiving the hash of the transaction that shall have its unconfirmed approvers pruned.
     *
     * It simply stores the provided parameters in its according protected properties.
     *
     * @param transactionHash hash of the transaction that shall have its unconfirmed approvers pruned
     */
    public UnconfirmedSubtanglePrunerJob(Hash transactionHash) {
        this.transactionHash = transactionHash;
    }

    /**
     * This method starts the processing of the job which triggers the actual removal of database entries.
     *
     * It iterates through all approvers and collects their hashes if they have not been approved. Then its deletes them
     * from the database (atomic) and also removes them from the runtime caches.
     *
     * @throws TransactionPruningException if anything goes wrong while cleaning up or persisting the changes
     */
    public void process() throws TransactionPruningException {
        try {
            // collect elements to delete
            List<Pair<Indexable, ? extends Class<? extends Persistable>>> elementsToDelete = new ArrayList<>();
            DAGHelper.get(transactionPruner.tangle).traverseApprovers(
                transactionHash,
                approverTransaction -> approverTransaction.snapshotIndex() == 0,
                approverTransaction -> elementsToDelete.add(new Pair<>(approverTransaction.getHash(), Transaction.class))
            );

            // clean database entries
            transactionPruner.tangle.deleteBatch(elementsToDelete);

            // clean runtime caches
            elementsToDelete.forEach(element -> transactionPruner.tipsViewModel.removeTipHash((Hash) element.low));
        } catch (Exception e) {
            throw new TransactionPruningException("failed to cleanup orphaned approvers of transaction " + transactionHash, e);
        }
    }

    /**
     * This method creates the serialized representation of the job, that is used to persist the state of the
     * {@link TransactionPruner}.
     *
     * It simply dumps the string representation of the {@link #transactionHash} .
     *
     * @return serialized representation of this job that can be used to persist its state
     */
    public String serialize() {
        return transactionHash.toString();
    }
}
