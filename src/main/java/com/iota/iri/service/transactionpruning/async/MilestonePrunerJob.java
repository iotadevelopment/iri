package com.iota.iri.service.transactionpruning.async;

import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.model.IntegerIndex;
import com.iota.iri.model.Milestone;
import com.iota.iri.model.Transaction;
import com.iota.iri.service.snapshot.Snapshot;
import com.iota.iri.service.transactionpruning.*;
import com.iota.iri.storage.Indexable;
import com.iota.iri.storage.Persistable;
import com.iota.iri.utils.Pair;
import com.iota.iri.utils.dag.DAGHelper;

import java.util.*;

/**
 * This class represents a cleanup job for the {@link TransactionPruner}.
 *
 * It removes milestones and all of their directly and indirectly referenced transactions ( and the orphaned subtangles
 * branching off of the deleted transactions). Therefore it is used by the
 * {@link com.iota.iri.service.snapshot.SnapshotManager} to clean up milestones prior to a snapshot.
 *
 * It gets processed one milestone at a time persisting the progress after each step.
 */
public class MilestonePrunerJob extends AsyncTransactionPrunerJob {
    /**
     * Holds the milestone index where this job starts cleaning up (read only).
     */
    private int startingIndex;

    /**
     * Holds the milestone index where this job stops cleaning up (read only).
     */
    private int targetIndex;

    /**
     * Holds the milestone index of the oldest milestone that was cleaned up already (the current progress).
     */
    private int currentIndex;

    /**
     * This method parses the string representation of a {@link MilestonePrunerJob} and creates the corresponding object.
     *
     * It splits the String input in two parts (delimited by a ";") and passes them into the constructor of a new
     * {@link MilestonePrunerJob} by interpreting them as {@link #startingIndex} and {@link #currentIndex} of the job.
     *
     * The {@link #targetIndex} is derived by the jobs position in the queue and the fact that milestone deletions
     * happen sequentially from the newest to the oldest milestone.
     *
     * @param input serialized String representation of a {@link MilestonePrunerJob}
     * @return a new {@link MilestonePrunerJob} with the provided details
     * @throws TransactionPruningException if anything goes wrong while parsing the input
     */
    protected static MilestonePrunerJob parse(String input) throws TransactionPruningException {
        String[] parts = input.split(";", 2);
        if(parts.length >= 2) {
            return new MilestonePrunerJob(Integer.valueOf(parts[0]), Integer.valueOf(parts[1]));
        }

        throw new TransactionPruningException("failed to parse TransactionPruner file - invalid input: " + input);
    }

    /**
     * Does same as {@link #MilestonePrunerJob(int, int)} but defaults to the {@link #currentIndex} being the same as
     * the {@link #startingIndex}. This is usually the case when we create a new job programmatically that does not get
     * restored from a state file.
     *
     * @param startingIndex milestone index that defines where to start cleaning up
     */
    public MilestonePrunerJob(int startingIndex) {
        this(startingIndex, startingIndex);
    }

    /**
     * Constructor of the job receiving both values that are relevant for the job.
     *
     * It simply stores the provided parameters in its according protected properties.
     *
     * Since cleanup jobs can be consolidated (to reduce the size of the {@link TransactionPruner} state file), we need
     * to be able provide both parameters even tho the job usually always "starts" with its {@code currentIndex} being
     * equal to the {@code startingIndex}.
     *
     * @param startingIndex milestone index that defines where to start cleaning up
     * @param currentIndex milestone index that defines the next milestone that should be cleaned up by this job
     */
    protected MilestonePrunerJob(int startingIndex, int currentIndex) {
        this.startingIndex = startingIndex;
        this.currentIndex = currentIndex;
    }

    public int getStartingIndex() {
        return startingIndex;
    }

    public MilestonePrunerJob setStartingIndex(int startingIndex) {
        this.startingIndex = startingIndex;

        return this;
    }

    public int getCurrentIndex() {
        return currentIndex;
    }

    public int getTargetIndex() {
        return targetIndex;
    }

    public void setTargetIndex(int targetIndex) {
        this.targetIndex = targetIndex;
    }

    /**
     * This method starts the processing of the job which triggers the actual removal of database entries.
     *
     * It iterates from the {@link #currentIndex} to the provided {@link #targetIndex} and processes every milestone
     * one by one. After each step is finished we persist the progress to be able to continue with the current progress
     * upon IRI restarts.
     *
     * @throws TransactionPruningException if anything goes wrong while cleaning up or persisting the changes
     */
    public void process() throws TransactionPruningException {
        while(!Thread.interrupted() && targetIndex < currentIndex) {
            cleanupMilestoneTransactions();

            currentIndex--;

            getTransactionPruner().saveState();
        }
    }

    /**
     * This method takes care of cleaning up a single milestone and all of its transactions and performs the actual
     * database operations.
     *
     * This method performs the deletions in an atomic way, which means that either the full processing succeeds or
     * fails. It does that by iterating through all the transactions that belong to the current milestone and first
     * collecting them in a List of items to delete. Once all transactions where found we issue a batchDelete.
     *
     * While processing the transactions that are directly or indirectly referenced by the chosen milestone, we also
     * issue additional {@link UnconfirmedSubtanglePrunerJob}s that remove the orphaned parts of the tangle that branch off
     * the deleted transactions because they would otherwise loose their connection to the rest of the tangle unless
     * they are branching off a solid entry point (in which case we wait with the deletion until the solid entry point
     * becomes irrelevant).
     *
     * After removing the entries from the database it also removes the entries from the relevant runtime caches.
     *
     * @throws TransactionPruningException if something goes wrong while cleaning up the milestone
     */
    private void cleanupMilestoneTransactions() throws TransactionPruningException {
        try {
            // collect elements to delete
            List<Pair<Indexable, ? extends Class<? extends Persistable>>> elementsToDelete = new ArrayList<>();
            MilestoneViewModel milestoneViewModel = MilestoneViewModel.get(getTangle(), currentIndex);
            if (milestoneViewModel != null) {
                elementsToDelete.add(new Pair<>(milestoneViewModel.getHash(), Transaction.class));
                elementsToDelete.add(new Pair<>(new IntegerIndex(milestoneViewModel.index()), Milestone.class));
                if (!getSnapshot().hasSolidEntryPoint(milestoneViewModel.getHash())) {
                    getTransactionPruner().addJob(new UnconfirmedSubtanglePrunerJob(milestoneViewModel.getHash()));
                }
                DAGHelper.get(getTangle()).traverseApprovees(
                milestoneViewModel.getHash(),
                approvedTransaction -> approvedTransaction.snapshotIndex() >= milestoneViewModel.index(),
                approvedTransaction -> {
                    elementsToDelete.add(new Pair<>(approvedTransaction.getHash(), Transaction.class));

                    if (!getSnapshot().hasSolidEntryPoint(approvedTransaction.getHash())) {
                        try {
                            getTransactionPruner().addJob(new UnconfirmedSubtanglePrunerJob(approvedTransaction.getHash()));
                        } catch(TransactionPruningException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                );
            }

            // clean database entries
            getTangle().deleteBatch(elementsToDelete);

            // clean runtime caches
            elementsToDelete.forEach(element -> {
                if(Transaction.class.equals(element.hi)) {
                    getTipsViewModel().removeTipHash((Hash) element.low);
                } else if(Milestone.class.equals(element.hi)) {
                    MilestoneViewModel.clear(((IntegerIndex) element.low).getValue());
                }
            });
        } catch(Exception e) {
            throw new TransactionPruningException("failed to cleanup milestone #" + currentIndex, e);
        }
    }

    /**
     * This method creates the serialized representation of the job, that is used to persist the state of the
     * {@link TransactionPruner}.
     *
     * It simply concatenates the {@link #startingIndex} and the {@link #currentIndex} as they are necessary to fully
     * describe the job.
     *
     * @return serialized representation of this job that can be used to persist its state
     */
    public String serialize() {
        return startingIndex + ";" + currentIndex;
    }
}
