package com.iota.iri.service.snapshot;

import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.model.IntegerIndex;
import com.iota.iri.model.Milestone;
import com.iota.iri.model.Transaction;
import com.iota.iri.storage.Indexable;
import com.iota.iri.storage.Persistable;
import com.iota.iri.utils.Pair;
import com.iota.iri.utils.dag.TraversalException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class represents a cleanup job for the local snapshots {@link GarbageCollector}.
 *
 * Since the {@code GarbageCollector} always cleans up the current job from the {@code startingIndex} to the
 * {@code startingIndex} of the previous job (or the milestone start index of the last global snapshot if no previous
 * job is found), we only need two values to fully describe a job and its progress ({@code startingIndex} and
 * {@code currentIndex}).
 */
public class GarbageCollectorJob {
    /**
     * Holds a reference to the {@link GarbageCollector} that this job belongs to.
     */
    protected GarbageCollector garbageCollector;

    /**
     * Holds the milestone index where this job starts cleaning up (read only).
     */
    protected int startingIndex;

    /**
     * Holds the milestone index of the oldest milestone that was cleaned up already (the current progress).
     */
    protected int currentIndex;

    /**
     * Constructor of the job receiving both values that are relevant for the job.
     *
     * It simply stores the provided parameters in its according protected properties.
     *
     * Since cleanup jobs can be consolidated (to reduce the size of the garbage collector balances file), we need to be
     * able provide both parameters even tho the job usually always "starts" with its {@code currentIndex} being equal
     * to the {@code startingIndex}.
     *
     * @param garbageCollector GarbageCollector that this job belongs to
     * @param startingIndex milestone index that defines where to start cleaning up
     * @param currentIndex milestone index that defines until where we have cleaned up already
     */
    public GarbageCollectorJob(GarbageCollector garbageCollector, int startingIndex, int currentIndex) {
        this.garbageCollector = garbageCollector;
        this.startingIndex = startingIndex;
        this.currentIndex = currentIndex;
    }

    /**
     * Getter of the {@link #startingIndex} which reflects the starting point of this job.
     *
     * It simply returns the stored protected property.
     *
     * @return milestone index that defines where to start cleaning up
     */
    public int getStartingIndex() {
        return startingIndex;
    }

    /**
     * Getter of the {@link #currentIndex} which reflects the progress of the job.
     *
     * It simply returns the stored protected property.
     *
     * @return milestone index that defines until where we have cleaned up already
     */
    public int getCurrentIndex() {
        return currentIndex;
    }

    /**
     * Setter of the {@link #currentIndex} which reflects the progress of the job.
     *
     * It simply sets the corresponding protected property.
     */
    public void setCurrentIndex(int currentIndex) {
        this.currentIndex = currentIndex;
    }

    /**
     * This method starts the processing of the job which triggers the actual removal of database entries.
     *
     * It iterates from the {@link #currentIndex} to the provided {@code cleanupTarget} and processes every milestone
     * one by one. After each step is finished we persist the progress to be able to continue with the current progress
     * upon IRI restarts.
     *
     * @param cleanupTarget milestone index that is considered the target for this job
     * @throws SnapshotException if anything goes wrong while cleaning up or persisting the changes
     */
    public void process(int cleanupTarget) throws SnapshotException {
        while(!garbageCollector.shuttingDown && cleanupTarget < getCurrentIndex()) {
            cleanupMilestoneTransactions(getCurrentIndex());

            setCurrentIndex(getCurrentIndex() - 1);

            garbageCollector.persistChanges();
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
     * After removing the entries from the database it also removes the entries from the relevant runtime caches.
     *
     * @param milestoneIndex the index of the milestone that shall be deleted
     * @throws SnapshotException if something goes wrong while cleaning up the milestone
     */
    protected void cleanupMilestoneTransactions(int milestoneIndex) throws SnapshotException {
        try {
            MilestoneViewModel milestoneViewModel = MilestoneViewModel.get(garbageCollector.tangle, milestoneIndex);
            if(milestoneViewModel != null) {
                List<Pair<Indexable, ? extends Class<? extends Persistable>>> elementsToDelete = new ArrayList<>();

                // collect elements to delete
                elementsToDelete.add(new Pair<>(new IntegerIndex(milestoneViewModel.index()), Milestone.class));
                elementsToDelete.add(new Pair<>(milestoneViewModel.getHash(), Transaction.class));
                garbageCollector.dagUtils.traverseApprovees(
                    milestoneViewModel,
                    approvedTransaction -> approvedTransaction.snapshotIndex() >= milestoneViewModel.index(),
                    approvedTransaction -> {
                        elementsToDelete.add(new Pair<>(approvedTransaction.getHash(), Transaction.class));

                        cleanupOrphanedApprovers(approvedTransaction, elementsToDelete, new HashSet<>());
                    }
                );

                // clean database entries
                garbageCollector.tangle.deleteBatch(elementsToDelete);

                // clean runtime caches
                MilestoneViewModel.clear(milestoneIndex);
                elementsToDelete.stream().forEach(element -> {
                    if(Transaction.class.equals(element.hi)) {
                        garbageCollector.tipsViewModel.removeTipHash((Hash) element.low);
                    }
                });
                garbageCollector.tipsViewModel.removeTipHash(milestoneViewModel.getHash());
            }
        } catch(Exception e) {
            throw new SnapshotException("failed to cleanup milestone #" + milestoneIndex, e);
        }
    }

    /**
     * This method removes all orphaned approvers of a transaction.
     *
     * Since orphaned approvers are only reachable from the transaction they approve (bottom -> up), we need to clean
     * them up as well when removing the transactions belonging to a milestone. Transactions are considered to be
     * orphaned if they have not been verified by a milestone, yet. While this definition is theoretically not
     * completely "safe" since a subtangle could stay unconfirmed for a very long time and then still get confirmed (and
     * therefore not "really" being orphaned), it practically doesn't cause any problems since it will be handled by the
     * solid entry points and can consequently be solidified again if it ever becomes necessary.
     *
     * If the LOCAL_SNAPSHOT_DEPTH is sufficiently high this becomes practically impossible at some point anyway.
     *
     * @param transaction the transaction that shall have its orphaned approvers removed
     * @param elementsToDelete List of elements that is used to gather the elements we want to delete
     * @param processedTransactions List of transactions that were processed already (so we don't process the same
     *                              transactions more than once)
     * @throws TraversalException if anything goes wrong while traversing the graph
     */
    protected void cleanupOrphanedApprovers(TransactionViewModel transaction, List<Pair<Indexable, ? extends Class<? extends Persistable>>> elementsToDelete, Set<Hash> processedTransactions) throws TraversalException {
        // remove all orphaned transactions that are branching off of our deleted transactions
        garbageCollector.dagUtils.traverseApprovers(
            transaction,
            approverTransaction -> approverTransaction.snapshotIndex() == 0,
            approverTransaction -> {
                elementsToDelete.add(new Pair<>(approverTransaction.getHash(), Transaction.class));
            },
            processedTransactions
        );
    }
}
