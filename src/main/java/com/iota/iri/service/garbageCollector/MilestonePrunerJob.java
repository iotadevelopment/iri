package com.iota.iri.service.garbageCollector;

import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.model.IntegerIndex;
import com.iota.iri.model.Milestone;
import com.iota.iri.model.Transaction;
import com.iota.iri.service.snapshot.SnapshotException;
import com.iota.iri.storage.Indexable;
import com.iota.iri.storage.Persistable;
import com.iota.iri.utils.Pair;
import com.iota.iri.utils.dag.DAGHelper;
import com.iota.iri.utils.dag.TraversalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * This class represents a cleanup job for the local snapshots {@link GarbageCollector}.
 *
 * Since the {@code GarbageCollector} always cleans up the current job from the {@code startingIndex} to the
 * {@code startingIndex} of the previous job (or the milestone start index of the last global snapshot if no previous
 * job is found), we only need two values to fully describe a job and its progress ({@code startingIndex} and
 * {@code currentIndex}).
 */
public class MilestonePrunerJob extends GarbageCollectorJob {
    /**
     * Logger for this class allowing us to dump debug and status messages.
     */
    protected static final Logger log = LoggerFactory.getLogger(MilestonePrunerJob.class);

    /**
     * Holds the milestone index where this job starts cleaning up (read only).
     */
    protected int startingIndex;

    /**
     * Holds the milestone index where this job stops cleaning up (read only).
     */
    protected int targetIndex;

    /**
     * Holds the milestone index of the oldest milestone that was cleaned up already (the current progress).
     */
    protected int currentIndex;

    /**
     * DAGHelper instance that is used to traverse the graph.
     */
    protected DAGHelper dagHelper;

    /**
     * This method consolidates the cleanup jobs by merging two or more jobs together if they are either both done or
     * pending.
     *
     * It is used to clean up the milestoneCleanupJobs queue and the corresponding garbage collector file, so it always has a
     * size of less than 4 jobs.
     *
     * Since the jobs are getting processed from the beginning of the list, we first check if the first two jobs are
     * "done" and merge them into a single one that reflects the "done" status of both jobs. Consecutively we check if
     * there are two or more pending jobs at the end that can also be consolidated.
     *
     * It is important to note that the jobs always clean from their startingPosition to the startingPosition of the
     * previous job (or the {@code milestoneStartIndex} of the last global snapshot if there is no previous one) without
     * any gaps in between which is required to be able to merge them.
     *
     * @throws GarbageCollectorException if an error occurs while persisting the state
     */
    public static void consolidateQueue(GarbageCollector garbageCollector, ArrayDeque<GarbageCollectorJob> jobQueue) throws GarbageCollectorException {
        // if we have at least 2 jobs -> check if we can consolidate them at the beginning
        if(jobQueue.size() >= 2) {
            // retrieve the first two jobs
            MilestonePrunerJob job1 = (MilestonePrunerJob) jobQueue.removeFirst();
            MilestonePrunerJob job2 = (MilestonePrunerJob) jobQueue.removeFirst();

            // if both first job are done -> consolidate them and persists the changes
            if(job1.getCurrentIndex() == garbageCollector.snapshotManager.getConfiguration().getMilestoneStartIndex() && job2.getCurrentIndex() == job1.getStartingIndex()) {
                MilestonePrunerJob consolidatedJob = new MilestonePrunerJob(job2.getStartingIndex(), job1.getCurrentIndex());
                consolidatedJob.registerGarbageCollector(garbageCollector);
                jobQueue.addFirst(consolidatedJob);

                garbageCollector.persistChanges();
            }

            // otherwise just add them back to the queue
            else {
                jobQueue.addFirst(job2);
                jobQueue.addFirst(job1);
            }
        }

        // if we have at least 2 jobs -> check if we can consolidate them at the end
        boolean cleanupSuccessfull = true;
        while(jobQueue.size() >= 2 && cleanupSuccessfull) {
            // retrieve the last two jobs
            MilestonePrunerJob job1 = (MilestonePrunerJob) jobQueue.removeLast();
            MilestonePrunerJob job2 = (MilestonePrunerJob) jobQueue.removeLast();

            // if both jobs are pending -> consolidate them and persists the changes
            if(job1.getCurrentIndex() == job1.getStartingIndex() && job2.getCurrentIndex() == job2.getStartingIndex()) {
                MilestonePrunerJob consolidatedJob = new MilestonePrunerJob(job1.getStartingIndex(), job1.getCurrentIndex());
                consolidatedJob.registerGarbageCollector(garbageCollector);
                jobQueue.addLast(consolidatedJob);

                garbageCollector.persistChanges();
            }

            // otherwise just add them back to the queue
            else {
                jobQueue.addLast(job2);
                jobQueue.addLast(job1);

                cleanupSuccessfull = false;
            }
        }

        int previousStartIndex = garbageCollector.snapshotManager.getConfiguration().getMilestoneStartIndex();
        for(GarbageCollectorJob currentJob : jobQueue) {
            ((MilestonePrunerJob) currentJob).setTargetIndex(previousStartIndex);

            previousStartIndex = ((MilestonePrunerJob) currentJob).getStartingIndex();
        }
    }

    public static void processQueue(GarbageCollector garbageCollector, ArrayDeque<GarbageCollectorJob> jobQueue) throws GarbageCollectorException {
        while(!garbageCollector.shuttingDown && jobQueue.size() >= 2 || ((MilestonePrunerJob) jobQueue.getFirst()).getTargetIndex() < ((MilestonePrunerJob) jobQueue.getFirst()).getCurrentIndex()) {
            GarbageCollectorJob firstJob = jobQueue.getFirst();
            firstJob.process();
            if(jobQueue.size() >= 2) {
                jobQueue.removeFirst();
                GarbageCollectorJob secondJob = jobQueue.getFirst();
                jobQueue.addFirst(firstJob);

                secondJob.process();

                // if both jobs are done we can consolidate them to one
                consolidateQueue(garbageCollector, jobQueue);
            } else {
                break;
            }
        }
    }

    public static MilestonePrunerJob parse(String input) throws GarbageCollectorException {
        String[] parts = input.split(";", 2);
        if(parts.length >= 2) {
            return new MilestonePrunerJob(Integer.valueOf(parts[0]), Integer.valueOf(parts[1]));
        }

        throw new GarbageCollectorException("failed to parse garbage collector file");
    }

    /**
     * Constructor of the job receiving both values that are relevant for the job.
     *
     * It simply stores the provided parameters in its according protected properties.
     *
     * Since cleanup jobs can be consolidated (to reduce the size of the garbage collector state file), we need to be
     * able provide both parameters even tho the job usually always "starts" with its {@code currentIndex} being equal
     * to the {@code startingIndex}.
     *
     * @param startingIndex milestone index that defines where to start cleaning up
     */
    public MilestonePrunerJob(int startingIndex) {
        this(startingIndex, startingIndex);
    }

    public MilestonePrunerJob(int startingIndex, int currentIndex) {
        this.startingIndex = startingIndex;
        this.currentIndex = currentIndex;
    }

    /**
     * Getter of the {@link #targetIndex} which reflects the starting point of this job.
     *
     * It simply returns the stored protected property.
     *
     * @return milestone index that defines where to start cleaning up
     */
    public int getTargetIndex() {
        return targetIndex;
    }

    public void setTargetIndex(int targetIndex) {
        this.targetIndex = targetIndex;
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
     * @throws SnapshotException if anything goes wrong while cleaning up or persisting the changes
     */
    public void process() throws GarbageCollectorException {
        while(!garbageCollector.shuttingDown && getTargetIndex() < getCurrentIndex()) {
            cleanupMilestoneTransactions();

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
     * @throws SnapshotException if something goes wrong while cleaning up the milestone
     */
    protected void cleanupMilestoneTransactions() throws GarbageCollectorException {
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
        } catch(Exception e) {
            throw new GarbageCollectorException("failed to cleanup milestone #" + getCurrentIndex(), e);
        }
    }

    public List<Pair<Indexable, ? extends Class<? extends Persistable>>> getElementsToDelete() throws Exception {
        List<Pair<Indexable, ? extends Class<? extends Persistable>>> elementsToDelete = new ArrayList<>();

        MilestoneViewModel milestoneViewModel = MilestoneViewModel.get(garbageCollector.tangle, getCurrentIndex());
        if (milestoneViewModel != null) {
            // collect elements to delete
            elementsToDelete.add(new Pair<>(milestoneViewModel.getHash(), Transaction.class));
            elementsToDelete.add(new Pair<>(new IntegerIndex(milestoneViewModel.index()), Milestone.class));
            DAGHelper.get(garbageCollector.tangle).traverseApprovees(
                milestoneViewModel.getHash(),
                approvedTransaction -> approvedTransaction.snapshotIndex() >= milestoneViewModel.index(),
                approvedTransaction -> {
                    elementsToDelete.add(new Pair<>(approvedTransaction.getHash(), Transaction.class));

                    if (!garbageCollector.snapshotManager.getInitialSnapshot().isSolidEntryPoint(approvedTransaction.getHash())) {
                        cleanupOrphanedApprovers(approvedTransaction, elementsToDelete, new HashSet<>());
                    }
                }
            );
        }

        return elementsToDelete;
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
    protected void cleanupOrphanedApprovers(TransactionViewModel transaction, List<Pair<Indexable, ? extends Class<? extends Persistable>>> elementsToDelete, Set<Hash> processedTransactions) {
        try {
            // remove all orphaned transactions that are branching off of our deleted transactions
            DAGHelper.get(garbageCollector.tangle).traverseApprovers(
                transaction.getHash(),
                approverTransaction -> approverTransaction.snapshotIndex() == 0,
                approverTransaction -> {
                    elementsToDelete.add(new Pair<>(approverTransaction.getHash(), Transaction.class));
                },
                processedTransactions
            );
        } catch(Exception e) {
            log.error("failed to clean up the orphaned approvers of " + transaction, e);
        }
    }

    @Override
    public String toString() {
        return startingIndex + ";" + currentIndex;
    }
}
