package com.iota.iri.service.snapshot;

import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.controllers.TipsViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.model.IntegerIndex;
import com.iota.iri.model.Milestone;
import com.iota.iri.model.Transaction;
import com.iota.iri.storage.Indexable;
import com.iota.iri.storage.Persistable;
import com.iota.iri.storage.Tangle;
import com.iota.iri.utils.Pair;
import com.iota.iri.utils.dag.DAGUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class SnapshotGarbageCollector {
    // create references to the classes of the cleaned up entities
    protected static Class<Persistable> CLASS_MILESTONE = (Class<Persistable>) ((Persistable) new Milestone()).getClass();
    protected static Class<Persistable> CLASS_TRANSACTION = (Class<Persistable>) ((Persistable) new Transaction()).getClass();

    /**
     * The interval in milliseconds that the garbage collector will check if new cleanup tasks are available.
     */
    protected static int GARBAGE_COLLECTOR_RESCAN_INTERVAL = 10000;

    /**
     * Logger for this class allowing us to dump debug and status messages.
     */
    protected static final Logger log = LoggerFactory.getLogger(SnapshotGarbageCollector.class);

    /**
     * Boolean flag that indicates if the node is being shutdown.
     */
    protected boolean shuttingDown = false;

    protected Tangle tangle;

    protected SnapshotManager snapshotManager;

    protected TipsViewModel tipsViewModel;

    /**
     * List of cleanup jobs that shall get processed by the garbage collector.
     */
    protected LinkedList<GarbageCollectorJob> cleanupJobs;

    protected DAGUtils dagUtils;

    /**
     * The constructor of this class stores the passed in parameters for future use and restores the previous state of
     * the garbage collector if there is a valid one.
     */
    public SnapshotGarbageCollector(Tangle tangle, SnapshotManager snapshotManager, TipsViewModel tipsViewModel) {
        this.tangle = tangle;
        this.snapshotManager = snapshotManager;
        this.tipsViewModel = tipsViewModel;
        this.dagUtils = DAGUtils.get(tangle);

        restoreCleanupJobs();
    }

    /**
     * This method allows us to add a new cleanup job.
     *
     * The job that is created through this method will take care of removing all the unnecessary database entries
     * before (and including) the given milestoneIndex.
     *
     * @param milestoneIndex
     * @throws SnapshotException if something goes wrong while persisting the job queue
     */
    public void addCleanupJob(int milestoneIndex) throws SnapshotException {
        cleanupJobs.addLast(new GarbageCollectorJob(milestoneIndex, milestoneIndex));

        persistChanges();
    }

    public SnapshotGarbageCollector start() {
        (new Thread(() -> {
            log.info("Snapshot Garbage Collector started ...");

            while(!shuttingDown) {
                try {
                    processCleanupJobs();

                    Thread.sleep(GARBAGE_COLLECTOR_RESCAN_INTERVAL);
                } catch(InterruptedException e) {
                    log.info("Snapshot Garbage Collector stopped ...");

                    shuttingDown = true;
                } catch(SnapshotException e) {
                    log.error("failed to cleanup the garbage", e);
                }
            }
        }, "Snapshot Garbage Collector")).start();

        return this;
    }

    public SnapshotGarbageCollector shutdown() {
        shuttingDown = true;

        return this;
    }

    public SnapshotGarbageCollector reset() {
        cleanupJobs = new LinkedList<>();

        getStateFile().delete();

        return this;
    }

    /**
     * This method takes care of cleaning up a single milestone and performs the actual database operations.
     *
     * This method performs the deletions in an atomic way, which means that either the full processing succeeds or
     * fails.
     *
     * @param milestoneIndex
     * @return the instance of the {@link SnapshotGarbageCollector} that it was called on to allow chaining
     * @throws SnapshotException if something goes wrong while cleaning up the milestone
     */
    protected SnapshotGarbageCollector cleanupMilestoneTransactions(int milestoneIndex) throws SnapshotException {
        try {
            MilestoneViewModel milestoneViewModel = MilestoneViewModel.get(tangle, milestoneIndex);
            if(milestoneViewModel != null) {
                List<Pair<Indexable, Class<Persistable>>> elementsToDelete = new ArrayList<>();

                elementsToDelete.add(new Pair<>(new IntegerIndex(milestoneViewModel.index()), CLASS_MILESTONE));
                elementsToDelete.add(new Pair<>(milestoneViewModel.getHash(), CLASS_TRANSACTION));

                dagUtils.traverseApprovees(
                    // start traversal at the milestone
                    milestoneViewModel,

                    // continue while the transaction belongs to the current milestone
                    approvedTransaction -> approvedTransaction.snapshotIndex() >= milestoneViewModel.index(),

                    // remove all approved transactions
                    approvedTransaction -> {
                        elementsToDelete.add(new Pair<>(approvedTransaction.getHash(), CLASS_TRANSACTION));

                        cleanupOrphanedApprovers(approvedTransaction, elementsToDelete, new HashSet<>());
                    }
                );

                MilestoneViewModel.clear(milestoneIndex);

                elementsToDelete.stream().forEach(element -> {
                    if(CLASS_TRANSACTION.equals(element.hi)) {
                        tipsViewModel.removeTipHash((Hash) element.low);
                    }
                });
                tipsViewModel.removeTipHash(milestoneViewModel.getHash());

                tangle.deleteBatch(elementsToDelete);
            }
        } catch(Exception e) {
            throw new SnapshotException("failed to cleanup milestone #" + milestoneIndex, e);
        }

        return this;
    }

    protected void cleanupOrphanedApprovers(TransactionViewModel transaction, List<Pair<Indexable, Class<Persistable>>> elementsToDelete, Set<Hash> processedTransactions) throws Exception {
        // remove all orphaned transactions that are branching off of our deleted transactions
        dagUtils.traverseApprovers(
            transaction,

            approverTransaction -> approverTransaction.snapshotIndex() == 0,

            approverTransaction -> {
                elementsToDelete.add(new Pair<>(approverTransaction.getHash(), CLASS_TRANSACTION));

                /*dagUtils.traverseApprovees(
                    approverTransaction,
                    approvedTransaction -> !approvedTransaction.isSolid(),
                    approvedTransaction -> {
                        cleanupOrphanedApprovers(approvedTransaction, elementsToDelete, processedTransactions);
                    }
                );*/
            },

            processedTransactions
        );
    }

    protected SnapshotGarbageCollector processCleanupJob(GarbageCollectorJob job, int cleanupTarget) throws SnapshotException {
        while(!shuttingDown && cleanupTarget < job.getCurrentIndex()) {
            cleanupMilestoneTransactions(job.getCurrentIndex());

            job.setCurrentIndex(job.getCurrentIndex() - 1);

            persistChanges();
        }

        return this;
    }

    protected SnapshotGarbageCollector processCleanupJobs() throws SnapshotException {
        // repeat until all jobs are processed
        while(!shuttingDown && cleanupJobs.size() >= 1) {
            GarbageCollectorJob firstJob = cleanupJobs.getFirst();
            processCleanupJob(firstJob, snapshotManager.getConfiguration().getMilestoneStartIndex());

            if(cleanupJobs.size() >= 2) {
                cleanupJobs.removeFirst();
                GarbageCollectorJob secondJob = cleanupJobs.getFirst();
                cleanupJobs.addFirst(firstJob);

                processCleanupJob(secondJob, firstJob.getStartingIndex());

                // if both jobs are done we can consolidate them to one
                consolidateCleanupJobs();
            } else {
                break;
            }
        }

        return this;
    }

    /**
     * This method consolidates the first two cleanup jobs into a single one if both are done.
     *
     * It is used to clean up the cleanupJobs queue while we process it, so it doesn't grow indefinitely while we add
     * new jobs.
     *
     * @throws SnapshotException if an error occurs while persisting the state
     */
    protected SnapshotGarbageCollector consolidateCleanupJobs() throws SnapshotException {
        // if we have at least 2 jobs -> check if we can consolidate them at the beginning
        if(cleanupJobs.size() >= 2) {
            // retrieve the first two jobs
            GarbageCollectorJob job1 = cleanupJobs.removeFirst();
            GarbageCollectorJob job2 = cleanupJobs.removeFirst();

            // if both first job are done -> consolidate them and persists the changes
            if(job1.getCurrentIndex() == snapshotManager.getConfiguration().getMilestoneStartIndex() && job2.getCurrentIndex() == job1.getStartingIndex()) {
                cleanupJobs.addFirst(new GarbageCollectorJob(job2.getStartingIndex(), job1.getCurrentIndex()));

                persistChanges();
            }

            // otherwise just add them back to the queue
            else {
                cleanupJobs.addFirst(job2);
                cleanupJobs.addFirst(job1);
            }
        }

        // if we have at least 2 jobs -> check if we can consolidate them at the end
        if(cleanupJobs.size() >= 2) {
            // retrieve the last two jobs
            GarbageCollectorJob job1 = cleanupJobs.removeLast();
            GarbageCollectorJob job2 = cleanupJobs.removeLast();

            // if both jobs are pending -> consolidate them and persists the changes
            if(job1.getCurrentIndex() == job1.getStartingIndex() && job2.getCurrentIndex() == job2.getStartingIndex()) {
                cleanupJobs.addLast(new GarbageCollectorJob(job1.getStartingIndex(), job1.getCurrentIndex()));

                persistChanges();
            }

            // otherwise just add them back to the queue
            else {
                cleanupJobs.addLast(job2);
                cleanupJobs.addLast(job1);
            }
        }

        return this;
    }

    /**
     * This method persists the changes of the garbage collector so IRI can continue cleaning up upon restarts.
     *
     * Since cleaning up the old database entries can take a long time, we need to make sure that it is possible to
     * continue where we stopped. This method therefore writes the state of the queued jobs into a file that is read
     * upon re-initialization of the GarbageCollector.
     *
     * @throws SnapshotException if something goes wrong while writing the state file
     */
    protected SnapshotGarbageCollector persistChanges() throws SnapshotException {
        try {
            Files.write(
                Paths.get(getStateFile().getAbsolutePath()),
                () -> cleanupJobs.stream().<CharSequence>map(entry -> Integer.toString(entry.getStartingIndex()) + ";" + Integer.toString(entry.getCurrentIndex())).iterator()
            );
        } catch(IOException e) {
            throw new SnapshotException("could not persists garbage collector state", e);
        }

        return this;
    }

    /**
     * This method tries to restore the previous state of the Garbage Collector by reading the state file that get's
     * persisted whenever we modify the queue.
     *
     * It is used to restore the state of the garbage collector between IRI restarts and speed up the pruning
     * operations. If it fails to restore the state it just continues with an empty state which doesn't cause any
     * problems with future jobs other than requiring them to perform unnecessary steps and therefore slowing them down
     * a bit.
     */
    protected SnapshotGarbageCollector restoreCleanupJobs() {
        cleanupJobs = new LinkedList<>();

        try {
            // create a reader for our file
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(
                    new BufferedInputStream(
                        new FileInputStream(getStateFile())
                    )
                )
            );

            // read the saved cleanup states back into our queue
            String line;
            while((line = reader.readLine()) != null) {
                String[] parts = line.split(";", 2);
                if(parts.length >= 2) {
                    cleanupJobs.addLast(new GarbageCollectorJob(Integer.valueOf(parts[0]), Integer.valueOf(parts[1])));
                }
            }

            reader.close();
        } catch(Exception e) { /* do nothing */ }

        return this;
    }

    protected File getStateFile() {
        return new File(snapshotManager.getConfiguration().getLocalSnapshotsMainnetBasePath() + ".snapshot.gc");
    }
}