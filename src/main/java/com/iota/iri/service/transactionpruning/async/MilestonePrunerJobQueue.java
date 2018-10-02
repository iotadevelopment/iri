package com.iota.iri.service.transactionpruning.async;

import com.iota.iri.conf.SnapshotConfig;
import com.iota.iri.service.transactionpruning.TransactionPruner;
import com.iota.iri.service.transactionpruning.TransactionPrunerJob;
import com.iota.iri.service.transactionpruning.TransactionPruningException;
import com.iota.iri.service.transactionpruning.jobs.MilestonePrunerJob;

import java.util.Deque;

/**
 * Represents a queue of {@link MilestonePrunerJob}s thar are being executed by the {@link AsyncTransactionPruner}.
 *
 * The {@link AsyncTransactionPruner} uses a separate queue for every job type, to be able to adjust the processing
 * logic based on the type of the job.
 */
public class MilestonePrunerJobQueue extends JobQueue {
    /**
     * Holds a reference to the config with snapshot related parameters.
     */
    private final SnapshotConfig snapshotConfig;

    /**
     * Creates a new queue that is tailored to handle {@link MilestonePrunerJob}s.
     *
     * Since the {@link MilestonePrunerJob}s can take a long time until they are fully processed, we handle them in a
     * different way than other jobs and consolidate the queue whenever possible.
     *
     * @param transactionPruner reference to the container of this queue
     * @param snapshotConfig reference to the config with snapshot related parameters
     */
    public MilestonePrunerJobQueue(TransactionPruner transactionPruner, SnapshotConfig snapshotConfig) {
        super(transactionPruner);

        this.snapshotConfig = snapshotConfig;
    }

    /**
     * {@inheritDoc}
     *
     * After adding the job we consolidate the queue.
     *
     * @param job job that shall be added to the queue
     */
    @Override
    public void addJob(TransactionPrunerJob job) {
        super.addJob(job);

        consolidate();
    }

    /**
     * This method processes the entire queue of this job.
     *
     * It first retrieves the first job and processes it until it is completely done. If it finds a second job, it
     * immediately tries to process that one as well. If both jobs are done it triggers a consolidation to merge both
     * done jobs into a single one.
     *
     * We keep the first done job in the queue (even though it might have been consolidated) because we need its
     * {@link MilestonePrunerJob#startingIndex} to determine the next jobs {@link MilestonePrunerJob#targetIndex}.
     *
     * @throws TransactionPruningException if anything goes wrong while processing the jobs
     */
    @Override
    public void processJobs() throws TransactionPruningException {
        Deque<TransactionPrunerJob> jobQueue = getJobs();

        while(
            !Thread.interrupted() &&
            jobQueue.size() >= 2 ||
            (
                jobQueue.size() >= 1 &&
                ((MilestonePrunerJob) jobQueue.getFirst()).getTargetIndex() < ((MilestonePrunerJob) jobQueue.getFirst()).getCurrentIndex()
            )
        ) {
            MilestonePrunerJob firstJob = (MilestonePrunerJob) jobQueue.removeFirst();
            firstJob.process();

            if(jobQueue.size() >= 1) {
                MilestonePrunerJob secondJob = (MilestonePrunerJob) jobQueue.getFirst();
                jobQueue.addFirst(firstJob);

                secondJob.process();

                consolidate();

                getTransactionPruner().saveState();
            } else {
                jobQueue.addFirst(firstJob);

                break;
            }
        }
    }

    /**
     * This method consolidates the cleanup jobs by merging two or more jobs together if they are either both done or
     * pending.
     *
     * It is used to clean up the queue and the corresponding {@link AsyncTransactionPruner} file, so it always has a
     * size of less than 4 jobs.
     *
     * Since the jobs are getting processed from the beginning of the queue, we first check if the first two jobs are
     * "done" and merge them into a single one that reflects the "done" status of both jobs. Consecutively we check if
     * there are two or more pending jobs at the end that can also be consolidated.
     *
     * It is important to note that the jobs always clean from their startingPosition to the startingPosition of the
     * previous job (or the {@code milestoneStartIndex} of the last global snapshot if there is no previous one) without
     * any gaps in between which is required to be able to merge them.
     */
    private void consolidate() {
        Deque<TransactionPrunerJob> jobQueue = getJobs();

        // if we have at least 2 jobs -> check if we can consolidate them at the beginning (both done)
        if(jobQueue.size() >= 2) {
            MilestonePrunerJob job1 = (MilestonePrunerJob) jobQueue.removeFirst();
            MilestonePrunerJob job2 = (MilestonePrunerJob) jobQueue.removeFirst();

            // if both first job are done -> consolidate them and persists the changes
            if(job1.getCurrentIndex() == snapshotConfig.getMilestoneStartIndex() && job2.getCurrentIndex() == job1.getStartingIndex()) {
                job1.setStartingIndex(job2.getStartingIndex());
                jobQueue.addFirst(job1);
            }

            // otherwise just add them back to the queue
            else {
                jobQueue.addFirst(job2);
                jobQueue.addFirst(job1);
            }
        }

        // if we have at least 2 jobs -> check if we can consolidate them at the end (both pending)
        boolean cleanupSuccessful = true;
        while(jobQueue.size() >= 2 && cleanupSuccessful) {
            MilestonePrunerJob job1 = (MilestonePrunerJob) jobQueue.removeLast();
            MilestonePrunerJob job2 = (MilestonePrunerJob) jobQueue.removeLast();

            // if both jobs are pending -> consolidate them and persists the changes
            if(job1.getCurrentIndex() == job1.getStartingIndex() && job2.getCurrentIndex() == job2.getStartingIndex()) {
                jobQueue.addLast(job1);
            }

            // otherwise just add them back to the queue
            else {
                jobQueue.addLast(job2);
                jobQueue.addLast(job1);

                cleanupSuccessful = false;
            }
        }

        // update the target index of the jobs
        int previousStartIndex = snapshotConfig.getMilestoneStartIndex();
        for(TransactionPrunerJob currentJob : jobQueue) {
            ((MilestonePrunerJob) currentJob).setTargetIndex(previousStartIndex);

            previousStartIndex = ((MilestonePrunerJob) currentJob).getStartingIndex();
        }
    }
}
