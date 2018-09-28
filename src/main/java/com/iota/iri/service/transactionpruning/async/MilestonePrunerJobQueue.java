package com.iota.iri.service.transactionpruning.async;

import com.iota.iri.conf.SnapshotConfig;
import com.iota.iri.service.transactionpruning.TransactionPrunerJob;
import com.iota.iri.service.transactionpruning.TransactionPruningException;

import java.util.Deque;

public class MilestonePrunerJobQueue extends JobQueue {
    private final SnapshotConfig snapshotConfig;

    public MilestonePrunerJobQueue(SnapshotConfig snapshotConfig) {
        super();

        this.snapshotConfig = snapshotConfig;
    }

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
                jobQueue.addFirst(job1.setStartingIndex(job2.getStartingIndex()));
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

        int previousStartIndex = snapshotConfig.getMilestoneStartIndex();
        for(TransactionPrunerJob currentJob : jobQueue) {
            ((MilestonePrunerJob) currentJob).setTargetIndex(previousStartIndex);

            previousStartIndex = ((MilestonePrunerJob) currentJob).getStartingIndex();
        }
    }
}
