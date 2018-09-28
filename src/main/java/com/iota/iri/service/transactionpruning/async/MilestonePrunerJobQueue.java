package com.iota.iri.service.transactionpruning.async;

import com.iota.iri.conf.SnapshotConfig;
import com.iota.iri.service.transactionpruning.TransactionPrunerJob;
import com.iota.iri.service.transactionpruning.TransactionPruningException;

import java.util.Deque;

public class MilestonePrunerJobQueue extends JobQueue {
    private final SnapshotConfig config;

    public MilestonePrunerJobQueue(AsyncTransactionPruner transactionPruner, SnapshotConfig config) {
        super(transactionPruner);

        this.config = config;
    }

    @Override
    public void addJob(TransactionPrunerJob job) {
        super.addJob(job);

        consolidate();
    }

    @Override
    public void processJobs() throws TransactionPruningException {
        Deque<TransactionPrunerJob> jobQueue = getJobs();

        // if we have at least 2 jobs -> check if we can consolidate them at the beginning (both done)
        if(jobQueue.size() >= 2) {
            MilestonePrunerJob job1 = (MilestonePrunerJob) jobQueue.removeFirst();
            MilestonePrunerJob job2 = (MilestonePrunerJob) jobQueue.removeFirst();

            // if both first job are done -> consolidate them and persists the changes
            if(job1.getCurrentIndex() == config.getMilestoneStartIndex() && job2.getCurrentIndex() == job1.getStartingIndex()) {
                MilestonePrunerJob consolidatedJob = new MilestonePrunerJob(job2.getStartingIndex(), job1.getCurrentIndex());
                consolidatedJob.registerGarbageCollector(getTransactionPruner());
                jobQueue.addFirst(consolidatedJob);
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
                MilestonePrunerJob consolidatedJob = new MilestonePrunerJob(job1.getStartingIndex(), job1.getCurrentIndex());
                consolidatedJob.registerGarbageCollector(getTransactionPruner());
                jobQueue.addLast(consolidatedJob);
            }

            // otherwise just add them back to the queue
            else {
                jobQueue.addLast(job2);
                jobQueue.addLast(job1);

                cleanupSuccessful = false;
            }
        }

        int previousStartIndex = config.getMilestoneStartIndex();
        for(TransactionPrunerJob currentJob : jobQueue) {
            ((MilestonePrunerJob) currentJob).setTargetIndex(previousStartIndex);

            previousStartIndex = ((MilestonePrunerJob) currentJob).getStartingIndex();
        }
    }

    private void consolidate() {
        ;
    }
}
