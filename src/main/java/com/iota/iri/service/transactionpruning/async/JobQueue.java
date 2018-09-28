package com.iota.iri.service.transactionpruning.async;

import com.iota.iri.service.transactionpruning.TransactionPrunerJob;
import com.iota.iri.service.transactionpruning.TransactionPruningException;

import java.util.ArrayDeque;
import java.util.Deque;

public class JobQueue {
    private final Deque<TransactionPrunerJob> jobs = new ArrayDeque<>();

    public Deque<TransactionPrunerJob> getJobs() {
        return jobs;
    }

    public void addJob(TransactionPrunerJob job) {
        jobs.addLast(job);
    }

    public void clear() {
        jobs.clear();
    }

    /**
     * This method processes the entire queue of this job type.
     *
     * While it finds jobs, it retrieves the first job, processes it and then removes it from the queue, before
     * persisting the changes to keep track of the progress.
     *
     * @throws TransactionPruningException if anything goes wrong while processing the jobs
     */
    public void processJobs() throws TransactionPruningException {
        while(!Thread.interrupted() && jobs.size() >= 1) {
            jobs.removeFirst().process();
        }
    }
}
