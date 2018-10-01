package com.iota.iri.service.transactionpruning.async;

import com.iota.iri.service.transactionpruning.TransactionPrunerJob;
import com.iota.iri.service.transactionpruning.TransactionPruningException;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.NoSuchElementException;

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
     * While it finds jobs, it retrieves the first job and processes it. If an error occurs while executing the job, we
     * add it back to the end of the queue, so it can be tried again later.
     *
     * @throws TransactionPruningException if anything goes wrong while processing the jobs
     */
    public void processJobs() throws TransactionPruningException {
        while(!Thread.interrupted() && jobs.size() >= 1) {
            TransactionPrunerJob currentJob;

            try {
                currentJob = jobs.removeFirst();
                try {
                    currentJob.process();
                } catch (TransactionPruningException e) {
                    jobs.addLast(currentJob);

                    throw e;
                }
            } catch(NoSuchElementException e) {
                /* something cleared the job in the mean time -> ignore */
            }
        }
    }
}
