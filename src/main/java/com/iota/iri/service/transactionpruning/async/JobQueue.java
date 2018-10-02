package com.iota.iri.service.transactionpruning.async;

import com.iota.iri.service.transactionpruning.TransactionPruner;
import com.iota.iri.service.transactionpruning.TransactionPrunerJob;
import com.iota.iri.service.transactionpruning.TransactionPruningException;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

/**
 * Represents a queue of {@link TransactionPrunerJob}s thar are being executed by the {@link AsyncTransactionPruner}.
 *
 * The {@link AsyncTransactionPruner} uses a separate queue for every job type, to be able to adjust the processing
 * logic based on the type of the job.
 */
public class JobQueue {
    /**
     * Holds a reference to the container of this queue.
     */
    private final TransactionPruner transactionPruner;

    /**
     * List of jobs that is used to internally store the queued jobs.
     */
    private final Deque<TransactionPrunerJob> jobs = new ArrayDeque<>();

    /**
     * Creates a queue of jobs that will process them in their insertion order and persist the state after every
     * processed job.
     *
     * This is used by all job types that do not require special routines for getting processed.
     *
     * @param transactionPruner reference to the container of this queue
     */
    public JobQueue(TransactionPruner transactionPruner) {
        this.transactionPruner = transactionPruner;
    }

    /**
     * Allows to add a job to the queue.
     *
     * It simply adds the job to the underlying {@link #jobs}.
     *
     * @param job job that shall be added to the queue
     */
    public void addJob(TransactionPrunerJob job) {
        jobs.addLast(job);
    }

    /**
     * Clears the stored jobs and empties the internal list.
     */
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

                    getTransactionPruner().saveState();
                } catch (TransactionPruningException e) {
                    jobs.addLast(currentJob);

                    getTransactionPruner().saveState();

                    throw e;
                }
            } catch(NoSuchElementException e) {
                /* something cleared the job in the mean time -> ignore */
            }
        }
    }

    /**
     * Returns a stream of jobs that can for example be used to serialize the jobs of this queue.
     *
     * @return Stream of jobs that are currently part of this queue
     */
    public Stream<TransactionPrunerJob> stream() {
        return getJobs().stream();
    }

    /**
     * Getter of the {@link #jobs}.
     *
     * It allows child classes to access the private property that holds the jobs.
     *
     * @return List of jobs that is used to internally store the queued jobs
     */
    protected Deque<TransactionPrunerJob> getJobs() {
        return jobs;
    }

    /**
     * Getter of the {@link #transactionPruner}.
     *
     * It allows child classes to access the private property that holds the reference to the container of this queue.
     *
     * @return reference to the container of this queue
     */
    protected TransactionPruner getTransactionPruner() {
        return transactionPruner;
    }
}
