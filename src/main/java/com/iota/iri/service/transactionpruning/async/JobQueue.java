package com.iota.iri.service.transactionpruning.async;

import com.iota.iri.service.transactionpruning.TransactionPrunerJob;
import com.iota.iri.service.transactionpruning.TransactionPruningException;

import java.util.ArrayDeque;
import java.util.Deque;

public class JobQueue {
    private final AsyncTransactionPruner transactionPruner;

    private Deque<TransactionPrunerJob> jobs = new ArrayDeque<>();

    public JobQueue(AsyncTransactionPruner transactionPruner) {
        this.transactionPruner = transactionPruner;
    }

    public void addJob(TransactionPrunerJob job) {
        jobs.addLast(job);
    }

    public AsyncTransactionPruner getTransactionPruner() {
        return transactionPruner;
    }

    public Deque<TransactionPrunerJob> getJobs() {
        return jobs;
    }

    public void processJobs() throws TransactionPruningException {
        while(!Thread.interrupted() && jobs.size() >= 1) {
            jobs.getFirst().process();

            jobs.removeFirst();

            transactionPruner.saveState();
        }
    }
}
