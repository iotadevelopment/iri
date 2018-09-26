package com.iota.iri.service.transactionpruning;

/**
 * This class is a template for a {@link TransactionPruner} job.
 *
 * It acts as a base class and a generic interface for processing the jobs in the {@link TransactionPruner}.
 */
abstract class TransactionPrunerJob {
    /**
     * Holds a reference to the {@link TransactionPruner} that this job belongs to.
     */
    TransactionPruner transactionPruner;

    /**
     * This method is used to inform the job about the {@link TransactionPruner} it belongs to.
     *
     * It automatically gets called when the jobs gets added to the {@link TransactionPruner}.
     *
     * @param transactionPruner TransactionPruner that this job belongs to
     */
    void registerGarbageCollector(TransactionPruner transactionPruner) {
        this.transactionPruner = transactionPruner;
    }

    /**
     * This method processes the cleanup job and performs the actual pruning.
     *
     * @throws TransactionPruningException if something goes wrong while processing the job
     */
    abstract void process() throws TransactionPruningException;

    /**
     * This method is used to serialize the job before it gets persisted in the {@link TransactionPruner} state file.
     *
     * @return string representing a serialized version of this job
     */
    abstract String serialize();
}
