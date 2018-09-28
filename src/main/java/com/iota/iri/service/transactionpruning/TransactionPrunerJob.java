package com.iota.iri.service.transactionpruning;

/**
 * This interface represents the basic contract for a job that get processed by the {@link TransactionPruner}.
 */
public interface TransactionPrunerJob {
    /**
     * This method is used to inform the job about the {@link TransactionPruner} it belongs to.
     *
     * It automatically gets called when the jobs gets added to the {@link TransactionPruner}.
     *
     * @param transactionPruner TransactionPruner that this job belongs to
     */
    void registerGarbageCollector(TransactionPruner transactionPruner);

    /**
     * This method processes the cleanup job and performs the actual pruning.
     *
     * @throws TransactionPruningException if something goes wrong while processing the job
     */
    void process() throws TransactionPruningException;

    /**
     * This method is used to serialize the job before it gets persisted by the {@link TransactionPruner}.
     *
     * @return string representing a serialized version of this job
     */
    String serialize();
}
