package com.iota.iri.service.transactionpruning;

import com.iota.iri.controllers.TipsViewModel;
import com.iota.iri.service.snapshot.Snapshot;
import com.iota.iri.storage.Tangle;

/**
 * This interface represents the basic contract for a job that get processed by the {@link TransactionPruner}.
 */
public interface TransactionPrunerJob {
    void setTransactionPruner(TransactionPruner transactionPruner);

    TransactionPruner getTransactionPruner();

    /**
     * Allows to set the {@link Tangle} object that this job should work on.
     *
     * We do not pass this parameter in via the constructor of the job because the container, that the job get's added
     * to (the {@link TransactionPruner} already has knowledge about the {@link Tangle} instance we are working on and
     * can automatically set the reference upon addition of the job.
     *
     * This way we reduce the amount of code that has to be written when creating a job and at the same time ensure that
     * all jobs are automatically working on the same correct {@link Tangle} instance, while still keeping full control
     * over the property in tests.
     *
     * @param tangle Tangle object which acts as a database interface
     */
    void setTangle(Tangle tangle);

    /**
     * This method returns the stored {@link Tangle} instance.
     *
     * @return Tangle object which acts as a database interface
     */
    Tangle getTangle();

    /**
     * Allows to set the {@link TipsViewModel} object that this job uses to remove pruned transactions from this cached
     * instance.
     *
     * We do not pass this parameter in via the constructor of the job because the container, that the job get's added
     * to (the {@link TransactionPruner} already has knowledge about the {@link TipsViewModel} instance we are working
     * on and can automatically set the reference upon addition of the job.
     *
     * This way we reduce the amount of code that has to be written when creating a job and at the same time ensure that
     * all jobs are automatically working on the same correct {@link TipsViewModel} instance, while still keeping full
     * control over the property in tests.
     *
     * @param tipsViewModel manager for the tips (required for removing pruned transactions from this manager)
     */
    void setTipsViewModel(TipsViewModel tipsViewModel);

    /**
     * This method returns the previously stored {@link TipsViewModel} instance.
     *
     * @return manager for the tips (required for removing pruned transactions from this manager)
     */
    TipsViewModel getTipsViewModel();

    void setSnapshot(Snapshot snapshot);

    Snapshot getSnapshot();

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
