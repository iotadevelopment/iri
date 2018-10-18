package com.iota.iri.service.snapshot;

/**
 * The data provider that allows to retrieve the {@link Snapshot} instances that are relevant for the node.
 */
public interface SnapshotProvider {
    /**
     * Returns the Snapshot that reflects the start of the ledger (either the last global or a local {@link Snapshot}).
     *
     * This Snapshot will be modified whenever a new local {@link Snapshot} is taken.
     *
     * @return the Snapshot that reflects the start of the ledger (either the last global or a local {@link Snapshot})
     */
    Snapshot getInitialSnapshot();

    /**
     * Returns the Snapshot that reflects the most recent "confirmed" state of the ledger.
     *
     * This Snapshot will be modified whenever a new milestone arrives and new transactions are confirmed.
     *
     * @return the Snapshot that represents the most recent "confirmed" state of the ledger
     */
    Snapshot getLatestSnapshot();
}
