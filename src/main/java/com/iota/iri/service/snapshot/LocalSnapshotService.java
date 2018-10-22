package com.iota.iri.service.snapshot;

import com.iota.iri.conf.SnapshotConfig;
import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.service.transactionpruning.TransactionPruner;
import com.iota.iri.storage.Tangle;

import java.util.Map;

/**
 * Represents the service for local snapshots that contains the relevant business logic for creating taking and
 * generating local {@link Snapshot}s.
 *
 * This class is stateless and does not hold any domain specific models.
 */
public interface LocalSnapshotService {
    void takeLocalSnapshot(Tangle tangle, SnapshotProvider snapshotProvider, SnapshotConfig config,
            TransactionPruner transactionPruner) throws SnapshotException;

    Snapshot generateLocalSnapshot(Tangle tangle, SnapshotProvider snapshotProvider, SnapshotConfig config,
            MilestoneViewModel targetMilestone) throws SnapshotException;

    Map<Hash, Integer> generateSolidEntryPoints(Tangle tangle, SnapshotProvider snapshotProvider, MilestoneViewModel
            targetMilestone) throws SnapshotException;

    Map<Hash, Integer> generateSeenMilestones(Tangle tangle, SnapshotConfig config, MilestoneViewModel targetMilestone)
            throws SnapshotException;
}