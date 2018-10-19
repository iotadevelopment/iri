package com.iota.iri.service.snapshot;

import com.iota.iri.MilestoneTracker;
import com.iota.iri.conf.SnapshotConfig;
import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.model.Hash;

import java.util.Map;

/**
 * Represents the manager for local {@link Snapshot}s that takes care of periodically creating a new {@link Snapshot}
 * when the configured interval has passed.
 *
 * After the local snapshot was taken it also triggers the pruning of old transactions to clean up the database.
 */
public interface LocalSnapshotManager {
    Snapshot generateLocalSnapshot(MilestoneViewModel targetMilestone) throws SnapshotException;

    void takeLocalSnapshot() throws SnapshotException;

    Map<Hash, Integer> generateSolidEntryPoints(MilestoneViewModel targetMilestone) throws SnapshotException;

    Map<Hash, Integer> generateSeenMilestones(MilestoneViewModel targetMilestone) throws SnapshotException;

    /**
     * Starts the automatic creation of local {@link Snapshot}s by spawning a background {@link Thread}, that
     * periodically checks if the last snapshot is older than {@link SnapshotConfig#getLocalSnapshotsIntervalSynced()}.
     *
     * When we detect that it is time for a local snapshot we internally trigger its creation.
     *
     * Note: If the node is not fully synced we use {@link SnapshotConfig#getLocalSnapshotsIntervalUnsynced()} instead.
     *
     * @param milestoneTracker tracker for the milestones to determine when a new local snapshot is due
     */
    void start(MilestoneTracker milestoneTracker);

    /**
     * Stops the {@link Thread} that takes care of creating the local {@link Snapshot}s and that was spawned by the
     * {@link #start(MilestoneTracker)} method.
     */
    void shutdown();
}
