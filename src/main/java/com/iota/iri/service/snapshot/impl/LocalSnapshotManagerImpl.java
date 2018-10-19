package com.iota.iri.service.snapshot.impl;

import com.iota.iri.MilestoneTracker;
import com.iota.iri.conf.SnapshotConfig;
import com.iota.iri.controllers.*;
import com.iota.iri.model.Hash;
import com.iota.iri.service.snapshot.LocalSnapshotManager;
import com.iota.iri.service.snapshot.Snapshot;
import com.iota.iri.service.snapshot.SnapshotException;
import com.iota.iri.service.snapshot.SnapshotProvider;
import com.iota.iri.service.transactionpruning.TransactionPruner;
import com.iota.iri.service.transactionpruning.jobs.MilestonePrunerJob;
import com.iota.iri.service.transactionpruning.TransactionPruningException;
import com.iota.iri.service.transactionpruning.jobs.UnconfirmedSubtanglePrunerJob;
import com.iota.iri.storage.Tangle;
import com.iota.iri.utils.ProgressLogger;
import com.iota.iri.utils.dag.DAGHelper;
import com.iota.iri.utils.dag.TraversalException;

import com.iota.iri.utils.thread.ThreadIdentifier;
import com.iota.iri.utils.thread.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.iota.iri.MilestoneTracker.Status.INITIALIZED;

/**
 * Implements the basic contract of the {@link LocalSnapshotManager}.
 */
public class LocalSnapshotManagerImpl implements LocalSnapshotManager {
    /**
     * The interval (in milliseconds) in which we check if a new local {@link Snapshot} is due.
     */
    private static final int LOCAL_SNAPSHOT_RESCAN_INTERVAL = 10000;

    /**
     * If transactions got orphaned we wait this additional time (in seconds) until we consider them orphaned.
     */
    private static final int ORPHANED_TRANSACTION_GRACE_TIME = 3600;

    /**
     * Maximum age in milestones since creation of solid entry points.
     *
     * Since it is possible to artificially keep old solid entry points alive by periodically attaching new transactions
     * to it, we limit the life time of solid entry points and ignore them whenever they become too old. This is a
     * measure against a potential attack vector of people trying to blow up the meta data of local snapshots.
     */
    private static final int SOLID_ENTRY_POINT_LIFETIME = 20000;

    /**
     * Logger for this class allowing us to dump debug and status messages.
     */
    private static final Logger log = LoggerFactory.getLogger(LocalSnapshotManagerImpl.class);

    /**
     * Data provider for the relevant {@link Snapshot} instances.
     */
    private final SnapshotProvider snapshotProvider;

    /**
     * Manager for the pruning jobs that allows us to clean up old transactions.
     */
    private final TransactionPruner transactionPruner;

    /**
     * Tangle object which acts as a database interface.
     */
    private final Tangle tangle;

    /**
     * Configuration with important snapshot related parameters.
     */
    private final SnapshotConfig configuration;

    /**
     * Holds a reference to the {@link ThreadIdentifier} for the monitor thread.
     *
     * Using a {@link ThreadIdentifier} for spawning the thread allows the {@link ThreadUtils} to spawn exactly one
     * thread for this instance even when we call the {@link #start(MilestoneTracker)} method multiple times.
     */
    private ThreadIdentifier monitorThreadIdentifier = new ThreadIdentifier("Local Snapshots Monitor");

    /**
     * Creates the {@link LocalSnapshotManager} that takes care of .
     *
     * It simply stores the passed in parameters in their private properties.
     *
     * @param snapshotProvider data provider for the relevant {@link Snapshot} instances
     * @param transactionPruner manager for the pruning jobs that allows us to clean up old transactions
     * @param tangle object which acts as a database interface
     * @param configuration configuration with important snapshot related parameters
     */
    public LocalSnapshotManagerImpl(SnapshotProvider snapshotProvider, TransactionPruner transactionPruner, Tangle tangle, SnapshotConfig configuration) {
        this.snapshotProvider = snapshotProvider;
        this.transactionPruner = transactionPruner;
        this.tangle = tangle;
        this.configuration = configuration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start(MilestoneTracker milestoneTracker) {
        ThreadUtils.spawnThread(() -> monitorThread(milestoneTracker), monitorThreadIdentifier);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown() {
        ThreadUtils.stopThread(monitorThreadIdentifier);
    }

    /**
     * This method contains the logic for the monitoring Thread.
     *
     * It periodically checks if a new {@link Snapshot} has to be taken until the {@link Thread} is terminated. If it
     * detects that a {@link Snapshot} is due it triggers the creation of the {@link Snapshot} by calling
     * {@link #takeLocalSnapshot()}.
     *
     * @param milestoneTracker tracker for the milestones to determine when a new local snapshot is due
     */
    private void monitorThread(MilestoneTracker milestoneTracker) {
        while (!Thread.currentThread().isInterrupted()) {
            int localSnapshotInterval = milestoneTracker.getStatus() == INITIALIZED &&
                    snapshotProvider.getLatestSnapshot().getIndex() == milestoneTracker.latestMilestoneIndex
                    ? configuration.getLocalSnapshotsIntervalSynced()
                    : configuration.getLocalSnapshotsIntervalUnsynced();

            if (snapshotProvider.getLatestSnapshot().getIndex() - snapshotProvider.getLatestSnapshot().getIndex() >
                    configuration.getLocalSnapshotsDepth() + localSnapshotInterval) {

                try {
                    takeLocalSnapshot();
                } catch (SnapshotException e) {
                    log.error("error while taking local snapshot", e);
                }
            }

            ThreadUtils.sleep(LOCAL_SNAPSHOT_RESCAN_INTERVAL);
        }
    }

    /**
     * This method determines if a transaction is orphaned.
     *
     * Since there is no hard definition for when a transaction can be considered to be orphaned, we define orphaned in
     * relation to a referenceTransaction. If the transaction or any of its direct or indirect approvers saw a
     * transaction being attached to it, that arrived after our reference transaction, we consider it "not orphaned".
     *
     * Since we currently use milestones as reference transactions that are sufficiently old, this definition in fact is
     * a relatively safe way to determine if a subtangle "above" a transaction got orphaned.
     *
     * @param transaction transaction that shall be checked
     * @param referenceTransaction transaction that acts as a judge to the other transaction
     * @return true if the transaction got orphaned and false otherwise
     * @throws SnapshotException if anything goes wrong while determining the orphaned status
     */
    private boolean isOrphaned(TransactionViewModel transaction, TransactionViewModel referenceTransaction, HashSet<Hash> processedTransactions) throws SnapshotException {
        long timeDiff = (referenceTransaction.getArrivalTime() / 1000L) - referenceTransaction.getTimestamp();

        if (((transaction.getArrivalTime() / 1000L) + ORPHANED_TRANSACTION_GRACE_TIME - timeDiff) > referenceTransaction.getTimestamp()) {
            return false;
        }

        AtomicBoolean nonOrphanedTransactionFound = new AtomicBoolean(false);
        try {
            DAGHelper.get(tangle).traverseApprovers(
                    transaction.getHash(),
                    currentTransaction -> !nonOrphanedTransactionFound.get(),
                    currentTransaction -> {
                        if (((currentTransaction.getArrivalTime() / 1000L) + ORPHANED_TRANSACTION_GRACE_TIME - timeDiff) > referenceTransaction.getTimestamp()) {
                            nonOrphanedTransactionFound.set(true);
                        }
                    },
                    processedTransactions
            );
        } catch (TraversalException e) {
            throw new SnapshotException("failed to determine orphaned status of " + transaction, e);
        }

        return !nonOrphanedTransactionFound.get();
    }

    /**
     * This method checks if a transaction is a solid entry point for the targetMilestone.
     *
     * A transaction is considered a solid entry point if it has non-orphaned approvers.
     *
     * To check if the transaction has non-orphaned approvers we first check if any of its approvers got confirmed by a
     * future milestone, since this is very cheap. If non of them got confirmed by another milestone we do the more
     * expensive check from {@link #isOrphaned(TransactionViewModel, TransactionViewModel, HashSet)}.
     *
     * Since solid entry points have a limited life time and to prevent potential problems due to temporary errors in
     * the database, we assume that the checked transaction is a solid entry point if any error occurs while determining
     * its status. This is a storage <=> reliability trade off, since the only bad effect of having too many solid entry
     * points) is a bigger snapshot file.
     *
     * @param transactionHash hash of the transaction that shall be checked
     * @param targetMilestone milestone that is used as an anchor for our checks
     * @return true if the transaction is a solid entry point and false otherwise
     */
    private boolean isSolidEntryPoint(Hash transactionHash, MilestoneViewModel targetMilestone) {
        Set<TransactionViewModel> unconfirmedApprovers = new HashSet<>();

        try {
            for (Hash approverHash : ApproveeViewModel.load(tangle, transactionHash).getHashes()) {
                TransactionViewModel approver = TransactionViewModel.fromHash(tangle, approverHash);

                if (approver.snapshotIndex() > targetMilestone.index()) {
                    return true;
                } else if (approver.snapshotIndex() == 0) {
                    unconfirmedApprovers.add(approver);
                }
            }

            HashSet<Hash> processedTransactions = new HashSet<>();

            TransactionViewModel milestoneTransaction = TransactionViewModel.fromHash(tangle, targetMilestone.getHash());
            for (TransactionViewModel unconfirmedApprover : unconfirmedApprovers) {
                if (!isOrphaned(unconfirmedApprover, milestoneTransaction, processedTransactions)) {
                    return true;
                }
            }
        } catch (Exception e) {
            log.error("failed to determine the solid entry point status for transaction " + transactionHash, e);

            return true;
        }

        return false;
    }

    private Map<Hash, Integer> generateSolidEntryPoints(Snapshot snapshot, MilestoneViewModel targetMilestone) throws SnapshotException {
        HashMap<Hash, Integer> solidEntryPoints = new HashMap<>();

        // check the old solid entry points and copy them if they are still relevant
        ProgressLogger oldSolidEntryPointsProgressLogger = new ProgressLogger("Taking local snapshot [2/4 analyzing old solid entry points]", log).start(snapshot.getSolidEntryPoints().size());
        snapshot.getSolidEntryPoints().forEach((hash, milestoneIndex) -> {
            if (!Hash.NULL_HASH.equals(hash)) {
                if (targetMilestone.index() - milestoneIndex <= SOLID_ENTRY_POINT_LIFETIME &&
                        isSolidEntryPoint(hash, targetMilestone)) {

                    solidEntryPoints.put(hash, milestoneIndex);
                } else {
                    try {
                        // only clean up if the corresponding milestone transaction was cleaned up already -> otherwise let the MilestonePrunerJob do this
                        if (TransactionViewModel.fromHash(tangle, hash).getType() == TransactionViewModel.PREFILLED_SLOT
                                ) {

                            transactionPruner.addJob(new UnconfirmedSubtanglePrunerJob(hash));
                        }
                    } catch (Exception e) {
                        log.error("failed to add cleanup job to garbage collector", e);
                    }
                }
            }

            oldSolidEntryPointsProgressLogger.progress();
        });

        ProgressLogger progressLogger = new ProgressLogger("Taking local snapshot [3/4 generating solid entry points]", log);
        try {
            // add new solid entry points
            progressLogger.start(targetMilestone.index() - snapshotProvider.getInitialSnapshot().getIndex());
            MilestoneViewModel nextMilestone = targetMilestone;
            while (nextMilestone != null && nextMilestone.index() > snapshotProvider.getInitialSnapshot().getIndex() && progressLogger.getCurrentStep() < progressLogger.getStepCount()) {
                MilestoneViewModel currentMilestone = nextMilestone;
                DAGHelper.get(tangle).traverseApprovees(
                        currentMilestone.getHash(),
                        currentTransaction -> currentTransaction.snapshotIndex() >= currentMilestone.index(),
                        currentTransaction -> {
                            if (isSolidEntryPoint(currentTransaction.getHash(), targetMilestone)) {
                                solidEntryPoints.put(currentTransaction.getHash(), targetMilestone.index());
                            }
                        }
                );

                solidEntryPoints.put(currentMilestone.getHash(), targetMilestone.index());

                nextMilestone = MilestoneViewModel.findClosestPrevMilestone(tangle, currentMilestone.index());
                progressLogger.progress();
            }
            progressLogger.finish();
        } catch (Exception e) {
            progressLogger.abort(e);

            throw new SnapshotException("could not generate the solid entry points for " + targetMilestone, e);
        }

        solidEntryPoints.put(Hash.NULL_HASH, targetMilestone.index());

        return solidEntryPoints;
    }

    private Map<Hash, Integer> generateSeenMilestones(MilestoneViewModel targetMilestone) throws SnapshotException {
        ProgressLogger seenMilestonesProgressLogger = new ProgressLogger("Taking local snapshot [3/3 processing seen milestones]", log);
        HashMap<Hash, Integer> seenMilestones = new HashMap<>();

        seenMilestonesProgressLogger.start(configuration.getLocalSnapshotsDepth());
        try {
            MilestoneViewModel seenMilestone = targetMilestone;
            while ((seenMilestone = MilestoneViewModel.findClosestNextMilestone(tangle, seenMilestone.index())) != null) {
                seenMilestones.put(seenMilestone.getHash(), seenMilestone.index());
                seenMilestonesProgressLogger.progress();
            }
        } catch (Exception e) {
            seenMilestonesProgressLogger.abort(e);

            throw new SnapshotException("could not generate the set of seen milestones", e);
        }
        seenMilestonesProgressLogger.finish();

        return seenMilestones;
    }

    private Snapshot generateLocalSnapshot(MilestoneViewModel targetMilestone) throws SnapshotException {
        if (targetMilestone == null) {
            throw new SnapshotException("the target milestone must not be null");
        } else if (targetMilestone.index() > snapshotProvider.getLatestSnapshot().getIndex()) {
            throw new SnapshotException("the snapshot target " + targetMilestone + " was not solidified yet");
        } else if (targetMilestone.index() < snapshotProvider.getInitialSnapshot().getIndex()) {
            throw new SnapshotException("the snapshot target " + targetMilestone + " is too old");
        }

        snapshotProvider.getInitialSnapshot().lockRead();
        snapshotProvider.getLatestSnapshot().lockRead();

        SnapshotImpl snapshot;
        try {
            int distanceFromInitialSnapshot = Math.abs(snapshotProvider.getInitialSnapshot().getIndex() - targetMilestone.index());
            int distanceFromLatestSnapshot = Math.abs(snapshotProvider.getLatestSnapshot().getIndex() - targetMilestone.index());

            if (distanceFromInitialSnapshot <= distanceFromLatestSnapshot) {
                snapshot = new SnapshotImpl(snapshotProvider.getInitialSnapshot());

                snapshot.replayMilestones(targetMilestone.index(), tangle);
            } else {
                snapshot = new SnapshotImpl(snapshotProvider.getLatestSnapshot());

                snapshot.rollBackMilestones(targetMilestone.index() + 1, tangle);
            }
        } finally {
            snapshotProvider.getInitialSnapshot().unlockRead();
            snapshotProvider.getLatestSnapshot().unlockRead();
        }

        snapshot.setSolidEntryPoints(generateSolidEntryPoints(snapshot, targetMilestone));
        snapshot.setSeenMilestones(generateSeenMilestones(targetMilestone));

        return snapshot;
    }

    private void takeLocalSnapshot() throws SnapshotException {
        // load necessary configuration parameters
        String basePath = configuration.getLocalSnapshotsBasePath();
        int snapshotDepth = configuration.getLocalSnapshotsDepth();

        // determine our target milestone
        int targetMilestoneIndex = snapshotProvider.getLatestSnapshot().getIndex() - snapshotDepth;

        // try to load the milestone
        MilestoneViewModel targetMilestone;
        try {
            targetMilestone = MilestoneViewModel.findClosestPrevMilestone(tangle, targetMilestoneIndex);
        } catch (Exception e) {
            throw new SnapshotException("could not load the target milestone", e);
        }

        // if we couldn't find a milestone with the given index -> abort
        if (targetMilestone == null) {
            throw new SnapshotException("missing milestone with an index of " + targetMilestoneIndex + " or lower");
        }

        Snapshot targetSnapshot;
        try {
            targetSnapshot = generateLocalSnapshot(targetMilestone);
        } catch (Exception e) {
            throw new SnapshotException("could not generate the snapshot", e);
        }

        try {
            int targetIndex = targetMilestone.index() - configuration.getLocalSnapshotsPruningDelay();
            int startingIndex = configuration.getMilestoneStartIndex() + 1;

            if (targetIndex >= startingIndex) {
                transactionPruner.addJob(new MilestonePrunerJob(startingIndex, targetMilestone.index() - configuration.getLocalSnapshotsPruningDelay()));
            }

        } catch (TransactionPruningException e) {
            throw new SnapshotException("could not add the cleanup job to the garbage collector", e);
        }

        targetSnapshot.writeToDisk(basePath);

        snapshotProvider.getInitialSnapshot().update(targetSnapshot);
    }
}
