package com.iota.iri.service.snapshot.impl;

import com.iota.iri.conf.SnapshotConfig;
import com.iota.iri.controllers.ApproveeViewModel;
import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.service.snapshot.LocalSnapshotService;
import com.iota.iri.service.snapshot.Snapshot;
import com.iota.iri.service.snapshot.SnapshotException;
import com.iota.iri.service.snapshot.SnapshotProvider;
import com.iota.iri.service.transactionpruning.TransactionPruner;
import com.iota.iri.service.transactionpruning.TransactionPruningException;
import com.iota.iri.service.transactionpruning.jobs.MilestonePrunerJob;
import com.iota.iri.service.transactionpruning.jobs.UnconfirmedSubtanglePrunerJob;
import com.iota.iri.storage.Tangle;
import com.iota.iri.utils.ProgressLogger;
import com.iota.iri.utils.dag.DAGHelper;
import com.iota.iri.utils.dag.TraversalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public class LocalSnapshotServiceImpl implements LocalSnapshotService {
    /**
     * Logger for this class allowing us to dump debug and status messages.
     */
    private static final Logger log = LoggerFactory.getLogger(LocalSnapshotServiceImpl.class);

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
     * {@inheritDoc}
     */
    @Override
    public void takeLocalSnapshot(Tangle tangle, SnapshotProvider snapshotProvider, SnapshotConfig config, TransactionPruner transactionPruner) throws SnapshotException {
        // load necessary configuration parameters
        String basePath = config.getLocalSnapshotsBasePath();
        int snapshotDepth = config.getLocalSnapshotsDepth();

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

        Snapshot newSnapshot;
        try {
            newSnapshot = generateLocalSnapshot(tangle, snapshotProvider, config, targetMilestone);

            Map<Hash, Integer> oldSolidEntryPoints = snapshotProvider.getInitialSnapshot().getSolidEntryPoints();
            Map<Hash, Integer> newSolidEntryPoints = newSnapshot.getSolidEntryPoints();

            // clean up the deleted solid entry points
            oldSolidEntryPoints.forEach((transactionHash, milestoneIndex) -> {
                if (!newSolidEntryPoints.containsKey(transactionHash)) {
                    try {
                        System.out.println("CLEANUP SOLID ENTRY POINT1: " + transactionHash);
                        // only clean up if the corresponding milestone transaction was cleaned up already -> otherwise
                        // let the MilestonePrunerJob do this
                        if (TransactionViewModel.fromHash(tangle, transactionHash).getType() ==
                                TransactionViewModel.PREFILLED_SLOT) {
                            System.out.println("CLEANUP SOLID ENTRY POINT: " + transactionHash);
                            transactionPruner.addJob(new UnconfirmedSubtanglePrunerJob(transactionHash));
                        }
                    } catch (Exception e) {
                        log.error("failed to add cleanup job to garbage collector", e);
                    }
                }
            });
        } catch (Exception e) {
            throw new SnapshotException("could not generate the snapshot", e);
        }

        try {
            int targetIndex = targetMilestone.index() - config.getLocalSnapshotsPruningDelay();
            int startingIndex = config.getMilestoneStartIndex() + 1;

            if (targetIndex >= startingIndex) {
                transactionPruner.addJob(new MilestonePrunerJob(startingIndex, targetMilestone.index() - config.getLocalSnapshotsPruningDelay()));
            }

        } catch (TransactionPruningException e) {
            throw new SnapshotException("could not add the cleanup job to the garbage collector", e);
        }

        newSnapshot.writeToDisk(basePath);

        snapshotProvider.getInitialSnapshot().update(newSnapshot);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Snapshot generateLocalSnapshot(Tangle tangle, SnapshotProvider snapshotProvider, SnapshotConfig config,
            MilestoneViewModel targetMilestone) throws SnapshotException {

        if (targetMilestone == null) {
            throw new SnapshotException("the target milestone must not be null");
        } else if (targetMilestone.index() > snapshotProvider.getLatestSnapshot().getIndex()) {
            throw new SnapshotException("the snapshot target " + targetMilestone + " was not solidified yet");
        } else if (targetMilestone.index() < snapshotProvider.getInitialSnapshot().getIndex()) {
            throw new SnapshotException("the snapshot target " + targetMilestone + " is too old");
        }

        snapshotProvider.getInitialSnapshot().lockRead();
        snapshotProvider.getLatestSnapshot().lockRead();

        Snapshot snapshot;
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

        snapshot.setSolidEntryPoints(generateSolidEntryPoints(tangle, snapshotProvider, targetMilestone));
        snapshot.setSeenMilestones(generateSeenMilestones(tangle, config, targetMilestone));

        return snapshot;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<Hash, Integer> generateSolidEntryPoints(Tangle tangle, SnapshotProvider snapshotProvider, MilestoneViewModel targetMilestone) throws SnapshotException {
        HashMap<Hash, Integer> solidEntryPoints = new HashMap<>();

        processOldSolidEntryPoints(tangle, snapshotProvider, targetMilestone, solidEntryPoints);
        processNewSolidEntryPoints(tangle, snapshotProvider, targetMilestone, solidEntryPoints);

        solidEntryPoints.put(Hash.NULL_HASH, targetMilestone.index());

        return solidEntryPoints;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<Hash, Integer> generateSeenMilestones(Tangle tangle, SnapshotConfig config, MilestoneViewModel targetMilestone) throws SnapshotException {
        ProgressLogger seenMilestonesProgressLogger = new ProgressLogger("Taking local snapshot [3/3 processing seen milestones]", log);
        HashMap<Hash, Integer> seenMilestones = new HashMap<>();

        seenMilestonesProgressLogger.start(config.getLocalSnapshotsDepth());
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
    private boolean isOrphaned(Tangle tangle, TransactionViewModel transaction, TransactionViewModel referenceTransaction,
                               HashSet<Hash> processedTransactions) throws SnapshotException {

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
     * expensive check from {@link #isOrphaned(Tangle, TransactionViewModel, TransactionViewModel, HashSet)}.
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
    private boolean isSolidEntryPoint(Tangle tangle, Hash transactionHash, MilestoneViewModel targetMilestone) {
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
                if (!isOrphaned(tangle, unconfirmedApprover, milestoneTransaction, processedTransactions)) {
                    return true;
                }
            }
        } catch (Exception e) {
            log.error("failed to determine the solid entry point status for transaction " + transactionHash, e);

            return true;
        }

        return false;
    }

    private void processOldSolidEntryPoints(Tangle tangle, SnapshotProvider snapshotProvider, MilestoneViewModel targetMilestone, Map<Hash, Integer> solidEntryPoints) {
        ProgressLogger oldSolidEntryPointsProgressLogger = new ProgressLogger(
                "Taking local snapshot [2/4 analyzing old solid entry points]", log
        ).start(snapshotProvider.getInitialSnapshot().getSolidEntryPoints().size());

        snapshotProvider.getInitialSnapshot().getSolidEntryPoints().forEach((hash, milestoneIndex) -> {
            if (!Hash.NULL_HASH.equals(hash) && targetMilestone.index() - milestoneIndex <= SOLID_ENTRY_POINT_LIFETIME
                    && isSolidEntryPoint(tangle, hash, targetMilestone)) {

                solidEntryPoints.put(hash, milestoneIndex);
            }

            oldSolidEntryPointsProgressLogger.progress();
        });

        oldSolidEntryPointsProgressLogger.finish();
    }

    private void processNewSolidEntryPoints(Tangle tangle, SnapshotProvider snapshotProvider, MilestoneViewModel targetMilestone,
            Map<Hash, Integer> solidEntryPoints) throws SnapshotException {

        ProgressLogger progressLogger = new ProgressLogger(
                "Taking local snapshot [3/4 generating solid entry points]", log);
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
                            if (isSolidEntryPoint(tangle, currentTransaction.getHash(), targetMilestone)) {
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
    }
}
