package com.iota.iri.service.snapshot.impl;

import com.iota.iri.MilestoneTracker;
import com.iota.iri.SignedFiles;
import com.iota.iri.conf.SnapshotConfig;
import com.iota.iri.controllers.*;
import com.iota.iri.model.Hash;
import com.iota.iri.service.snapshot.SnapshotException;
import com.iota.iri.service.snapshot.SnapshotState;
import com.iota.iri.service.transactionpruning.TransactionPruner;
import com.iota.iri.service.transactionpruning.jobs.MilestonePrunerJob;
import com.iota.iri.service.transactionpruning.async.AsyncTransactionPruner;
import com.iota.iri.service.transactionpruning.TransactionPruningException;
import com.iota.iri.service.transactionpruning.jobs.UnconfirmedSubtanglePrunerJob;
import com.iota.iri.storage.Tangle;
import com.iota.iri.utils.ProgressLogger;
import com.iota.iri.utils.dag.DAGHelper;
import com.iota.iri.utils.dag.TraversalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.iota.iri.MilestoneTracker.Status.INITIALIZED;

public class SnapshotManager {
    /**
     * Time in seconds that we wait for orphaned transactions to consider them orphaned.
     */
    private static final int ORPHANED_TRANSACTION_GRACE_TIME = 3600;
    /**
     * Logger for this class allowing us to dump debug and status messages.
     */
    private static final Logger log = LoggerFactory.getLogger(SnapshotManager.class);

    private static Snapshot builtinSnapshot = null;

    /**
     * Maximum age in milestones since creation of solid entry points.
     *
     * Since it is possible to artificially keep old solid entry points alive by periodically attaching new transactions
     * to it, we limit the life time of solid entry points and ignore them whenever they become too old. This is a
     * measure against a potential attack vector of people trying to blow up the meta data of local snapshots.
     */
    private static final int SOLID_ENTRY_POINT_LIFETIME = 20000;

    private static final String SNAPSHOT_PUBKEY = "TTXJUGKTNPOOEXSTQVVACENJOQUROXYKDRCVK9LHUXILCLABLGJTIPNF9REWHOIMEUKWQLUOKD9CZUYAC";

    private static final int SNAPSHOT_PUBKEY_DEPTH = 6;

    private static final int SNAPSHOT_INDEX = 9;

    public static final int SPENT_ADDRESSES_INDEX = 7;

    private Tangle tangle;

    private TransactionPruner transactionPruner;

    private SnapshotConfig configuration;

    private DAGHelper dagHelper;

    private Snapshot initialSnapshot;

    private Snapshot latestSnapshot;

    private boolean shuttingDown;

    private static final int LOCAL_SNAPSHOT_RESCAN_INTERVAL = 10000;

    /**
     * This method is the constructor of the SnapshotManager.
     *
     * It stores the instances that this class depends on and tries to load the initial Snapshot, by first checking if
     * local snapshots are enabled and available and then falling back to the builtin Snapshot of the IRI.jar
     *
     * @param tangle wrapper for the database interface
     * @param configuration configuration of the node
     * @throws IOException if something goes wrong while processing the snapshot files
     */
    public SnapshotManager(Tangle tangle, TipsViewModel tipsViewModel, SnapshotConfig configuration) throws IOException {
        // save the necessary dependencies
        this.tangle = tangle;
        this.configuration = configuration;
        this.dagHelper = DAGHelper.get(tangle);

        // try to load a local snapshot first
        initialSnapshot = loadLocalSnapshot();

        // if we could not loaded a local snapshot -> fall back to the builtin one
        if (initialSnapshot == null) {
            initialSnapshot = loadBuiltInSnapshot();
        }

        // create a working copy of the initial snapshot that keeps track of the latest state
        latestSnapshot = initialSnapshot.clone();

        // initialize the snapshot garbage collector that takes care of cleaning up old transaction data
        transactionPruner = new AsyncTransactionPruner(tangle, tipsViewModel, getInitialSnapshot(), configuration);
        try {
            transactionPruner.restoreState();
        } catch (TransactionPruningException e) {
            log.info("could not restore the state of the TransactionPruner", e);
        }
    }

    public void init(MilestoneTracker milestoneTracker) {
        // if local snapshots are enabled we initialize the parts taking care of local snapshots
        if (configuration.getLocalSnapshotsEnabled()) {
            spawnMonitorThread(milestoneTracker);

            if (configuration.getLocalSnapshotsPruningEnabled()) {
                ((AsyncTransactionPruner) transactionPruner).start();
            }
        }
    }

    private void spawnMonitorThread(MilestoneTracker milestoneTracker) {
        (new Thread(() -> {
            log.info("Local Snapshot Monitor started ...");

            // load necessary configuration parameters
            int snapshotDepth = configuration.getLocalSnapshotsDepth();

            while (!shuttingDown) {
                long scanStart = System.currentTimeMillis();

                int localSnapshotInterval = milestoneTracker.getStatus() == INITIALIZED && latestSnapshot.getIndex() == milestoneTracker.latestMilestoneIndex
                        ? configuration.getLocalSnapshotsIntervalSynced()
                        : configuration.getLocalSnapshotsIntervalUnsynced();

                if (latestSnapshot.getIndex() - initialSnapshot.getIndex() > snapshotDepth + localSnapshotInterval) {
                    try {
                        takeLocalSnapshot();
                    } catch (SnapshotException e) {
                        log.error("error while taking local snapshot", e);
                    }
                }

                try {
                    Thread.sleep(Math.max(1, LOCAL_SNAPSHOT_RESCAN_INTERVAL - (System.currentTimeMillis() - scanStart)));
                } catch (InterruptedException e) {
                    log.info("Local Snapshot Monitor stopped ...");

                    shuttingDown = true;
                }
            }
        }, "Local Snapshot Monitor")).start();
    }

    public void shutDown() {
        shuttingDown = true;
        initialSnapshot = null;
        latestSnapshot = null;

        ((AsyncTransactionPruner) transactionPruner).shutdown();
    }

    public SnapshotConfig getConfiguration() {
        return configuration;
    }

    /**
     * This is the getter of the initialSnapshot property.
     *
     * It simply returns the stored private property.
     *
     * @return the Snapshot that the node was initialized with
     */
    public Snapshot getInitialSnapshot() {
        return initialSnapshot;
    }

    /**
     * This is the getter of the latestSnapshot property.
     *
     * It simply returns the stored private property.
     *
     * @return the Snapshot that represents the most recent "confirmed" state of the ledger
     */
    public Snapshot getLatestSnapshot() {
        return latestSnapshot;
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
     * @throws TraversalException if anything goes wrong while traversing the graph
     */
    private boolean isOrphaned(TransactionViewModel transaction, TransactionViewModel referenceTransaction, HashSet<Hash> processedTransactions) throws TraversalException {
        long timeDiff = (referenceTransaction.getArrivalTime() / 1000L) - referenceTransaction.getTimestamp();

        if (((transaction.getArrivalTime() / 1000L) + ORPHANED_TRANSACTION_GRACE_TIME - timeDiff) > referenceTransaction.getTimestamp()) {
            return false;
        }

        AtomicBoolean nonOrphanedTransactionFound = new AtomicBoolean(false);
        dagHelper.traverseApprovers(
                transaction.getHash(),
                currentTransaction -> !nonOrphanedTransactionFound.get(),
                currentTransaction -> {
                    if (((currentTransaction.getArrivalTime() / 1000L) + ORPHANED_TRANSACTION_GRACE_TIME - timeDiff) > referenceTransaction.getTimestamp()) {
                        nonOrphanedTransactionFound.set(true);
                    }
                },
                processedTransactions
        );

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

    private HashMap<Hash, Integer> generateSolidEntryPoints(Snapshot snapshot, MilestoneViewModel targetMilestone) throws SnapshotException {
        HashMap<Hash, Integer> solidEntryPoints = new HashMap<>();

        // check the old solid entry points and copy them if they are still relevant
        ProgressLogger oldSolidEntryPointsProgressLogger = new ProgressLogger("Taking local snapshot [2/4 analyzing old solid entry points]", log).start(snapshot.getSolidEntryPoints().size());
        snapshot.getSolidEntryPoints().forEach((hash, milestoneIndex) -> {
            if (!Hash.NULL_HASH.equals(hash)) {
                if (
                        targetMilestone.index() - milestoneIndex <= SOLID_ENTRY_POINT_LIFETIME &&
                                isSolidEntryPoint(hash, targetMilestone)
                        ) {
                    solidEntryPoints.put(hash, milestoneIndex);
                } else {
                    try {
                        // only clean up if the corresponding milestone transaction was cleaned up already -> otherwise let the MilestonePrunerJob do this
                        if (
                                TransactionViewModel.fromHash(tangle, hash).getType() == TransactionViewModel.PREFILLED_SLOT
                                ) {
                            transactionPruner.addJob(new UnconfirmedSubtanglePrunerJob(hash));
                        }
                    } catch (TransactionPruningException e) {
                        log.error("could not add cleanup job to garbage collector", e);
                    } catch (Exception e) {

                    }
                }
            }

            oldSolidEntryPointsProgressLogger.progress();
        });

        ProgressLogger progressLogger = new ProgressLogger("Taking local snapshot [3/4 generating solid entry points]", log);
        try {
            // add new solid entry points
            progressLogger.start(targetMilestone.index() - initialSnapshot.getIndex());
            MilestoneViewModel nextMilestone = targetMilestone;
            while (nextMilestone != null && nextMilestone.index() > initialSnapshot.getIndex() && progressLogger.getCurrentStep() < progressLogger.getStepCount()) {
                MilestoneViewModel currentMilestone = nextMilestone;
                dagHelper.traverseApprovees(
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

    private HashMap<Hash, Integer> generateSeenMilestones(MilestoneViewModel targetMilestone) throws SnapshotException {
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

    private Snapshot generateSnapshot(MilestoneViewModel targetMilestone) throws SnapshotException {
        if (targetMilestone == null) {
            throw new SnapshotException("the target milestone must not be null");
        } else if (targetMilestone.index() > latestSnapshot.getIndex()) {
            throw new SnapshotException("the snapshot target " + targetMilestone + " was not solidified yet");
        } else if (targetMilestone.index() < initialSnapshot.getIndex()) {
            throw new SnapshotException("the snapshot target " + targetMilestone + " is too old");
        }

        initialSnapshot.lockRead();
        latestSnapshot.lockRead();

        Snapshot snapshot;
        try {
            int distanceFromInitialSnapshot = Math.abs(initialSnapshot.getIndex() - targetMilestone.index());
            int distanceFromLatestSnapshot = Math.abs(latestSnapshot.getIndex() - targetMilestone.index());

            if (distanceFromInitialSnapshot <= distanceFromLatestSnapshot) {
                snapshot = initialSnapshot.clone();

                snapshot.replayMilestones(targetMilestone.index(), tangle);
            } else {
                snapshot = latestSnapshot.clone();

                snapshot.rollBackMilestones(targetMilestone.index() + 1, tangle);
            }
        } finally {
            initialSnapshot.unlockRead();
            latestSnapshot.unlockRead();
        }

        snapshot.setSolidEntryPoints(generateSolidEntryPoints(snapshot, targetMilestone));
        snapshot.setSeenMilestones(generateSeenMilestones(targetMilestone));

        return snapshot;
    }

    private Snapshot loadLocalSnapshot() {
        try {
            // if local snapshots are enabled
            if (configuration.getLocalSnapshotsEnabled()) {
                // load the remaining configuration parameters
                String basePath = configuration.getLocalSnapshotsBasePath();

                // create a file handle for our snapshot file
                File localSnapshotFile = new File(basePath + ".snapshot.state");

                // create a file handle for our snapshot metadata file
                File localSnapshotMetadDataFile = new File(basePath + ".snapshot.meta");

                // if the local snapshot files exists -> load them
                if (
                        localSnapshotFile.exists() &&
                                localSnapshotFile.isFile() &&
                                localSnapshotMetadDataFile.exists() &&
                                localSnapshotMetadDataFile.isFile()
                        ) {
                    // retrieve the state to our local snapshot
                    SnapshotStateImpl snapshotState = SnapshotStateImpl.fromFile(localSnapshotFile.getAbsolutePath());

                    // check the supply of the snapshot state
                    if (!snapshotState.hasCorrectSupply()) {
                        throw new IllegalStateException("the snapshot state file has an invalid supply");
                    }

                    // check the consistency of the snapshot state
                    if (!snapshotState.isConsistent()) {
                        throw new IllegalStateException("the snapshot state file is not consistent");
                    }

                    // retrieve the meta data to our local snapshot
                    SnapshotMetaDataImpl snapshotMetaData = SnapshotMetaDataImpl.fromFile(localSnapshotMetadDataFile);

                    log.info("Resumed from local snapshot #" + snapshotMetaData.getIndex() + " ...");

                    // return our Snapshot
                    return new Snapshot(snapshotState, snapshotMetaData);
                }
            }
        } catch (Exception e) {
            log.info("No valid Local Snapshot file found");
        }

        // otherwise just return null
        return null;
    }

    private Snapshot loadBuiltInSnapshot() throws SnapshotException {
        if (builtinSnapshot == null) {
            // read the config vars for the built in snapshot files
            boolean testnet = configuration.isTestnet();
            String snapshotPath = configuration.getSnapshotFile();
            String snapshotSigPath = configuration.getSnapshotSignatureFile();
            int milestoneStartIndex = configuration.getMilestoneStartIndex();

            // verify the signature of the builtin snapshot file
            if (!testnet && !SignedFiles.isFileSignatureValid(
                    snapshotPath,
                    snapshotSigPath,
                    SNAPSHOT_PUBKEY,
                    SNAPSHOT_PUBKEY_DEPTH,
                    SNAPSHOT_INDEX
            )) {
                throw new SnapshotException("the snapshot signature is invalid");
            }

            // restore the snapshot state from its file
            SnapshotState snapshotState = SnapshotStateImpl.fromFile(snapshotPath);

            // check the supply of the snapshot state
            if (!snapshotState.hasCorrectSupply()) {
                throw new IllegalStateException("the snapshot state file has an invalid supply");
            }

            // check the consistency of the snaphot state
            if (!snapshotState.isConsistent()) {
                throw new IllegalStateException("the snapshot state file is not consistent");
            }

            // create solid entry points
            HashMap<Hash, Integer> solidEntryPoints = new HashMap<>();
            solidEntryPoints.put(Hash.NULL_HASH, milestoneStartIndex);

            // return our snapshot
            builtinSnapshot = new Snapshot(
                    snapshotState,
                    new SnapshotMetaDataImpl(
                            Hash.NULL_HASH,
                            milestoneStartIndex,
                            configuration.getSnapshotTime(),
                            solidEntryPoints,
                            new HashMap<>()
                    )
            );
        }

        return builtinSnapshot.clone();
    }

    private void takeLocalSnapshot() throws SnapshotException {
        // load necessary configuration parameters
        String basePath = configuration.getLocalSnapshotsBasePath();
        int snapshotDepth = configuration.getLocalSnapshotsDepth();

        // determine our target milestone
        int targetMilestoneIndex = latestSnapshot.getIndex() - snapshotDepth;

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
            targetSnapshot = generateSnapshot(targetMilestone);
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

        try {
            targetSnapshot.state.writeFile(basePath + ".snapshot.state");
            targetSnapshot.metaData.writeFile(basePath + ".snapshot.meta");
        } catch (IOException e) {
            throw new SnapshotException("could not write local snapshot files", e);
        }

        initialSnapshot.update(targetSnapshot);
    }
}
