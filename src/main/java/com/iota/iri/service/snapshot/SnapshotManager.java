package com.iota.iri.service.snapshot;

import com.iota.iri.MilestoneTracker;
import com.iota.iri.SignedFiles;
import com.iota.iri.conf.SnapshotConfig;
import com.iota.iri.controllers.*;
import com.iota.iri.model.Hash;
import com.iota.iri.storage.Tangle;
import com.iota.iri.utils.ProgressLogger;
import com.iota.iri.utils.dag.DAGHelper;
import com.iota.iri.utils.dag.TraversalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.iota.iri.MilestoneTracker.Status.INITIALIZED;

public class SnapshotManager {
    private static final Logger log = LoggerFactory.getLogger(SnapshotManager.class);

    private static Snapshot builtinSnapshot = null;

    private static final int OUTER_SHELL_SIZE = 50;

    /**
     * Maximum age in milestones since creation of solid entry points.
     *
     * Since it is possible to artificially keep old solid entry points alive by periodically attaching new transactions
     * to it, we limit the life time of solid entry points and ignore them whenever they become too old. This is a
     * measure against a potential attack vector of people trying to blow up the meta data of local snapshots.
     */
    private static final int SOLID_ENTRY_POINT_LIFETIME = 100;

    public static int GENERATE_FROM_INITIAL = 1;

    public static int GENERATE_FROM_LATEST = -1;

    public static String SNAPSHOT_PUBKEY = "TTXJUGKTNPOOEXSTQVVACENJOQUROXYKDRCVK9LHUXILCLABLGJTIPNF9REWHOIMEUKWQLUOKD9CZUYAC";

    public static int SNAPSHOT_PUBKEY_DEPTH = 6;

    public static int SNAPSHOT_INDEX = 6;

    public static int SPENT_ADDRESSES_INDEX = 7;

    private Tangle tangle;

    private GarbageCollector snapshotGarbageCollector;

    private TipsViewModel tipsViewModel;

    private SnapshotConfig configuration;

    private DAGHelper dagHelper;

    private Snapshot initialSnapshot;

    private Snapshot latestSnapshot;

    private boolean shuttingDown;

    private static int LOCAL_SNAPSHOT_RESCAN_INTERVAL = 10000;

    /**
     * This method is the constructor of the SnapshotManager.
     *
     * It stores the instances that this class depends on and tries to load the initial Snapshot, by first checking
     * if local snapshots are enabled and available and then falling back to the builtin Snapshot of the IRI.jar
     *
     * @param tangle wrapper for the database interface
     * @param configuration configuration of the node
     * @throws IOException if something goes wrong while processing the snapshot files
     */
    public SnapshotManager(Tangle tangle, TipsViewModel tipsViewModel, SnapshotConfig configuration) throws IOException {
        // save the necessary dependencies
        this.tangle = tangle;
        this.tipsViewModel = tipsViewModel;
        this.configuration = configuration;
        this.dagHelper = DAGHelper.get(tangle);

        // try to load a local snapshot first
        initialSnapshot = loadLocalSnapshot();

        // if we could not loaded a local snapshot -> fall back to the builtin one
        if(initialSnapshot == null) {
            initialSnapshot = loadBuiltInSnapshot();
        }

        // create a working copy of the initial snapshot that keeps track of the latest state
        latestSnapshot = initialSnapshot.clone();

        // initialize the snapshot garbage collector that takes care of cleaning up old transaction data
        snapshotGarbageCollector = new GarbageCollector(tangle, this, tipsViewModel);
    }

    public void init(MilestoneTracker milestoneTracker) {
        // if local snapshots are enabled we initialize the parts taking care of local snapshots
        if(configuration.getLocalSnapshotsEnabled()) {
            spawnMonitorThread(milestoneTracker);

            if(configuration.getLocalSnapshotsPruningEnabled()) {
                snapshotGarbageCollector.start();
            }
        }
    }

    public void spawnMonitorThread(MilestoneTracker milestoneTracker) {
        (new Thread(() -> {
            log.info("Local Snapshot Monitor started ...");

            // load necessary configuration parameters
            int snapshotDepth = configuration.getLocalSnapshotsDepth();

            while(!shuttingDown) {
                long scanStart = System.currentTimeMillis();

                int LOCAL_SNAPSHOT_INTERVAL = milestoneTracker.getStatus() == INITIALIZED && latestSnapshot.getIndex() == milestoneTracker.latestMilestoneIndex
                                              ? configuration.getLocalSnapshotsIntervalSynced()
                                              : configuration.getLocalSnapshotsIntervalUnsynced();

                if(latestSnapshot.getIndex() - initialSnapshot.getIndex() > snapshotDepth + LOCAL_SNAPSHOT_INTERVAL) {
                    try {
                        takeLocalSnapshot();
                    } catch(SnapshotException e) {
                        log.error("Error while taking local snapshot: " + e.getMessage());
                    }
                }

                try {
                    Thread.sleep(Math.max(1, LOCAL_SNAPSHOT_RESCAN_INTERVAL - (System.currentTimeMillis() - scanStart)));
                } catch(InterruptedException e) {
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
     * This method resets the SnapshotManager and sets the latestSnapshot value back to its starting point.
     *
     * This can be used to recover from errors if the state of the Snapshot ever becomes corrupted (due to syncing or
     * processing errors).
     */
    public void resetLatestSnapshot() {
        latestSnapshot = initialSnapshot.clone();
    }

    public void calculateSnapshotState(Snapshot snapshot, MilestoneViewModel currentMilestone, int generationMode) throws SnapshotException {
        // retrieve the balance diff from the db
        StateDiffViewModel stateDiffViewModel;
        try {
            stateDiffViewModel = StateDiffViewModel.load(tangle, currentMilestone.getHash());
        } catch(Exception e) {
            throw new SnapshotException("could not retrieve the StateDiff for " + currentMilestone.toString(), e);
        }

        // if we have a diff apply it (the values get multiplied by the generationMode to reflect the direction)
        if(stateDiffViewModel != null && !stateDiffViewModel.isEmpty()) {
            // create the SnapshotStateDiff object for our changes
            SnapshotStateDiff snapshotStateDiff = new SnapshotStateDiff(
            stateDiffViewModel.getDiff().entrySet().stream().map(
            hashLongEntry -> new HashMap.SimpleEntry<>(
            hashLongEntry.getKey(), generationMode * hashLongEntry.getValue()
            )
            ).collect(
            Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)
            )
            );

            // this should never happen since we check the StateDiffs already when applying them in the
            // MilestoneTracker but better give a reasonable error message if it ever does
            if(!snapshotStateDiff.isConsistent()) {
                throw new SnapshotException("the StateDiff belonging to " + currentMilestone.toString() + " is inconsistent");
            }

            // apply the balance changes to the snapshot
            snapshot.update(
            snapshotStateDiff,
            currentMilestone.index(),
            currentMilestone.getHash()
            );

            // this should never happen since we check the snapshots already when applying them but better give
            // a reasonable error message if it ever does
            if(!snapshot.hasCorrectSupply() || !snapshot.isConsistent()) {
                throw new SnapshotException("the StateDiff belonging to " + currentMilestone.toString() +" leads to an invalid supply");
            }
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
     * @throws TraversalException if anything goes wrong while traversing the graph
     */
    private boolean isOrphaned(TransactionViewModel transaction, TransactionViewModel referenceTransaction) throws TraversalException {
        if(transaction.getArrivalTime() > referenceTransaction.getTimestamp()) {
            return false;
        }

        AtomicBoolean nonOrphanedTransactionFound = new AtomicBoolean(false);
        dagHelper.traverseApprovers(
            transaction.getHash(),
            currentTransaction -> !nonOrphanedTransactionFound.get(),
            currentTransaction -> {
                if(currentTransaction.getArrivalTime() > referenceTransaction.getTimestamp()) {
                    nonOrphanedTransactionFound.set(true);
                }
            }
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
     * expensive check from {@link #isOrphaned(TransactionViewModel, TransactionViewModel)}.
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

                if(approver.snapshotIndex() > targetMilestone.index()) {
                    return true;
                } else if(approver.snapshotIndex() == 0) {
                    unconfirmedApprovers.add(approver);
                }
            }

            TransactionViewModel milestoneTransaction = TransactionViewModel.fromHash(tangle, targetMilestone.getHash());
            for (TransactionViewModel unconfirmedApprover : unconfirmedApprovers) {
                if(!isOrphaned(unconfirmedApprover, milestoneTransaction)) {
                    return true;
                }
            }
        } catch(Exception e) {
            log.error("failed to determine the solid entry point status for transaction " + transactionHash, e);

            return true;
        }

        return false;
    }

    private HashMap<Hash, Integer> generateSolidEntryPoints(Snapshot snapshot, MilestoneViewModel targetMilestone) throws SnapshotException {
        ProgressLogger progressLogger = new ProgressLogger("Taking local snapshot [2/3 generating solid entry points]", log);
        HashMap<Hash, Integer> solidEntryPoints = new HashMap<>();

        // check the old solid entry points and copy them if they are still relevant
        snapshot.getSolidEntryPoints().entrySet().stream().forEach(solidEntryPoint -> {
            if(targetMilestone.index() - solidEntryPoint.getValue() <= SOLID_ENTRY_POINT_LIFETIME && isSolidEntryPoint(solidEntryPoint.getKey(), targetMilestone)) {
                solidEntryPoints.put(solidEntryPoint.getKey(), solidEntryPoint.getValue());
            }
        });

        try {
            // add new solid entry points
            progressLogger.start(Math.min(OUTER_SHELL_SIZE, targetMilestone.index() - initialSnapshot.getIndex()));
            MilestoneViewModel nextMilestone = targetMilestone;
            while(nextMilestone != null && nextMilestone.index() > initialSnapshot.getIndex() && progressLogger.getCurrentStep() < progressLogger.getStepCount()) {
                MilestoneViewModel currentMilestone = nextMilestone;
                dagHelper.traverseApprovees(
                    currentMilestone.getHash(),
                    currentTransaction -> currentTransaction.snapshotIndex() >= currentMilestone.index(),
                    currentTransaction -> {
                        if(isSolidEntryPoint(currentTransaction.getHash(), targetMilestone)) {
                            solidEntryPoints.put(currentTransaction.getHash(), targetMilestone.index());
                        }
                    }
                );

                nextMilestone = MilestoneViewModel.findClosestPrevMilestone(tangle, currentMilestone.index());
                progressLogger.progress();
            }
            progressLogger.finish();
        } catch(Exception e) {
            progressLogger.abort(e);

            throw new SnapshotException("could not generate the solid entry points for " + targetMilestone, e);
        }

        solidEntryPoints.put(Hash.NULL_HASH, targetMilestone.index());

        return solidEntryPoints;
    }

    public Snapshot generateSnapshot(MilestoneViewModel targetMilestone) throws SnapshotException {
        // variables used by the snapshot generation process
        Snapshot snapshot;
        int generationMode;

        // variables to keep track of the progress of the tasks in this method
        int amountOfMilestonesToProcess;
        int stepCounter;

        //region SANITIZE PARAMETERS AND DETERMINE WHICH SNAPSHOT TO WORK FROM /////////////////////////////////////

        // acquire locks for our snapshots
        initialSnapshot.lockRead();
        latestSnapshot.lockRead();

        // handle the following block in a try to be able to always unlock the snapshots
        try {
            // check if the milestone is not null
            if(targetMilestone == null) {
                throw new SnapshotException("the target milestone must not be null");
            }

            // check if the milestone was solidified already
            if(targetMilestone.index() > latestSnapshot.getIndex()) {
                throw new SnapshotException("the target " + targetMilestone + " was not solidified yet");
            }

            // check if the milestone came after our initial one
            if(targetMilestone.index() < initialSnapshot.getIndex()) {
                throw new SnapshotException("the target " + targetMilestone.toString() + " is too old");
            }

            // determine the distance of our target snapshot from our two snapshots (initial / latest)
            int distanceFromInitialSnapshot = Math.abs(initialSnapshot.getIndex() - targetMilestone.index());
            int distanceFromLatestSnapshot = Math.abs(latestSnapshot.getIndex() - targetMilestone.index());

            // determine which generation mode is the fastest one
            generationMode = distanceFromInitialSnapshot <= distanceFromLatestSnapshot
                             ? GENERATE_FROM_INITIAL
                             : GENERATE_FROM_LATEST;

            // store how many milestones has to be processed to generate the ledger state (for reasonable debug messages)
            amountOfMilestonesToProcess = generationMode == GENERATE_FROM_INITIAL
                                          ? distanceFromInitialSnapshot
                                          : distanceFromLatestSnapshot;

            // clone the corresponding snapshot state
            snapshot = generationMode == GENERATE_FROM_INITIAL
                       ? initialSnapshot.clone()
                       : latestSnapshot.clone();
        } finally {
            // unlock our snapshots
            initialSnapshot.unlockRead();
            latestSnapshot.unlockRead();
        }

        //endregion ////////////////////////////////////////////////////////////////////////////////////////////////////

        // if the target snapshot is our starting point we can return immediately
        if(targetMilestone.index() == snapshot.getIndex()) {
            return snapshot;
        }

        //region GENERATE THE SNAPSHOT STATE ///////////////////////////////////////////////////////////////////////////

        // calculate the starting point for our snapshot generation
        int startingMilestoneIndex = snapshot.getIndex() + (generationMode == GENERATE_FROM_INITIAL ? 1 : 0);

        // retrieve the first milestone for our snapshot generation
        MilestoneViewModel startingMilestone;
        try {
            startingMilestone = MilestoneViewModel.get(tangle, startingMilestoneIndex);
        } catch(Exception e) {
            throw new SnapshotException("could not retrieve the milestone #" + startingMilestoneIndex, e);
        }
        if(startingMilestone == null) {
            throw new SnapshotException("could not retrieve the milestone #" + startingMilestoneIndex);
        }

        // dump a progress message before we start
        dumpLogMessage("Taking local snapshot", "1/3 calculating new snapshot state", stepCounter = 0, amountOfMilestonesToProcess);

        // iterate through the milestones to our target
        while(generationMode == GENERATE_FROM_INITIAL ? startingMilestone.index() <= targetMilestone.index() : startingMilestone.index() > targetMilestone.index()) {
            // calculate the correct ledger state based on our current milestone
            calculateSnapshotState(snapshot, startingMilestone, generationMode);

            // retrieve the next milestone
            MilestoneViewModel nextMilestone;
            try {
                nextMilestone = generationMode == GENERATE_FROM_INITIAL
                                ? MilestoneViewModel.findClosestNextMilestone(tangle, startingMilestone.index())
                                : MilestoneViewModel.findClosestPrevMilestone(tangle, startingMilestone.index());
            } catch(Exception e) {
                throw new SnapshotException("could not iterate to the next milestone from " + startingMilestone.toString(), e);
            }
            if(nextMilestone == null) {
                throw new SnapshotException("could not iterate to the next milestone from " + startingMilestone.toString());
            }

            // iterate to the the next milestone
            startingMilestone = nextMilestone;

            // dump a progress message after every step
            dumpLogMessage("Taking local snapshot", "1/3 calculating new snapshot state", ++stepCounter, amountOfMilestonesToProcess);
        }

        //endregion ////////////////////////////////////////////////////////////////////////////////////////////////////

        //region ANALYZE OLD TRANSACTIONS THAT CAN BE PRUNED //////////////////////////////////////////////////////////

        HashMap<Hash, Integer> solidEntryPoints = generateSolidEntryPoints(snapshot, targetMilestone);
        System.out.println("SOLID ENTRY POINTS: " + solidEntryPoints.size());

        //endregion ////////////////////////////////////////////////////////////////////////////////////////////////////

        //region GENERATE THE LIST OF SEEN MILESTONES //////////////////////////////////////////////////////////////////

        ProgressLogger seenMilestonesProgressLogger = new ProgressLogger("Taking local snapshot [3/3 processing seen milestones]", log);

        seenMilestonesProgressLogger.start(configuration.getLocalSnapshotsDepth());
        HashMap<Hash, Integer> seenMilestones = new HashMap<>();
        try {
            MilestoneViewModel seenMilestone = targetMilestone;
            while((seenMilestone = MilestoneViewModel.findClosestNextMilestone(tangle, seenMilestone.index())) != null) {
                seenMilestones.put(seenMilestone.getHash(), seenMilestone.index());
                seenMilestonesProgressLogger.progress();
            }
        } catch(Exception e) {
            seenMilestonesProgressLogger.abort(e);

            throw new SnapshotException("could not generate the set of seen milestones", e);
        }
        seenMilestonesProgressLogger.finish();

        //endregion ////////////////////////////////////////////////////////////////////////////////////////////////////

        //region UPDATE THE SCALAR METADATA VALUES OF THE NEW SNAPSHOT  ////////////////////////////////////////////////

        // retrieve the transaction belonging to our targetMilestone
        TransactionViewModel targetMilestoneTransaction;
        try {
            targetMilestoneTransaction = TransactionViewModel.fromHash(tangle, targetMilestone.getHash());
        } catch(Exception e) {
            throw new SnapshotException("could not retrieve the transaction belonging to " + targetMilestone.toString(), e);
        }
        if(targetMilestoneTransaction == null) {
            throw new SnapshotException("could not retrieve the transaction belonging to " + targetMilestone.toString());
        }

        // set the snapshot index, timestamp and solid entry points to that of our target milestone transaction
        snapshot.setIndex(targetMilestone.index());
        snapshot.setTimestamp(targetMilestoneTransaction.getTimestamp());
        snapshot.setSolidEntryPoints(solidEntryPoints);
        snapshot.setHash(targetMilestone.getHash());
        snapshot.setSeenMilestones(seenMilestones);

        //endregion ////////////////////////////////////////////////////////////////////////////////////////////////////

        // return the result
        return snapshot;
    }

    long lastDumpTime = System.currentTimeMillis();

    String lastLogMessage;

    public void dumpLogMessage(String job, String task, int currentStep, int maxSteps) {
        String logMessage = job + " [" + task + "]: " + (int) (((double) currentStep / (double) maxSteps) * 100) + "% ...";

        if(!logMessage.equals(lastLogMessage) && (System.currentTimeMillis() - lastDumpTime >= 5000 || currentStep == 1 || currentStep == maxSteps)) {
            log.info(logMessage);

            lastLogMessage = logMessage;
            lastDumpTime = System.currentTimeMillis();
        }
    }

    public Snapshot loadLocalSnapshot() {
        try {
            // load necessary configuration parameters
            boolean localSnapshotsEnabled = configuration.getLocalSnapshotsEnabled();

            // if local snapshots are enabled
            if(localSnapshotsEnabled) {
                // load the remaining configuration parameters
                String basePath = configuration.getLocalSnapshotsBasePath();

                // create a file handle for our snapshot file
                File localSnapshotFile = new File(basePath + ".snapshot.state");

                // create a file handle for our snapshot metadata file
                File localSnapshotMetadDataFile = new File(basePath + ".snapshot.meta");

                // if the local snapshot files exists -> load them
                if(
                localSnapshotFile.exists() &&
                localSnapshotFile.isFile() &&
                localSnapshotMetadDataFile.exists() &&
                localSnapshotMetadDataFile.isFile()
                ) {
                    // retrieve the state to our local snapshot
                    SnapshotState snapshotState = SnapshotState.fromFile(localSnapshotFile.getAbsolutePath());

                    // check the supply of the snapshot state
                    if(!snapshotState.hasCorrectSupply()) {
                        throw new IllegalStateException("the snapshot state file has an invalid supply");
                    }

                    // check the consistency of the snapshot state
                    if(!snapshotState.isConsistent()) {
                        throw new IllegalStateException("the snapshot state file is not consistent");
                    }

                    // retrieve the meta data to our local snapshot
                    SnapshotMetaData snapshotMetaData = SnapshotMetaData.fromFile(localSnapshotMetadDataFile);

                    log.info("Resumed from local snapshot #" + snapshotMetaData.getIndex() + " ...");

                    // return our Snapshot
                    return new Snapshot(snapshotState, snapshotMetaData);
                }
            }
        } catch(Exception e) {
            log.info("No valid Local Snapshot file found");
        }

        // otherwise just return null
        return null;
    }

    public Snapshot loadBuiltInSnapshot() throws IOException, IllegalStateException {
        if (builtinSnapshot == null) {
            // read the config vars for the built in snapshot files
            boolean testnet = configuration.isTestnet();
            String snapshotPath = configuration.getSnapshotFile();
            String snapshotSigPath = configuration.getSnapshotSignatureFile();
            int milestoneStartIndex = configuration.getMilestoneStartIndex();

            // verify the signature of the builtin snapshot file
            if(!testnet && !SignedFiles.isFileSignatureValid(
            snapshotPath,
            snapshotSigPath,
            SNAPSHOT_PUBKEY,
            SNAPSHOT_PUBKEY_DEPTH,
            SNAPSHOT_INDEX
            )) {
                throw new IllegalStateException("the snapshot signature is invalid");
            }

            // restore the snapshot state from its file
            SnapshotState snapshotState = SnapshotState.fromFile(snapshotPath);

            // check the supply of the snapshot state
            if(!snapshotState.hasCorrectSupply()) {
                throw new IllegalStateException("the snapshot state file has an invalid supply");
            }

            // check the consistency of the snaphot state
            if(!snapshotState.isConsistent()) {
                throw new IllegalStateException("the snapshot state file is not consistent");
            }

            // create solid entry points
            HashMap<Hash, Integer> solidEntryPoints = new HashMap<>();
            solidEntryPoints.put(Hash.NULL_HASH, milestoneStartIndex);

            // return our snapshot
            builtinSnapshot = new Snapshot(
            snapshotState,
            new SnapshotMetaData(
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

    public Snapshot takeLocalSnapshot() throws SnapshotException {
        // load necessary configuration parameters
        boolean testnet = configuration.isTestnet();
        String basePath = configuration.getLocalSnapshotsBasePath();
        int snapshotDepth = configuration.getLocalSnapshotsDepth();

        // determine our target milestone
        int targetMilestoneIndex = latestSnapshot.getIndex() - snapshotDepth;

        // try to load the milestone
        MilestoneViewModel targetMilestone = null;
        try {
            targetMilestone = MilestoneViewModel.findClosestPrevMilestone(tangle, targetMilestoneIndex);
        } catch(Exception e) {
            throw new SnapshotException("could not load the target milestone", e);
        }

        // if we couldn't find a milestone with the given index -> abort
        if(targetMilestone == null) {
            throw new SnapshotException("missing milestone with an index of " + targetMilestoneIndex + " or lower");
        }

        Snapshot targetSnapshot = null;
        try {
            targetSnapshot = generateSnapshot(targetMilestone);
        } catch(Exception e) {
            throw new SnapshotException("could not generate the snapshot");
        }

        snapshotGarbageCollector.addCleanupJob(targetMilestone.index() - configuration.getLocalSnapshotsPruningDelay());

        try {
            targetSnapshot.state.writeFile(basePath + ".snapshot.state");
            targetSnapshot.metaData.writeFile(basePath + ".snapshot.meta");
        } catch(IOException e) {
            throw new SnapshotException("could not write local snapshot files", e);
        }

        initialSnapshot = targetSnapshot;

        return targetSnapshot;
    }
}
