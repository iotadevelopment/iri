package com.iota.iri.service.snapshot;

import com.iota.iri.MilestoneTracker;
import com.iota.iri.SignedFiles;
import com.iota.iri.conf.SnapshotConfig;
import com.iota.iri.controllers.*;
import com.iota.iri.model.Hash;
import com.iota.iri.storage.Tangle;
import com.iota.iri.utils.ProgressLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class SnapshotManager {
    private static final Logger log = LoggerFactory.getLogger(SnapshotManager.class);

    private static Snapshot builtinSnapshot = null;

    public static int GENERATE_FROM_INITIAL = 1;

    public static int GENERATE_FROM_LATEST = -1;

    public static String SNAPSHOT_PUBKEY = "TTXJUGKTNPOOEXSTQVVACENJOQUROXYKDRCVK9LHUXILCLABLGJTIPNF9REWHOIMEUKWQLUOKD9CZUYAC";

    public static int SNAPSHOT_PUBKEY_DEPTH = 6;

    public static int SNAPSHOT_INDEX = 6;

    public static int SPENT_ADDRESSES_INDEX = 7;

    private Tangle tangle;

    private SnapshotGarbageCollector snapshotGarbageCollector;

    private TipsViewModel tipsViewModel;

    private SnapshotConfig configuration;

    private Snapshot initialSnapshot;

    private Snapshot latestSnapshot;

    private boolean shuttingDown;

    private static int LOCAL_SNAPSHOT_RESCAN_INTERVAL = 10000;

    private ConcurrentHashMap<Hash, Integer> orphanedApprovers;

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

        // try to load a local snapshot first
        initialSnapshot = loadLocalSnapshot();

        // if we could not loaded a local snapshot -> fall back to the builtin one
        if(initialSnapshot == null) {
            initialSnapshot = loadBuiltInSnapshot();
        }

        // create a working copy of the initial snapshot that keeps track of the latest state
        latestSnapshot = initialSnapshot.clone();

        // initialize the snapshot garbage collector that takes care of cleaning up old transaction data
        snapshotGarbageCollector = new SnapshotGarbageCollector(tangle, this, tipsViewModel);
    }

    public void init(MilestoneTracker milestoneTracker) {
        // if local snapshots are enabled we initialize the parts taking care of local snapshots
        if(configuration.getLocalSnapshotsEnabled()) {
            spawnMonitorThread(milestoneTracker);

            snapshotGarbageCollector.start();
        }
    }

    public void spawnMonitorThread(MilestoneTracker milestoneTracker) {
        (new Thread(() -> {
            log.info("Local Snapshot Monitor started ...");

            // load necessary configuration parameters
            int snapshotDepth = configuration.getLocalSnapshotsDepth();
            int LOCAL_SNAPSHOT_INTERVAL = 10;

            while(!shuttingDown) {
                long scanStart = System.currentTimeMillis();

                if(latestSnapshot.getIndex() == milestoneTracker.latestMilestoneIndex && latestSnapshot.getIndex() - initialSnapshot.getIndex() > snapshotDepth + LOCAL_SNAPSHOT_INTERVAL) {
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
            if(!snapshot.getState().hasCorrectSupply() || !snapshot.getState().isConsistent()) {
                throw new SnapshotException("the StateDiff belonging to " + currentMilestone.toString() +" leads to an invalid supply");
            }
        }
    }

    public Snapshot generateSnapshot(MilestoneViewModel targetMilestone) throws SnapshotException {
        // read required config variables
        boolean testnet = configuration.isTestnet();

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
        MilestoneViewModel currentMilestone;
        try {
             currentMilestone = MilestoneViewModel.get(tangle, startingMilestoneIndex);
        } catch(Exception e) {
            throw new SnapshotException("could not retrieve the milestone #" + startingMilestoneIndex, e);
        }
        if(currentMilestone == null) {
            throw new SnapshotException("could not retrieve the milestone #" + startingMilestoneIndex);
        }

        // dump a progress message before we start
        dumpLogMessage("Taking local snapshot", "1/3 calculating new snapshot state", stepCounter = 0, amountOfMilestonesToProcess);

        // iterate through the milestones to our target
        while(generationMode == GENERATE_FROM_INITIAL ? currentMilestone.index() <= targetMilestone.index() : currentMilestone.index() > targetMilestone.index()) {
            // calculate the correct ledger state based on our current milestone
            calculateSnapshotState(snapshot, currentMilestone, generationMode);

            // retrieve the next milestone
            MilestoneViewModel nextMilestone;
            try {
                nextMilestone = generationMode == GENERATE_FROM_INITIAL
                              ? MilestoneViewModel.findClosestNextMilestone(tangle, currentMilestone.index())
                              : MilestoneViewModel.findClosestPrevMilestone(tangle, currentMilestone.index());
            } catch(Exception e) {
                throw new SnapshotException("could not iterate to the next milestone from " + currentMilestone.toString(), e);
            }
            if(nextMilestone == null) {
                throw new SnapshotException("could not iterate to the next milestone from " + currentMilestone.toString());
            }

            // iterate to the the next milestone
            currentMilestone = nextMilestone;

            // dump a progress message after every step
            dumpLogMessage("Taking local snapshot", "1/3 calculating new snapshot state", ++stepCounter, amountOfMilestonesToProcess);
        }

        //endregion ////////////////////////////////////////////////////////////////////////////////////////////////////

        //region ANALYZE OLD TRANSACTIONS THAT CAN BE PRUNED //////////////////////////////////////////////////////////

        // determine the initial snapshot index
        int initialSnapshotIndex = initialSnapshot.getIndex();

        // create a set where we collect the solid entry points
        HashMap<Hash, Integer> solidEntryPoints = new HashMap<>();
        solidEntryPoints.put(Hash.NULL_HASH, testnet ? 0 : configuration.getMilestoneStartIndex());

        // copy the old solid entry points which are still valid
        snapshot.getMetaData().getSolidEntryPoints().entrySet().stream().forEach(solidEntryPoint -> {
            if(solidEntryPoint.getValue() > targetMilestone.index()) {
                solidEntryPoints.put(solidEntryPoint.getKey(), solidEntryPoint.getValue());
            }
        });

        final int OUTER_SHELL_SIZE = 500;

        // dump a progress message before we start
        dumpLogMessage("Taking local snapshot", "2/3 processing old transactions", stepCounter = 0, amountOfMilestonesToProcess = Math.min(OUTER_SHELL_SIZE, targetMilestone.index() - initialSnapshotIndex));

        // iterate down through the tangle in "steps" (one milestone at a time) so the data structures don't get too big
        currentMilestone = targetMilestone;
        while(currentMilestone != null && currentMilestone.index() > initialSnapshotIndex && stepCounter < amountOfMilestonesToProcess) {
            // create a set where we collect the solid entry points
            Set<Hash> seenMilestoneTransactions = new HashSet<>();

            // retrieve the transaction belonging to our current milestone
            TransactionViewModel milestoneTransaction;
            try {
                milestoneTransaction = TransactionViewModel.fromHash(tangle, currentMilestone.getHash());
            } catch(Exception e) {
                throw new SnapshotException("could not retrieve the transaction belonging to " + currentMilestone.toString(), e);
            }
            if(milestoneTransaction == null) {
                throw new SnapshotException("could not retrieve the transaction belonging to " + currentMilestone.toString());
            }

            // create a queue where we collect the transactions that shall be examined (starting with our milestone)
            final Queue<TransactionViewModel> transactionsToExamine = new LinkedList<>(Collections.singleton(milestoneTransaction));

            // iterate through our queue and process all elements (while we iterate we add more)
            TransactionViewModel currentTransaction;
            while((currentTransaction = transactionsToExamine.poll()) != null) {
                // only process transactions that we haven't seen yet
                if(seenMilestoneTransactions.add(currentTransaction.getHash())) {
                    // if the transaction is a solid entry point -> add it to our list
                    int solidEntryPointIndex = getSolidEntryPointIndex(currentTransaction, targetMilestone);
                    if(!solidEntryPoints.containsKey(currentTransaction.getHash()) && solidEntryPointIndex != -1) {
                        solidEntryPoints.put(currentTransaction.getHash(), solidEntryPointIndex);
                    }

                    // only examine transactions that are not part of the solid entry points
                    if(!initialSnapshot.getMetaData().hasSolidEntryPoint(currentTransaction.getBranchTransactionHash())) {
                        // retrieve the branch transaction of our current transaction
                        TransactionViewModel branchTransaction;
                        try {
                            branchTransaction = currentTransaction.getBranchTransaction(tangle);
                        } catch(Exception e) {
                            throw new SnapshotException("could not retrieve the branch transaction of " + currentTransaction.toString(), e);
                        }
                        if(branchTransaction == null) {
                            throw new SnapshotException("could not retrieve the branch transaction of " + currentTransaction.toString());
                        }

                        // if the branch transaction is still approved by our current milestone -> add it to our queue
                        if(branchTransaction.snapshotIndex() == currentMilestone.index()) {
                            transactionsToExamine.add(branchTransaction);
                        }
                    }

                    // only examine transactions that are not part of the solid entry points
                    if(!initialSnapshot.getMetaData().hasSolidEntryPoint(currentTransaction.getTrunkTransactionHash())) {
                        // retrieve the trunk transaction of our current transaction
                        TransactionViewModel trunkTransaction;
                        try {
                            trunkTransaction = currentTransaction.getTrunkTransaction(tangle);
                        } catch(Exception e) {
                            throw new SnapshotException("could not retrieve the trunk transaction of " + currentTransaction.toString(), e);
                        }
                        if(trunkTransaction == null) {
                            throw new SnapshotException("could not retrieve the trunk transaction of " + currentTransaction.toString());
                        }

                        // if the trunk transaction is still approved by our current milestone -> add it to our queue
                        if(trunkTransaction.snapshotIndex() == currentMilestone.index()) {
                            transactionsToExamine.add(trunkTransaction);
                        }
                    }
                }
            }

            // iterate to the previous milestone
            try {
                currentMilestone = MilestoneViewModel.findClosestPrevMilestone(tangle, currentMilestone.index());
            } catch(Exception e) {
                throw new SnapshotException("could not iterate to the previous milestone", e);
            }

            // dump the progress after every step
            dumpLogMessage("Taking local snapshot", "2/3 processing old transactions", ++stepCounter, amountOfMilestonesToProcess);
        }

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
        snapshot.getMetaData().setIndex(targetMilestone.index());
        snapshot.getMetaData().setTimestamp(targetMilestoneTransaction.getTimestamp());
        snapshot.getMetaData().setSolidEntryPoints(solidEntryPoints);
        snapshot.getMetaData().setHash(targetMilestone.getHash());
        snapshot.getMetaData().setSeenMilestones(seenMilestones);

        //endregion ////////////////////////////////////////////////////////////////////////////////////////////////////

        // return the result
        return snapshot;
    }

    public int getSolidEntryPointIndex(TransactionViewModel transaction, MilestoneViewModel targetMilestone) throws SnapshotException {
        // create a set where we collect the solid entry points
        Set<Hash> seenApprovers = new HashSet<>();

        // retrieve the approvers of our transaction
        ApproveeViewModel approvers;
        try {
            approvers = transaction.getApprovers(tangle);
        } catch(Exception e) {
            throw new SnapshotException("could not get the approvers of " + transaction.toString(), e);
        }

        // create a variable for our result
        int result = -1;

        // examine the parents of our transaction
        for(Hash approverHash : approvers.getHashes()) {
            // only process transactions that we haven't seen yet
            if(seenApprovers.add(approverHash)) {
                // retrieve the transaction belonging to our approver hash
                TransactionViewModel approverTransaction;
                try {
                    approverTransaction = TransactionViewModel.fromHash(tangle, approverHash);
                } catch(Exception e) {
                    throw new SnapshotException("could not retrieve the transaction belonging to hash " + approverHash.toString(), e);
                }

                // check if the approver was referenced by another milestone in the future
                if(approverTransaction.snapshotIndex() > targetMilestone.index()) {
                    result = Math.max(result, approverTransaction.snapshotIndex());
                }
            }
        }

        // return false if we didnt find a referenced transaction
        return result;
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

    public Snapshot loadLocalSnapshot() throws IOException, IllegalStateException {
        // load necessary configuration parameters
        boolean localSnapshotsEnabled = configuration.getLocalSnapshotsEnabled();

        // if local snapshots are enabled
        if(localSnapshotsEnabled) {
            // load the remaining configuration parameters
            boolean testnet = configuration.isTestnet();
            String basePath = testnet
                            ? configuration.getLocalSnapshotsTestnetBasePath()
                            : configuration.getLocalSnapshotsMainnetBasePath();

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

        // otherwise just return null
        return null;
    }

    public Snapshot loadBuiltInSnapshot() throws IOException, IllegalStateException {
        if (builtinSnapshot == null) {
            // read the config vars for the built in snapshot files
            boolean testnet = configuration.isTestnet();
            String snapshotPath = configuration.getSnapshotFile();
            String snapshotSigPath = configuration.getSnapshotSignatureFile();
            int milestoneStartIndex = testnet ? 0 : configuration.getMilestoneStartIndex();

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
        String basePath = testnet
                          ? configuration.getLocalSnapshotsTestnetBasePath()
                          : configuration.getLocalSnapshotsMainnetBasePath();
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

        snapshotGarbageCollector.addCleanupJob(targetMilestone.index());

        try {
            targetSnapshot.getState().writeFile(basePath + ".snapshot.state");
            targetSnapshot.getMetaData().writeFile(basePath + ".snapshot.meta");
        } catch(IOException e) {
            throw new SnapshotException("could not write local snapshot files", e);
        }

        initialSnapshot = targetSnapshot;

        return targetSnapshot;
    }
}
