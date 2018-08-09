package com.iota.iri.service.snapshot;

import com.iota.iri.SignedFiles;
import com.iota.iri.conf.Configuration;
import com.iota.iri.controllers.ApproveeViewModel;
import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.controllers.StateDiffViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.storage.Tangle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class SnapshotManager {
    private static final Logger log = LoggerFactory.getLogger(SnapshotManager.class);

    public static int GENERATE_FROM_INITIAL = 1;

    public static int GENERATE_FROM_LATEST = -1;

    public static String SNAPSHOT_PUBKEY = "TTXJUGKTNPOOEXSTQVVACENJOQUROXYKDRCVK9LHUXILCLABLGJTIPNF9REWHOIMEUKWQLUOKD9CZUYAC";

    public static int SNAPSHOT_PUBKEY_DEPTH = 6;

    public static int SNAPSHOT_INDEX = 6;

    public static int SPENT_ADDRESSES_INDEX = 7;

    private Tangle tangle;

    private Configuration configuration;

    private Snapshot initialSnapshot;

    private Snapshot latestSnapshot;

    /**
     * This methdd is the constructor of the SnapshotManager.
     *
     * It stores the instances that this class depends on and tries to load the initial Snapshot, by first checking
     * if local snapshots are enabled and available and then falling back to the builtin Snapshot of the IRI.jar
     *
     * @param tangle wrapper for the database interface
     * @param configuration configuration of the node
     * @throws IOException if something goes wrong while processing the snapshot files
     */
    public SnapshotManager(Tangle tangle, Configuration configuration) throws IOException {
        // save the necessary dependencies
        this.tangle = tangle;
        this.configuration = configuration;

        // try to load a local snapshot first
        initialSnapshot = loadLocalSnapshot();

        // if we could not loaded a local snapshot -> fall back to the builtin one
        if(initialSnapshot == null) {
            initialSnapshot = loadBuiltInSnapshot();
        }

        // make a working copy of the initial snapshot that keeps track of the latest state
        latestSnapshot = initialSnapshot.clone();
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

    public Snapshot generateSnapshot(MilestoneViewModel targetMilestone) throws SnapshotException {
        // check if the milestone is not null
        if(targetMilestone == null) {
            throw new SnapshotException("the target milestone must not be null");
        }

        // acquire locks for our snapshots
        initialSnapshot.lockWrite();
        latestSnapshot.lockWrite();

        // process everything that follows in a try block to be able to always unlock when we are done
        try {
            // check if the milestone was solidified already
            if(targetMilestone.index() > latestSnapshot.getIndex()) {
                throw new SnapshotException("the target " + targetMilestone.toString() + " was not solidified yet");
            }

            // check if the milestone came after our initial one
            if(targetMilestone.index() < initialSnapshot.getIndex()) {
                throw new SnapshotException("the target " + targetMilestone.toString() + " is too old");
            }

            // determine the distance of our target snapshot from our two snapshots (initial / latest)
            int distanceFromInitialSnapshot = Math.abs(initialSnapshot.getIndex() - targetMilestone.index());
            int distanceFromLatestSnapshot = Math.abs(latestSnapshot.getIndex() - targetMilestone.index());

            // determine which generation mode is the fastest one
            int generationMode = distanceFromInitialSnapshot <= distanceFromLatestSnapshot
                               ? GENERATE_FROM_INITIAL
                               : GENERATE_FROM_LATEST;

            // clone the corresponding snapshot state
            Snapshot snapshot = generationMode == GENERATE_FROM_INITIAL
                              ? initialSnapshot.clone()
                              : latestSnapshot.clone();

            // if the target is the selected milestone we can return immediately
            if(targetMilestone.index() == snapshot.getIndex()) {
                return snapshot;
            }

            // calculate the starting point for our snapshot generation
            int startingMilestoneIndex = snapshot.getIndex() + (generationMode == GENERATE_FROM_INITIAL ? 1 : 0);

            // retrieve the first milestone for our snapshot generation
            MilestoneViewModel currentMilestone;
            try {
                 currentMilestone = MilestoneViewModel.get(tangle, startingMilestoneIndex);
            } catch(Exception e) {
                throw new SnapshotException(
                    "could not retrieve the milestone #" + startingMilestoneIndex, e
                );
            }

            // this should not happen but better give a reasonable error message if it ever does
            if(currentMilestone == null) {
                throw new SnapshotException("could not retrieve the milestone #" + startingMilestoneIndex);
            }

            // iterate through the milestones to our target
            while(generationMode == GENERATE_FROM_INITIAL ? currentMilestone.index() <= targetMilestone.index()
                                                          : currentMilestone.index() > targetMilestone.index()) {
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
                        throw new SnapshotException(
                            "the StateDiff belonging to " + currentMilestone.toString() + " is inconsistent"
                        );
                    }

                    // apply the balance changes to the snapshot
                    snapshot.update(
                        snapshotStateDiff,
                        currentMilestone.index()
                    );

                    // this should never happen since we check the snapshots already when applying them but better give
                    // a reasonable error message if it ever does
                    if(!snapshot.getState().hasCorrectSupply() || !snapshot.getState().isConsistent()) {
                        throw new SnapshotException(
                            "the StateDiff belonging to " + currentMilestone.toString() +" leads to an invalid supply"
                        );
                    }
                }

                // retrieve the next milestone
                MilestoneViewModel nextMilestone;
                try {
                    nextMilestone = generationMode == GENERATE_FROM_INITIAL
                                       ? MilestoneViewModel.findClosestNextMilestone(tangle, currentMilestone.index())
                                       : MilestoneViewModel.findClosestPrevMilestone(tangle, currentMilestone.index());
                } catch(Exception e) {
                    throw new SnapshotException(
                        "could not iterate to the next milestone from " + currentMilestone.toString(), e
                    );
                }

                // this should not happen but better give a reasonable error message if it ever does
                if(currentMilestone == null) {
                    throw new SnapshotException(
                        "could not iterate to the next milestone from " + currentMilestone.toString()
                    );
                }

                // iterate to the the next milestone
                currentMilestone = nextMilestone;
            }

            // retrieve the transaction belonging to our targetMilestone
            TransactionViewModel targetMilestoneTransaction;
            try {
                targetMilestoneTransaction = TransactionViewModel.fromHash(tangle, targetMilestone.getHash());
            } catch(Exception e) {
                throw new SnapshotException(
                    "could not retrieve the transaction belonging to " + targetMilestone.toString(), e
                );
            }

            // set the snapshot index and timestamp to that of our target milestone transaction
            snapshot.getMetaData().setIndex(targetMilestone.index());
            snapshot.getMetaData().setTimestamp(targetMilestoneTransaction.getTimestamp());

            // return the result
            return snapshot;
        }

        // after we are done ...
        finally {
            // ... release the locks for our snapshots
            initialSnapshot.unlockWrite();
            latestSnapshot.unlockWrite();
        }
    }

    public void generateSolidEntryPoints(MilestoneViewModel targetMilestone) throws SnapshotException {
        // create a set where we collect the solid entry points
        Set<Hash> solidEntryPoints = new HashSet<>();

        // iterate down through the tangle in "steps" (one milestone at a time) so the data structures don't get too big
        MilestoneViewModel currentMilestone = targetMilestone;
        while(currentMilestone != null) {
            // retrieve the transaction belonging to our current milestone
            TransactionViewModel milestoneTransaction;
            try {
                milestoneTransaction = TransactionViewModel.fromHash(tangle, currentMilestone.getHash());
            } catch(Exception e) {
                throw new SnapshotException("could not retrieve the transaction belonging to " + currentMilestone.toString(), e);
            }

            // create a queue where we collect the transactions that shall be examined (starting with our milestone)
            final Queue<TransactionViewModel> transactionsToExamine = new LinkedList<>(
                Collections.singleton(milestoneTransaction)
            );

            // define how big the outer shell should be
            int outerShellSize = 0;

            // iterate through our queue and process all elements (while we iterate we add more)
            TransactionViewModel currentTransaction;
            while((currentTransaction = transactionsToExamine.poll()) != null && ++outerShellSize <= 100) {
                // retrieve the approvers of our transaction
                ApproveeViewModel approvers;
                try {
                    approvers = currentTransaction.getApprovers(tangle);
                } catch(Exception e) {
                    throw new SnapshotException("could not get the approvers of " + currentTransaction.toString(), e);
                }

                // examine the parents of our transaction (and check if they are solid entry points)
                for(Hash approverHash: approvers.getHashes()) {
                    // retrieve the transaction belonging to our approver hash
                    TransactionViewModel approverTransaction;
                    try {
                        approverTransaction = TransactionViewModel.fromHash(tangle, approverHash);
                    } catch(Exception e) {
                        throw new SnapshotException(
                            "could not retrieve the transaction belonging to hash " + approverHash.toString(),
                            e
                        );
                    }

                    // check if the approver was referenced by another milestone in the future
                    if(approverTransaction.snapshotIndex() > targetMilestone.index()) {
                        solidEntryPoints.add(approverHash);
                    }
                }

                // retrieve the branch transaction of our current transaction
                TransactionViewModel branchTransaction;
                try {
                    branchTransaction = currentTransaction.getBranchTransaction(tangle);
                } catch(Exception e) {
                    throw new SnapshotException(
                        "could not retrieve the branch transaction of " + currentTransaction.toString(),
                        e
                    );
                }

                // if the branch transaction is still approved by our current milestone -> add it to our queue
                if(branchTransaction.snapshotIndex() == currentMilestone.index()) {
                    transactionsToExamine.offer(branchTransaction);
                }

                // retrieve the trunk transaction of our current transaction
                TransactionViewModel trunkTransaction;
                try {
                    trunkTransaction = currentTransaction.getTrunkTransaction(tangle);
                } catch(Exception e) {
                    throw new SnapshotException(
                        "could not retrieve the trunk transaction of " + currentTransaction.toString(),
                        e
                    );
                }

                // if the trunk transaction is still approved by our current milestone -> add it to our queue
                if(trunkTransaction.snapshotIndex() == currentMilestone.index()) {
                    transactionsToExamine.offer(trunkTransaction);
                }
            }

            // iterate to the previous milestone
            try {
                currentMilestone = MilestoneViewModel.findClosestPrevMilestone(tangle, currentMilestone.index());
            } catch(Exception e) {
                throw new SnapshotException("could not iterate to the previous milestone", e);
            }

            // dump some debug messages
            System.out.println(solidEntryPoints.size());
        }

        // dump some debug messages
        System.out.println(solidEntryPoints.toString());
    }

    public Snapshot loadLocalSnapshot() throws IOException, IllegalStateException {
        // load necessary configuration parameters
        boolean localSnapshotsEnabled = configuration.booling(Configuration.DefaultConfSettings.LOCAL_SNAPSHOTS_ENABLED);

        // if local snapshots are enabled
        if(localSnapshotsEnabled) {
            // load the remaining configuration parameters
            boolean testnet = configuration.booling(Configuration.DefaultConfSettings.TESTNET);
            String basePath = configuration.string(
                testnet ? Configuration.DefaultConfSettings.LOCAL_SNAPSHOTS_TESTNET_BASE_PATH
                        : Configuration.DefaultConfSettings.LOCAL_SNAPSHOTS_MAINNET_BASE_PATH
            );

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
        // read the config vars for the built in snapshot files
        boolean testnet = configuration.booling(Configuration.DefaultConfSettings.TESTNET);
        String snapshotPath = configuration.string(Configuration.DefaultConfSettings.SNAPSHOT_FILE);
        String snapshotSigPath = configuration.string(Configuration.DefaultConfSettings.SNAPSHOT_SIGNATURE_FILE);

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

        // return our snapshot
        return new Snapshot(
            snapshotState,
            new SnapshotMetaData(
                testnet ? 0 : configuration.integer(Configuration.DefaultConfSettings.MILESTONE_START_INDEX),
                configuration.longNum(Configuration.DefaultConfSettings.SNAPSHOT_TIME),
                new HashSet<Hash>(Collections.singleton(Hash.NULL_HASH))
            )
        );
    }

    public Snapshot writeLocalSnapshot() throws SnapshotException {
        // load necessary configuration parameters
        boolean testnet = configuration.booling(Configuration.DefaultConfSettings.TESTNET);
        String basePath = configuration.string(
            testnet ? Configuration.DefaultConfSettings.LOCAL_SNAPSHOTS_TESTNET_BASE_PATH
                    : Configuration.DefaultConfSettings.LOCAL_SNAPSHOTS_MAINNET_BASE_PATH
        );
        int snapshotDepth = configuration.integer(Configuration.DefaultConfSettings.LOCAL_SNAPSHOTS_DEPTH);

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
