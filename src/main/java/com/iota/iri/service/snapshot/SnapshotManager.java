package com.iota.iri.service.snapshot;

import com.iota.iri.SignedFiles;
import com.iota.iri.conf.Configuration;
import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.controllers.StateDiffViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.storage.Tangle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

public class SnapshotManager {
    private static final Logger log = LoggerFactory.getLogger(SnapshotManager.class);

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

    public Snapshot generateSnapshot(MilestoneViewModel targetMilestone) throws Exception {
        // check if the milestone was solidified already
        if(targetMilestone.index() > latestSnapshot.getIndex()) {
            throw new IllegalArgumentException("the milestone was not solidified yet");
        }

        // clone the current snapshot state
        Snapshot snapshot = latestSnapshot.clone();

        // if the target is the latest milestone we can return immediately
        if(targetMilestone.index() == latestSnapshot.getIndex()) {
            return snapshot;
        }

        // retrieve the latest milestone
        MilestoneViewModel currentMilestone = MilestoneViewModel.get(tangle, latestSnapshot.getIndex());

        // this should not happen but better give a reasonable error message if it ever does
        if(currentMilestone == null) {
            throw new IllegalStateException("could not load the latest milestone from the database");
        }

        // descend the milestones down to our target
        while(currentMilestone.index() > targetMilestone.index()) {
            // retrieve the balance diff from the db
            StateDiffViewModel stateDiffViewModel = StateDiffViewModel.load(tangle, currentMilestone.getHash());

            // if we have a diff apply it (with inverted values)
            if(stateDiffViewModel != null && !stateDiffViewModel.isEmpty()) {
                // create the SnapshotStateDiff object for our changes
                SnapshotStateDiff snapshotStateDiff = new SnapshotStateDiff(stateDiffViewModel.getDiff().entrySet().stream().map(
                    hashLongEntry -> new HashMap.SimpleEntry<>(hashLongEntry.getKey(), -hashLongEntry.getValue())
                ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));

                // apply the balance changes to the snapshot (with inverted values)
                snapshot.update(
                    snapshotStateDiff,
                    currentMilestone.index()
                );
            }

            // iterate to the next milestone
            currentMilestone = MilestoneViewModel.findClosestPrevMilestone(tangle, currentMilestone.index());

            // this should not happen but better give a reasonable error message if it ever does
            if(currentMilestone == null) {
                throw new IllegalStateException("could not reach the target milestone - missing links in the database");
            }
        }

        // set the snapshot index to our target milestone
        snapshot.getMetaData().setIndex(targetMilestone.index());

        // return the result
        return snapshot;
    }

    public Snapshot loadLocalSnapshot() throws IOException, IllegalStateException {
        // load necessary configuration parameters
        boolean localSnapshotsEnabled = configuration.booling(Configuration.DefaultConfSettings.LOCAL_SNAPSHOTS_ENABLED);

        System.out.println(localSnapshotsEnabled);

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

        return targetSnapshot;
    }
}
