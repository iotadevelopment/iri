package com.iota.iri.service.snapshot;

import com.iota.iri.Iota;
import com.iota.iri.conf.Configuration;
import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.controllers.StateDiffViewModel;
import com.iota.iri.model.Hash;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class SnapshotManager {
    Iota instance;

    Configuration configuration;

    public SnapshotManager(Iota instance) {
        // store a reference to the IOTA instance so we can access all relevant objects
        this.instance = instance;

        this.configuration = instance.configuration;
    }

    public Snapshot generateSnapshot(MilestoneViewModel targetMilestone) throws Exception {
        // check if the milestone was solidified already
        if(targetMilestone.index() > instance.milestone.latestSnapshot.metaData().milestoneIndex()) {
            throw new IllegalArgumentException("the milestone was not solidified yet");
        }

        // clone the current snapshot state
        Snapshot snapshot = instance.milestone.latestSnapshot.clone();

        // if the target is the latest milestone we can return immediately
        if(targetMilestone.index() == instance.milestone.latestSnapshot.metaData().milestoneIndex()) {
            return snapshot;
        }

        // retrieve the latest milestone
        MilestoneViewModel currentMilestone = MilestoneViewModel.get(instance.tangle, instance.milestone.latestSolidSubtangleMilestoneIndex);

        // this should not happen but better give a reasonable error message if it ever does
        if(currentMilestone == null) {
            throw new IllegalStateException("could not load the latest milestone from the database");
        }

        // descend the milestones down to our target
        while(currentMilestone.index() > targetMilestone.index()) {
            // retrieve the balance diff from the db
            StateDiffViewModel stateDiffViewModel = StateDiffViewModel.load(instance.tangle, currentMilestone.getHash());

            // if we have a diff apply it
            if(stateDiffViewModel != null && !stateDiffViewModel.isEmpty()) {
                // apply the balance changes to the snapshot (with inverted values)
                snapshot.apply(
                    stateDiffViewModel.getDiff().entrySet().stream().map(
                        hashLongEntry -> new HashMap.SimpleEntry<>(hashLongEntry.getKey(), -hashLongEntry.getValue())
                    ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
                    currentMilestone.index()
                );
            }

            // iterate to the next milestone
            currentMilestone = MilestoneViewModel.findClosestPrevMilestone(instance.tangle, currentMilestone.index());

            // this should not happen but better give a reasonable error message if it ever does
            if(currentMilestone == null) {
                throw new IllegalStateException("could not reach the target milestone - missing links in the database");
            }
        }

        // return the result
        return snapshot;
    }

    public Snapshot loadLocalSnapshot() throws IOException {
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
            File localSnapshotFile = new File(basePath + ".snap");

            // create a file handle for our snapshot metadata file
            File localSnapshotMetadDataFile = new File(basePath + ".msnap");

            // if the local snapshot files exists -> load them
            if(
                localSnapshotFile.exists() &&
                localSnapshotFile.isFile() &&
                localSnapshotMetadDataFile.exists() &&
                localSnapshotMetadDataFile.isFile()
            ) {
                // retrieve the meta data to our local snapshot
                SnapshotMetaData snapshotMetaData = SnapshotMetaData.fromFile(localSnapshotMetadDataFile);

                // construct the initial state from our local files
                Map<Hash, Long> initialState = Snapshot.initInitialState(localSnapshotFile.getAbsolutePath());

                // verify the consistency of our snapshot state
                Snapshot.checkStateHasCorrectSupply(initialState);
                Snapshot.checkInitialSnapshotIsConsistent(initialState);

                // return our Snapshot
                return new Snapshot(initialState, snapshotMetaData);
            }
        }

        // otherwise just return null
        return null;
    }

    public Snapshot writeLocalSnapshot() {
        return null;
    }
}
