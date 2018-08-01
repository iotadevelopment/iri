package com.iota.iri.service.snapshot;

import com.iota.iri.Iota;
import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.controllers.StateDiffViewModel;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class LocalSnapshotManager {
    Iota instance;

    public LocalSnapshotManager(Iota instance) {
        // store a reference to the IOTA instance so we can access all relevant objects
        this.instance = instance;
    }

    public Snapshot getSnapshot(MilestoneViewModel targetMilestone) throws Exception {
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
}
