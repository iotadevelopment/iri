package com.iota.iri.service.localSnapshot;

import com.iota.iri.Iota;
import com.iota.iri.Milestone;
import com.iota.iri.Snapshot;
import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.controllers.StateDiffViewModel;
import com.iota.iri.model.Hash;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class LocalSnapshotManager {
    Iota instance;

    public LocalSnapshotManager(Iota instance) {
        // store a reference to the IOTA instance so we can access all relevant objects
        this.instance = instance;
    }

    public Snapshot getSnapshot(Hash milestoneHash) throws Exception {
        return getSnapshot(MilestoneViewModel.fromHash(instance.tangle, milestoneHash));
    }

    public Snapshot getSnapshot(int milestoneIndex) throws Exception {
        return getSnapshot(MilestoneViewModel.get(instance.tangle, milestoneIndex));
    }

    public Snapshot getSnapshot(MilestoneViewModel targetMilestone) throws Exception {
        // check if the milestone was solidified already
        if(targetMilestone.index() > instance.milestone.latestSolidSubtangleMilestoneIndex) {
            throw new IllegalArgumentException("milestone not solidified yet");
        }

        // clone the current snapshot state
        Snapshot snapshot = instance.milestone.latestSnapshot.clone();

        // if the target is the latest milestone we can return immediately
        if(targetMilestone.index() == instance.milestone.latestSolidSubtangleMilestoneIndex) {
            return snapshot;
        }

        // retrieve the latest milestone
        MilestoneViewModel currentMilestone = MilestoneViewModel.get(instance.tangle, instance.milestone.latestSolidSubtangleMilestoneIndex);

        // descend the milestones down to our target
        while(currentMilestone.index() > targetMilestone.index()) {
            // apply the balance changes to the snapshot (with inverted values)
            snapshot.apply(
                StateDiffViewModel.load(instance.tangle, currentMilestone.getHash()).getDiff().entrySet().stream().map(
                    hashLongEntry -> new HashMap.SimpleEntry<>(hashLongEntry.getKey(), -hashLongEntry.getValue())
                ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
                currentMilestone.index()
            );

            // iterate to the next milestone
            currentMilestone = currentMilestone.previous(instance.tangle);
        }

        // return the result
        return snapshot;
    }
}
