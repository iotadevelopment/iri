package com.iota.iri.service.tipselection.impl;

import com.iota.iri.MilestoneTracker;
import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.service.snapshot.SnapshotManager;
import com.iota.iri.service.tipselection.EntryPointSelector;
import com.iota.iri.storage.Tangle;

/**
 * Implementation of <tt>EntryPointSelector</tt> that given a depth N, returns a N-deep milestone.
 * Meaning <CODE>milestone(latestSolid - depth)</CODE>
 * Used to as a starting point for the random walk.
 */
public class EntryPointSelectorImpl implements EntryPointSelector {

    private final Tangle tangle;
    private final MilestoneTracker milestone;
    private final SnapshotManager snapshotManager;

    public EntryPointSelectorImpl(Tangle tangle, MilestoneTracker milestone, SnapshotManager snapshotManager) {
        this.tangle = tangle;
        this.milestone = milestone;
        this.snapshotManager = snapshotManager;
    }

    @Override
    public Hash getEntryPoint(int depth) throws Exception {
        int milestoneIndex = Math.max(snapshotManager.getLatestSnapshot().getIndex() - depth - 1, -1);
        MilestoneViewModel milestoneViewModel =
                MilestoneViewModel.findClosestNextMilestone(tangle, milestoneIndex);
        if (milestoneViewModel != null && milestoneViewModel.getHash() != null) {
            return milestoneViewModel.getHash();
        }

        return milestone.latestSolidSubtangleMilestone;
    }
}
