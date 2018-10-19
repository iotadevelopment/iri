package com.iota.iri.service.snapshot;

import com.iota.iri.MilestoneTracker;

public interface SnapshotManager {
    void init(MilestoneTracker milestoneTracker);

    void shutDown();
}
