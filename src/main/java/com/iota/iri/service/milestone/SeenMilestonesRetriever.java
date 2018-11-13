package com.iota.iri.service.milestone;

public interface SeenMilestonesRetriever {
    void retrieveSeenMilestones();

    void start();

    void shutdown();
}
