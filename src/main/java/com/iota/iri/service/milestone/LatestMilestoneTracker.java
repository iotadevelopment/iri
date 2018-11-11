package com.iota.iri.service.milestone;

import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;

public interface LatestMilestoneTracker {
    void setLatestMilestone(Hash latestMilestoneHash, int latestMilestoneIndex);

    int getLatestMilestoneIndex();

    Hash getLatestMilestoneHash();

    MilestoneValidity analyzeMilestoneCandidate(Hash candidateTransactionHash) throws Exception;

    MilestoneValidity analyzeMilestoneCandidate(TransactionViewModel potentialMilestoneTransaction) throws Exception;

    /**
     * Since the {@link LatestMilestoneTracker} currently scans all milestone candidates whenever IRI restarts, this flag
     * @return
     */
    boolean initialScanComplete();

    void start();

    void shutdown();
}
