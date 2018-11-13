package com.iota.iri.service.milestone.impl;

import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.network.TransactionRequester;
import com.iota.iri.service.milestone.SeenMilestonesRetriever;
import com.iota.iri.service.snapshot.SnapshotProvider;
import com.iota.iri.storage.Tangle;
import com.iota.iri.utils.log.interval.IntervalLogger;
import com.iota.iri.utils.thread.DedicatedScheduledExecutorService;
import com.iota.iri.utils.thread.SilentScheduledExecutorService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class SeenMilestonesRetrieverImpl implements SeenMilestonesRetriever {
    private static final int RETRIEVE_RANGE = 50;

    private static final int RESCAN_INTERVAL = 1000;

    /**
     * Holds the logger of this class (a rate limited logger than doesn't spam the CLI output).<br />
     */
    private static final IntervalLogger log = new IntervalLogger(SeenMilestonesRetrieverImpl.class);

    private final Tangle tangle;

    private final SnapshotProvider snapshotProvider;

    private final TransactionRequester transactionRequester;

    /**
     * Holds a reference to the manager of the background worker.<br />
     */
    private final SilentScheduledExecutorService executorService = new DedicatedScheduledExecutorService(
            "Seen Milestones Retriever", log.delegate());

    private final Map<Hash, Integer> seenMilestones;

    public SeenMilestonesRetrieverImpl(Tangle tangle, SnapshotProvider snapshotProvider, TransactionRequester transactionRequester) {
        this.tangle = tangle;
        this.snapshotProvider = snapshotProvider;
        this.transactionRequester = transactionRequester;

        seenMilestones = new ConcurrentHashMap<>(snapshotProvider.getInitialSnapshot().getSeenMilestones());
    }

    public void retrieveSeenMilestones() {
        // retrieve milestones from our local snapshot (if they are still missing)
        seenMilestones.forEach((milestoneHash, milestoneIndex) -> {
            try {
                // remove old milestones that are not relevant anymore
                if (milestoneIndex <= snapshotProvider.getLatestSnapshot().getIndex()) {
                    seenMilestones.remove(milestoneHash);
                }

                // check milestones that are within our check range
                else if (milestoneIndex < snapshotProvider.getLatestSnapshot().getIndex() + RETRIEVE_RANGE) {
                    TransactionViewModel milestoneTransaction = TransactionViewModel.fromHash(tangle, milestoneHash);
                    if (milestoneTransaction.getType() == TransactionViewModel.PREFILLED_SLOT &&
                            !transactionRequester.isTransactionRequested(milestoneHash, true)) {

                        transactionRequester.requestTransaction(milestoneHash, true);
                    }

                    seenMilestones.remove(milestoneHash);
                }
            } catch (Exception e) { /* do nothing */ }
        });

        if (seenMilestones.size() == 0) {
            shutdown();
        }
    }

    @Override
    public void start() {
        executorService.silentScheduleWithFixedDelay(this::retrieveSeenMilestones, 0, RESCAN_INTERVAL,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
        executorService.shutdownNow();
    }
}
