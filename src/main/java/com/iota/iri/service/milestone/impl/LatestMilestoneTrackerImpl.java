package com.iota.iri.service.milestone.impl;

import com.iota.iri.conf.IotaConfig;
import com.iota.iri.controllers.AddressViewModel;
import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.crypto.SpongeFactory;
import com.iota.iri.model.Hash;
import com.iota.iri.model.HashFactory;
import com.iota.iri.service.milestone.LatestMilestoneTracker;
import com.iota.iri.service.milestone.MilestoneService;
import com.iota.iri.service.milestone.MilestoneSolidifier;
import com.iota.iri.service.milestone.MilestoneValidity;
import com.iota.iri.service.snapshot.Snapshot;
import com.iota.iri.service.snapshot.SnapshotProvider;
import com.iota.iri.service.snapshot.SnapshotService;
import com.iota.iri.storage.Tangle;
import com.iota.iri.utils.log.interval.IntervalLogger;
import com.iota.iri.utils.thread.DedicatedScheduledExecutorService;
import com.iota.iri.utils.thread.SilentScheduledExecutorService;
import com.iota.iri.zmq.MessageQ;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.iota.iri.service.milestone.MilestoneValidity.INCOMPLETE;
import static com.iota.iri.service.milestone.MilestoneValidity.INVALID;
import static com.iota.iri.service.milestone.MilestoneValidity.VALID;

public class LatestMilestoneTrackerImpl implements LatestMilestoneTracker {
    private static final int MAX_CANDIDATES_TO_ANALYZE = 1000;

    private static final int RESCAN_INTERVAL = 5000;

    private static final IntervalLogger log = new IntervalLogger(LatestMilestoneTrackerImpl.class);

    private final Tangle tangle;

    private final SnapshotProvider snapshotProvider;

    private final MilestoneService milestoneService;

    private final MilestoneSolidifier milestoneSolidifier;

    private final MessageQ messageQ;

    private final IotaConfig config;

    private final SilentScheduledExecutorService executorService;

    private final Hash coordinatorAddress;

    private int latestMilestoneIndex;

    private Hash latestMilestoneHash;

    private final Set<Hash> seenMilestoneCandidates = new HashSet<>();

    private final Deque<Hash> milestoneCandidatesToAnalyze = new ArrayDeque<>();

    private boolean firstRun = true;

    /**
     * Flag which indicates if this tracker was fully initialized.
     */
    private boolean initialized = false;

    public LatestMilestoneTrackerImpl(Tangle tangle, SnapshotProvider snapshotProvider, SnapshotService snapshotService,
            MilestoneService milestoneService, MilestoneSolidifier milestoneSolidifier, MessageQ messageQ,
            IotaConfig config) {

        this.tangle = tangle;
        this.snapshotProvider = snapshotProvider;
        this.milestoneService = milestoneService;
        this.milestoneSolidifier = milestoneSolidifier;
        this.messageQ = messageQ;
        this.config = config;

        executorService = new DedicatedScheduledExecutorService("Latest Milestone Tracker", log.delegate());
        coordinatorAddress = HashFactory.ADDRESS.create(config.getCoordinator());

        // bootstrap with the latest snapshot first
        Snapshot latestSnapshot = snapshotProvider.getLatestSnapshot();
        setLatestMilestone(latestSnapshot.getHash(), latestSnapshot.getIndex());

        // check if we have a bigger milestone as the last in our DB (faster bootstrap)
        try {
            MilestoneViewModel lastMilestoneInDatabase = MilestoneViewModel.latest(tangle);
            if (lastMilestoneInDatabase != null && lastMilestoneInDatabase.index() > getLatestMilestoneIndex()) {
                setLatestMilestone(lastMilestoneInDatabase.getHash(), lastMilestoneInDatabase.index());
            }
        } catch (Exception e) {
             // just continue with the previously set latest milestone
        }
    }

    @Override
    public void setLatestMilestone(Hash latestMilestoneHash, int latestMilestoneIndex) {
        messageQ.publish("lmi %d %d", this.latestMilestoneIndex, latestMilestoneIndex);
        log.delegate().info("Latest milestone has changed from #" + this.latestMilestoneIndex + " to #" + latestMilestoneIndex);

        this.latestMilestoneHash = latestMilestoneHash;
        this.latestMilestoneIndex = latestMilestoneIndex;
    }

    @Override
    public int getLatestMilestoneIndex() {
        return latestMilestoneIndex;
    }

    @Override
    public Hash getLatestMilestoneHash() {
        return latestMilestoneHash;
    }

    @Override
    public MilestoneValidity analyzeMilestoneCandidate(Hash candidateTransactionHash) throws Exception {
        return analyzeMilestoneCandidate(TransactionViewModel.fromHash(tangle, candidateTransactionHash));
    }

    @Override
    public MilestoneValidity analyzeMilestoneCandidate(TransactionViewModel potentialMilestoneTransaction) throws Exception {
        if (coordinatorAddress.equals(potentialMilestoneTransaction.getAddressHash()) && potentialMilestoneTransaction.getCurrentIndex() == 0) {
            int milestoneIndex = milestoneService.getMilestoneIndex(potentialMilestoneTransaction);

            switch (milestoneService.validateMilestone(tangle, snapshotProvider, config, potentialMilestoneTransaction, SpongeFactory.Mode.CURLP27, 1)) {
                case VALID:
                    if (milestoneIndex > latestMilestoneIndex) {
                        setLatestMilestone(potentialMilestoneTransaction.getHash(), milestoneIndex);
                    } else {
                        MilestoneViewModel latestMilestoneViewModel = MilestoneViewModel.latest(tangle);
                        if (latestMilestoneViewModel != null && latestMilestoneViewModel.index() > latestMilestoneIndex) {
                            messageQ.publish("lmi %d %d", latestMilestoneIndex, latestMilestoneViewModel.index());
                            log.delegate().info("Latest milestone has changed from #" + latestMilestoneIndex + " to #" + latestMilestoneViewModel.index());

                            latestMilestoneHash = latestMilestoneViewModel.getHash();
                            latestMilestoneIndex = latestMilestoneViewModel.index();
                        }
                    }

                    if(!potentialMilestoneTransaction.isSolid()) {
                        milestoneSolidifier.add(potentialMilestoneTransaction.getHash(), milestoneIndex);
                    }

                    potentialMilestoneTransaction.isMilestone(tangle, snapshotProvider.getInitialSnapshot(), true);

                    return VALID;

                case INCOMPLETE:
                    milestoneSolidifier.add(potentialMilestoneTransaction.getHash(), milestoneIndex);

                    potentialMilestoneTransaction.isMilestone(tangle, snapshotProvider.getInitialSnapshot(), true);

                    return INCOMPLETE;
            }
        }

        return INVALID;
    }

    @Override
    public boolean initialScanComplete() {
        return initialized;
    }

    @Override
    public void start() {
        executorService.silentScheduleWithFixedDelay(this::latestMilestoneTrackerThread, 0, RESCAN_INTERVAL,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
        executorService.shutdownNow();
    }

    private void latestMilestoneTrackerThread() {
        try {
            // will not fire on the first run because we have an empty list
            if (milestoneCandidatesToAnalyze.size() > 1) {
                log.info("Processing milestone candidates (" + milestoneCandidatesToAnalyze.size() + " remaining) ...");
            }

            // add all new milestone candidates to the list of candidates that need to be analyzed
            for(Hash hash: AddressViewModel.load(tangle, coordinatorAddress).getHashes()) {
                // allow the thread to be interrupted
                if (Thread.currentThread().isInterrupted()) {
                    return;
                }

                if (seenMilestoneCandidates.add(hash)) {
                    // we add at the beginning to process newly received milestones first
                    milestoneCandidatesToAnalyze.addFirst(hash);
                }
            }

            // log how many milestone candidates are remaining to be analyzed
            if (firstRun) {
                firstRun = false;

                if (milestoneCandidatesToAnalyze.size() > 1) {
                    log.info("Processing milestone candidates (" + milestoneCandidatesToAnalyze.size() + " remaining) ...");
                }
            }

            // analyze
            int candidatesToAnalyze = Math.min(milestoneCandidatesToAnalyze.size(), MAX_CANDIDATES_TO_ANALYZE);
            for (int i = 0; i < candidatesToAnalyze; i++) {
                // allow the thread to be interrupted
                if (Thread.currentThread().isInterrupted()) {
                    return;
                }

                Hash candidateTransactionHash = milestoneCandidatesToAnalyze.pollFirst();
                if(analyzeMilestoneCandidate(candidateTransactionHash) == INCOMPLETE) {
                    seenMilestoneCandidates.remove(candidateTransactionHash);
                }
            }

            // once all candidates have been processed we set the milestone tracker to initialized
            if (milestoneCandidatesToAnalyze.size() == 0 && !initialized) {
                initialized = true;

                log.info("Processing milestone candidates ... [DONE]").triggerOutput(true);
            }
        } catch (Exception e) {
            log.error("error while analyzing the milestone candidates", e);
        }
    }

}
