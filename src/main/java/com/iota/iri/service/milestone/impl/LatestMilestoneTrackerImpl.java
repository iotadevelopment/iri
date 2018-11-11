package com.iota.iri.service.milestone.impl;

import com.iota.iri.MilestoneTracker;
import com.iota.iri.conf.IotaConfig;
import com.iota.iri.controllers.AddressViewModel;
import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.hash.SpongeFactory;
import com.iota.iri.model.Hash;
import com.iota.iri.model.HashFactory;
import com.iota.iri.service.milestone.LatestMilestoneTracker;
import com.iota.iri.service.milestone.MilestoneService;
import com.iota.iri.service.milestone.MilestoneSolidifier;
import com.iota.iri.service.milestone.MilestoneValidity;
import com.iota.iri.service.snapshot.SnapshotProvider;
import com.iota.iri.service.snapshot.SnapshotService;
import com.iota.iri.storage.Tangle;
import com.iota.iri.utils.log.Logger;
import com.iota.iri.utils.log.interval.IntervalLogger;
import com.iota.iri.utils.thread.DedicatedScheduledExecutorService;
import com.iota.iri.utils.thread.SilentScheduledExecutorService;
import com.iota.iri.zmq.MessageQ;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.iota.iri.service.milestone.MilestoneValidity.INCOMPLETE;
import static com.iota.iri.service.milestone.MilestoneValidity.INVALID;
import static com.iota.iri.service.milestone.MilestoneValidity.VALID;

public class LatestMilestoneTrackerImpl implements LatestMilestoneTracker {
    private static final int MAX_CANDIDATES_TO_ANALYZE = 10000;

    private static final int RESCAN_INTERVAL = 5000;

    private static final IntervalLogger log = new IntervalLogger(LatestMilestoneTrackerImpl.class);

    private final SilentScheduledExecutorService lastMilestoneTrackerExecutorService =
            new DedicatedScheduledExecutorService("Latest Milestone Tracker", LoggerFactory.getLogger(LatestMilestoneTrackerImpl.class));

    private final Tangle tangle;

    private final SnapshotProvider snapshotProvider;

    private final SnapshotService snapshotService;

    private final MilestoneService milestoneService;

    private final MilestoneSolidifier milestoneSolidifier;

    private final MessageQ messageQ;

    private final IotaConfig config;

    private final Hash coordinatorAddress;

    private final Set<Hash> seenMilestoneCandidates = new HashSet<>();

    private final Deque<Hash> milestoneCandidatesToAnalyze = new ArrayDeque<>();

    private int latestMilestoneIndex;

    private Hash latestMilestoneHash;

    /**
     * The current status of the {@link MilestoneTracker}.
     */
    private boolean initialized = false;

    public LatestMilestoneTrackerImpl(Tangle tangle, SnapshotProvider snapshotProvider, SnapshotService snapshotService, MilestoneService milestoneService, MilestoneSolidifier milestoneSolidifier, MessageQ messageQ, IotaConfig config) {
        this.tangle = tangle;
        this.snapshotProvider = snapshotProvider;
        this.snapshotService = snapshotService;
        this.milestoneService = milestoneService;
        this.milestoneSolidifier = milestoneSolidifier;
        this.messageQ = messageQ;
        this.config = config;
        coordinatorAddress = HashFactory.ADDRESS.create(config.getCoordinator());

        this.latestMilestoneIndex = snapshotProvider.getLatestSnapshot().getIndex();
        this.latestMilestoneHash = snapshotProvider.getLatestSnapshot().getHash();
    }

    @Override
    public void setLatestMilestoneIndex(int latestMilestoneIndex) {
        this.latestMilestoneIndex = latestMilestoneIndex;
    }

    @Override
    public int getLatestMilestoneIndex() {
        return latestMilestoneIndex;
    }

    @Override
    public void setLatestMilestoneHash(Hash latestMilestoneHash) {
        this.latestMilestoneHash = latestMilestoneHash;
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

            switch (milestoneService.validateMilestone(tangle, snapshotProvider, snapshotService, config, potentialMilestoneTransaction, SpongeFactory.Mode.CURLP27, 1)) {
                case VALID:
                    if (milestoneIndex > latestMilestoneIndex) {
                        messageQ.publish("lmi %d %d", latestMilestoneIndex, milestoneIndex);
                        log.delegate().info("Latest milestone has changed from #" + latestMilestoneIndex + " to #" + milestoneIndex);

                        latestMilestoneHash = potentialMilestoneTransaction.getHash();
                        latestMilestoneIndex = milestoneIndex;
                    } else {
                        MilestoneViewModel latestMilestoneViewModel = MilestoneViewModel.latest(tangle);
                        if (latestMilestoneViewModel.index() > latestMilestoneIndex) {
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
    public boolean isInitialized() {
        return initialized;
    }

    @Override
    public void start() {
        lastMilestoneTrackerExecutorService.silentScheduleWithFixedDelay(this::latestMilestoneTrackerThread, 0,
                RESCAN_INTERVAL, TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
        lastMilestoneTrackerExecutorService.shutdownNow();
    }

    private void latestMilestoneTrackerThread() {
        try {
            // log how many milestone candidates are remaining to be analyzed
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
                    milestoneCandidatesToAnalyze.addLast(hash);
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
                    milestoneCandidatesToAnalyze.addLast(candidateTransactionHash);
                }
            }

            // once all candidates have been processed we set the milestone tracker to initialized
            if (milestoneCandidatesToAnalyze.size() == 0 && !initialized) {
                initialized = true;
            }
        } catch (Exception e) {
            log.error("error while analyzing the milestone candidates", e);
        }
    }

}
