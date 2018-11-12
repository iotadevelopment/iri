package com.iota.iri;

import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.network.TransactionRequester;
import com.iota.iri.service.milestone.LatestMilestoneTracker;
import com.iota.iri.service.milestone.MilestoneService;
import com.iota.iri.service.snapshot.Snapshot;
import com.iota.iri.service.snapshot.SnapshotProvider;
import com.iota.iri.storage.Tangle;
import com.iota.iri.zmq.MessageQ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class MilestoneTracker {
    private static final Logger log = LoggerFactory.getLogger(MilestoneTracker.class);

    /**
     * Available runtime states of the {@link MilestoneTracker}.
     */
    public enum Status {
        INITIALIZING,
        INITIALIZED
    }

    private static final int RESCAN_INTERVAL = 5000;

    /**
     * How often (in milliseconds) to dump log messages about status updates.
     */
    private static int STATUS_LOG_INTERVAL = 5000;

    private final Tangle tangle;

    private final SnapshotProvider snapshotProvider;

    private final MilestoneService milestoneService;

    private final LatestMilestoneTracker latestMilestoneTracker;

    private final TransactionRequester transactionRequester;

    private final MessageQ messageQ;

    private LedgerValidator ledgerValidator;

    private boolean shuttingDown;

    /**
     * The current status of the {@link MilestoneTracker}.
     */
    private Status status = Status.INITIALIZING;

    public MilestoneTracker(Tangle tangle, SnapshotProvider snapshotProvider, MilestoneService milestoneService,
            LatestMilestoneTracker latestMilestoneTracker, TransactionRequester transactionRequester,
            MessageQ messageQ) {

        this.tangle = tangle;
        this.snapshotProvider = snapshotProvider;
        this.milestoneService = milestoneService;
        this.latestMilestoneTracker = latestMilestoneTracker;
        this.transactionRequester = transactionRequester;
        this.messageQ = messageQ;
    }

    /**
     * This method returns the current status of the {@link MilestoneTracker}.
     *
     * It allows us to determine if all of the "startup" tasks have succeeded.
     *
     * @return the current status of the {@link MilestoneTracker}
     */
    public Status getStatus() {
        return this.status;
    }

    public void start(LedgerValidator ledgerValidator) {
        this.ledgerValidator = ledgerValidator;

        // start the threads
        spawnSeenMilestonesRetriever();
    }

    private void spawnSeenMilestonesRetriever() {
        new Thread(() -> {
            // create a concurrent HashMap so we can remove while we are iterating
            ConcurrentHashMap<Hash, Integer> seenMilestones = new ConcurrentHashMap<>(snapshotProvider.getInitialSnapshot().getSeenMilestones());

            while(!shuttingDown) {
                // retrieve milestones from our local snapshot (if they are still missing)
                seenMilestones.forEach((milestoneHash, milestoneIndex) -> {
                    try {
                        // remove old milestones that are not relevant anymore
                        if(milestoneIndex <= snapshotProvider.getLatestSnapshot().getIndex()) {
                            seenMilestones.remove(milestoneHash);
                        }

                        // check milestones that are within our check range
                        else if(milestoneIndex < snapshotProvider.getLatestSnapshot().getIndex() + 50) {
                            TransactionViewModel milestoneTransaction = TransactionViewModel.fromHash(tangle, milestoneHash);
                            if(milestoneTransaction == null || milestoneTransaction.getType() == TransactionViewModel.PREFILLED_SLOT) {
                                transactionRequester.requestTransaction(milestoneHash, true);
                            } else {
                                seenMilestones.remove(milestoneHash);
                            }
                        }
                    } catch(Exception e) { /* do nothing */ }
                });

                try { Thread.sleep(1000); } catch (InterruptedException e) { e.printStackTrace(); }
            }
        }, "Milestone Solidifier").start();
    }

    void shutDown() {
        shuttingDown = true;
    }
}
