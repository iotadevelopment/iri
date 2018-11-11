package com.iota.iri;

import com.iota.iri.conf.IotaConfig;
import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.model.HashFactory;
import com.iota.iri.network.TransactionRequester;
import com.iota.iri.service.milestone.LatestMilestoneTracker;
import com.iota.iri.service.milestone.MilestoneService;
import com.iota.iri.service.milestone.MilestoneSolidifier;
import com.iota.iri.service.milestone.impl.MilestoneServiceImpl;
import com.iota.iri.service.snapshot.Snapshot;
import com.iota.iri.service.snapshot.SnapshotProvider;
import com.iota.iri.service.snapshot.SnapshotService;
import com.iota.iri.storage.Tangle;
import com.iota.iri.zmq.MessageQ;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class MilestoneTracker {
    private static final Logger log = LoggerFactory.getLogger(MilestoneTracker.class);

    private static final MilestoneService milestoneService = new MilestoneServiceImpl();

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

    private final SnapshotService snapshotService;

    private final LatestMilestoneTracker latestMilestoneTracker;

    private final MilestoneSolidifier milestoneSolidifier;

    private final TransactionRequester transactionRequester;

    private final MessageQ messageQ;

    private final IotaConfig config;

    private final Hash coordinatorAddress;

    private LedgerValidator ledgerValidator;

    private boolean shuttingDown;

    /**
     * The current status of the {@link MilestoneTracker}.
     */
    private Status status = Status.INITIALIZING;

    public MilestoneTracker(Tangle tangle, SnapshotProvider snapshotProvider, SnapshotService snapshotService,
            LatestMilestoneTracker latestMilestoneTracker, MilestoneSolidifier milestoneSolidifier,
            TransactionRequester transactionRequester, MessageQ messageQ, IotaConfig config) {

        this.tangle = tangle;
        this.snapshotProvider = snapshotProvider;
        this.snapshotService = snapshotService;
        this.latestMilestoneTracker = latestMilestoneTracker;
        this.milestoneSolidifier = milestoneSolidifier;
        this.transactionRequester = transactionRequester;
        this.messageQ = messageQ;
        this.config = config;

        //configure
        this.coordinatorAddress = HashFactory.ADDRESS.create(config.getCoordinator());
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
        spawnSolidMilestoneTracker();
        spawnSeenMilestonesRetriever();
    }

    private void spawnSolidMilestoneTracker() {
        (new Thread(() -> {
            log.info("Tracker started.");
            while (!shuttingDown) {
                long scanTime = System.currentTimeMillis();

                try {
                    if(snapshotProvider.getLatestSnapshot().getIndex() < latestMilestoneTracker.getLatestMilestoneIndex()) {
                        updateLatestSolidSubtangleMilestone();
                    }

                    Thread.sleep(Math.max(1, RESCAN_INTERVAL - (System.currentTimeMillis() - scanTime)));
                } catch (final Exception e) {
                    log.error("Error during Solid Milestone updating", e);
                }
            }
        }, "Solid Milestone Tracker")).start();
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

    private int errorCausingMilestone = Integer.MAX_VALUE;

    private int binaryBackoffCounter = 0;

    private void updateLatestSolidSubtangleMilestone() throws Exception {
        // introduce some variables that help us to emit log messages while processing the milestones
        int prevSolidMilestoneIndex = snapshotProvider.getLatestSnapshot().getIndex();
        long lastScan = System.currentTimeMillis();

        // get the next milestone
        MilestoneViewModel nextMilestone = MilestoneViewModel.findClosestNextMilestone(tangle, prevSolidMilestoneIndex, prevSolidMilestoneIndex + 1);

        // while we have a milestone which is solid
        while (!shuttingDown && nextMilestone != null) {
            if(nextMilestone.index() > errorCausingMilestone) {
                System.out.println(errorCausingMilestone + " / " + nextMilestone.index());

                binaryBackoffCounter = 0;
                errorCausingMilestone = Integer.MAX_VALUE;
            }

            // advance to the next milestone if we were able to update the ledger state
            if (ledgerValidator.applyMilestoneToLedger(nextMilestone)) {
                if(nextMilestone.index() > latestMilestoneTracker.getLatestMilestoneIndex()) {
                    latestMilestoneTracker.setLatestMilestone(nextMilestone.getHash(), nextMilestone.index());
                }

                Snapshot latestSnapshot = snapshotProvider.getLatestSnapshot();
                nextMilestone = MilestoneViewModel.findClosestNextMilestone(tangle, latestSnapshot.getIndex(),
                        latestSnapshot.getIndex() + 1);
            } else {
                if (TransactionViewModel.fromHash(tangle, nextMilestone.getHash()).isSolid()) {
                    int currentIndex = nextMilestone.index();
                    int targetIndex = nextMilestone.index() - binaryBackoffCounter;
                    for (int i = currentIndex; i >= targetIndex; i--) {
                        milestoneService.resetCorruptedMilestone(tangle, snapshotProvider, snapshotService, i, "updateLatestSolidSubtangleMilestone");
                    }

                    if(binaryBackoffCounter++ == 0) {
                        errorCausingMilestone = nextMilestone.index();

                        System.out.println(errorCausingMilestone + " / " + nextMilestone.index());
                    }
                }

                nextMilestone = null;
            }

            // dump a log message in intervals and when we terminate
            if(prevSolidMilestoneIndex != snapshotProvider.getLatestSnapshot().getIndex() && (
            System.currentTimeMillis() - lastScan >= STATUS_LOG_INTERVAL || nextMilestone == null
            )) {
                messageQ.publish("lmsi %d %d", prevSolidMilestoneIndex, snapshotProvider.getLatestSnapshot().getIndex());
                messageQ.publish("lmhs %s", snapshotProvider.getLatestSnapshot().getHash());
                log.info("Latest SOLID SUBTANGLE milestone has changed from #"
                         + prevSolidMilestoneIndex + " to #"
                         + snapshotProvider.getLatestSnapshot().getIndex());

                lastScan = System.currentTimeMillis();
                prevSolidMilestoneIndex = snapshotProvider.getLatestSnapshot().getIndex();
            }
        }
    }

    void shutDown() {
        shuttingDown = true;
    }
}
