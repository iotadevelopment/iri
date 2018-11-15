package com.iota.iri.service.milestone.impl;

import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.service.ledger.LedgerService;
import com.iota.iri.service.milestone.LatestMilestoneTracker;
import com.iota.iri.service.milestone.MilestoneException;
import com.iota.iri.service.milestone.MilestoneService;
import com.iota.iri.service.milestone.LatestSolidMilestoneTracker;
import com.iota.iri.service.snapshot.Snapshot;
import com.iota.iri.service.snapshot.SnapshotProvider;
import com.iota.iri.storage.Tangle;
import com.iota.iri.utils.log.interval.IntervalLogger;
import com.iota.iri.utils.thread.DedicatedScheduledExecutorService;
import com.iota.iri.utils.thread.SilentScheduledExecutorService;
import com.iota.iri.zmq.MessageQ;

import java.util.concurrent.TimeUnit;

/**
 * This class implements the basic contract of the {@link LatestSolidMilestoneTracker} and extends it with mechanisms to
 * recover from database corruptions by incorporating a backoff strategy that reverts the changes introduced by previous
 * milestones whenever an error is detected until the problem causing milestone was found.<br />
 */
public class LatestSolidMilestoneTrackerImpl implements LatestSolidMilestoneTracker {
    /**
     * Holds the interval (in milliseconds) in which the {@link #checkForNewLatestSolidMilestones()} method gets
     * called by the background worker.<br />
     */
    private static final int RESCAN_INTERVAL = 5000;

    /**
     * Holds the logger of this class (a rate limited logger than doesn't spam the CLI output).<br />
     */
    private static final IntervalLogger log = new IntervalLogger(LatestSolidMilestoneTrackerImpl.class);

    /**
     * Holds the Tangle object which acts as a database interface.<br />
     */
    private final Tangle tangle;

    /**
     * The snapshot provider which gives us access to the relevant snapshots that the node uses (for the ledger
     * state).<br />
     */
    private final SnapshotProvider snapshotProvider;

    /**
     * Holds a reference to the service instance containing the business logic of the milestone package.<br />
     */
    private final MilestoneService milestoneService;

    /**
     * Holds a reference to the manager that keeps track of the latest milestone.<br />
     */
    private final LatestMilestoneTracker latestMilestoneTracker;

    /**
     * Holds a reference to the service that contains the logic for applying milestones to the ledger state.<br />
     */
    private final LedgerService ledgerService;

    /**
     * Holds a reference to the ZeroMQ interface that allows us to emit messages for external recipients.<br />
     */
    private final MessageQ messageQ;

    /**
     * Holds a reference to the manager of the background worker.<br />
     */
    private final SilentScheduledExecutorService executorService = new DedicatedScheduledExecutorService(
            "Latest Solid Milestone Tracker", log.delegate());

    /**
     * Holds the milestone index of the milestone that caused the repair logic to get started.<br />
     */
    private int errorCausingMilestoneIndex = Integer.MAX_VALUE;

    /**
     * Counter for the backoff repair strategy (see {@link #revertPrecedingMilestones(MilestoneViewModel)}.<br />
     */
    private int repairBackoffCounter = 0;

    /**
     * Creates a manager that keeps track of the latest solid milestones and that triggers the application of these
     * milestones and their corresponding balance changes to the latest {@link Snapshot} by incorporating a background
     * worker that periodically checks for new solid milestones.<br />
     * <br />
     * We simply store the passed in dependencies in their corresponding properties.<br />
     *
     * @param tangle Tangle object which acts as a database interface
     * @param snapshotProvider manager for the snapshots that allows us to retrieve the relevant snapshots of this node
     * @param milestoneService contains the important business logic when dealing with milestones
     * @param latestMilestoneTracker the manager that keeps track of the latest milestone
     * @param ledgerService the manager for
     * @param messageQ ZeroMQ interface that allows us to emit messages for external recipients
     */
    public LatestSolidMilestoneTrackerImpl(Tangle tangle, SnapshotProvider snapshotProvider,
            MilestoneService milestoneService, LedgerService ledgerService,
            LatestMilestoneTracker latestMilestoneTracker, MessageQ messageQ) {

        this.tangle = tangle;
        this.snapshotProvider = snapshotProvider;
        this.milestoneService = milestoneService;
        this.ledgerService = ledgerService;
        this.latestMilestoneTracker = latestMilestoneTracker;
        this.messageQ = messageQ;
    }

    @Override
    public void start() {
        executorService.silentScheduleWithFixedDelay(this::latestSolidMilestoneTrackerThread, 0, RESCAN_INTERVAL,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public void shutdown() {
        executorService.shutdownNow();
    }

    /**
     * {@inheritDoc}
     * <br />
     * In addition to applying the found milestones to the ledger state it also issues log messages and keeps the
     * {@link LatestMilestoneTracker} in sync (if we happen to process a new latest milestone faster).<br />
     */
    @Override
    public void checkForNewLatestSolidMilestones() throws MilestoneException {
        try {
            int currentSolidMilestoneIndex = snapshotProvider.getLatestSnapshot().getIndex();
            if (currentSolidMilestoneIndex < latestMilestoneTracker.getLatestMilestoneIndex()) {
                MilestoneViewModel nextMilestone;
                while (!Thread.currentThread().isInterrupted() &&
                        (nextMilestone = MilestoneViewModel.get(tangle, currentSolidMilestoneIndex + 1)) != null &&
                        TransactionViewModel.fromHash(tangle, nextMilestone.getHash()).isSolid()) {

                    syncLatestMilestoneTracker(nextMilestone);
                    applySolidMilestoneToLedger(nextMilestone);
                    logChange(currentSolidMilestoneIndex);

                    currentSolidMilestoneIndex = snapshotProvider.getLatestSnapshot().getIndex();
                }
            }
        } catch (Exception e) {
            throw new MilestoneException(e);
        }
    }

    /**
     * This method contains the logic for the background worker.<br />
     * <br />
     * It simply calls {@link #checkForNewLatestSolidMilestones()} and wraps with a log handler that prevents the {@link
     * MilestoneException} to crash the worker.<br />
     */
    private void latestSolidMilestoneTrackerThread() {
        try {
            checkForNewLatestSolidMilestones();
        } catch (MilestoneException e) {
            log.error("error while updating the solid milestone", e);
        }
    }

    /**
     * This method tries to apply the given milestone to the ledger.<br />
     * <br />
     * If the application of the milestone fails, we start a repair routine which will revert the milestones preceding
     * our current milestone and consequently try to reapply them in the next iteration of the {@link
     * #checkForNewLatestSolidMilestones()} method (until the problem is solved).<br />
     *
     * @param milestone the milestone that shall be applied to the ledger state
     * @throws Exception if anything unexpected goes wrong while applying the milestone to the ledger
     */
    private void applySolidMilestoneToLedger(MilestoneViewModel milestone) throws Exception {
        if (!ledgerService.applyMilestoneToLedger(milestone)) {
            revertPrecedingMilestones(milestone);
        } else {
            resetRepairBackoffCounterIfProblemSolved(milestone);
        }
    }

    /**
     * This method resets the internal variables that are used to keep track of the repair process.<br />
     * <br />
     * It gets called whenever we advance to a milestone that has a higher milestone index than the milestone that
     * initially caused the repair routine to kick in (see {@link #revertPrecedingMilestones(MilestoneViewModel)}.<br />
     *
     * @param processedMilestone the milestone that currently gets processed
     */
    private void resetRepairBackoffCounterIfProblemSolved(MilestoneViewModel processedMilestone) {
        if(repairBackoffCounter != 0 && processedMilestone.index() > errorCausingMilestoneIndex) {
            repairBackoffCounter = 0;
            errorCausingMilestoneIndex = Integer.MAX_VALUE;
        }
    }

    /**
     * This method is a utility method that allows us to keep the {@link LatestMilestoneTracker} in sync with our
     * current progress.<br />
     * <br />
     * Since the {@link LatestMilestoneTracker} scans all old milestones during its startup (which can take a while to
     * finish) it can happen that we see a newer latest milestone faster than this manager.<br />
     * <br />
     * Note: This method ensures that the latest milestone index is always bigger or equals the latest solid milestone
     *       index.
     *
     * @param processedMilestone the milestone that currently gets processed
     */
    private void syncLatestMilestoneTracker(MilestoneViewModel processedMilestone) {
        if(processedMilestone.index() > latestMilestoneTracker.getLatestMilestoneIndex()) {
            latestMilestoneTracker.setLatestMilestone(processedMilestone.getHash(), processedMilestone.index());
        }
    }

    /**
     * This method emits a log message whenever the latest solid milestone changes.<br />
     * <br />
     * It simply compares the current latest milestone index against the previous milestone index and emits the log
     * messages using the {@link #log} and the {@link #messageQ} instances if it differs.<br />
     *
     * @param prevSolidMilestoneIndex the milestone index before the change
     */
    private void logChange(int prevSolidMilestoneIndex) {
        Snapshot latestSnapshot = snapshotProvider.getLatestSnapshot();
        int latestMilestoneIndex = latestSnapshot.getIndex();
        Hash latestMilestoneHash = latestSnapshot.getHash();

        if (prevSolidMilestoneIndex != latestMilestoneIndex) {
            log.info("Latest SOLID milestone index changed from #" + prevSolidMilestoneIndex + " to #" + latestMilestoneIndex);

            messageQ.publish("lmsi %d %d", prevSolidMilestoneIndex, latestMilestoneIndex);
            messageQ.publish("lmhs %s", latestMilestoneHash);
        }
    }

    /**
     * This method tries to actively repair the ledger by reverting the milestones preceding the given milestone.<br />
     * <br />
     * It gets called when a milestone could not be applied to the ledger state because of problems like "inconsistent
     * balances". While this should theoretically never happen (because milestones are by definition "consistent"), it
     * can still happen because IRI crashed or got stopped in the middle of applying a milestone or if a milestone
     * was processed in the wrong order.<br />
     * <br />
     * Every time we call this method the internal {@link #repairBackoffCounter} gets increased which causes the next
     * call of this method to repair an additional milestone. This means that whenever we face an error we first try to
     * reset only the last milestone, then the two last milestones, then the three last milestones (and so on ...) until
     * the problem was fixed.<br />
     * <br />
     * To be able to tell when the problem is fixed and the {@link #repairBackoffCounter} can be reset, we store the
     * milestone index that caused the problem the first time we call this method.<br />
     *
     * @param errorCausingMilestone the milestone that failed to be applied
     * @throws MilestoneException if we failed to reset the corrupted milestone
     */
    private void revertPrecedingMilestones(MilestoneViewModel errorCausingMilestone) throws MilestoneException {
        if(repairBackoffCounter++ == 0) {
            errorCausingMilestoneIndex = errorCausingMilestone.index();
        }

        for (int i = errorCausingMilestone.index(); i > errorCausingMilestone.index() - repairBackoffCounter; i--) {
            milestoneService.resetCorruptedMilestone(i);
        }
     }
}
