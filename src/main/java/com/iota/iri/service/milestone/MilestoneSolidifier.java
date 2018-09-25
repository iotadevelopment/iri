package com.iota.iri.service.milestone;

import com.iota.iri.TransactionValidator;
import com.iota.iri.model.Hash;
import com.iota.iri.service.snapshot.SnapshotManager;
import com.iota.iri.utils.thread.ThreadIdentifier;
import com.iota.iri.utils.thread.ThreadUtils;
import com.iota.iri.utils.log.StatusLogger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class implements the logic for solidifying unsolid milestones.
 *
 * It manages a map of unsolid milestones to collect all milestones that have to be solidified. It then periodically
 * issues checkSolidity calls on the earliest milestone to solidify it.
 *
 * To save resources and make the call a little bit more efficient, we cache the earliest milestone in private class
 * properties so the relatively expensive task of having to search for the earliest milestone in the map only has to be
 * performed after the earliest milestone has become solid or irrelevant for our node.
 */
public class MilestoneSolidifier {
    private static final int SOLIDIFICATION_QUEUE_SIZE = 10;

    /**
     * Defines the interval in which solidity checks are issued (in milliseconds).
     */
    private static final int SOLIDIFICATION_INTERVAL = 100;

    /**
     * Defines the maximum amount of transactions that are allowed to get processed while trying to solidify a milestone.
     */
    private static final int SOLIDIFICATION_TRANSACTIONS_LIMIT = 20000;

    /**
     * Defines after how many solidification attempts we increase the {@link #SOLIDIFICATION_TRANSACTIONS_LIMIT}.
     */
    private static final int SOLIDIFICATION_TRANSACTIONS_LIMIT_INCREMENT_INTERVAL = 50;

    /**
     * Defines how often we can at maximum increase the {@link #SOLIDIFICATION_TRANSACTIONS_LIMIT}.
     */
    private static final int SOLIDIFICATION_TRANSACTIONS_LIMIT_MAX_INCREMENT = 5;

    /**
     * Logger for this class allowing us to dump debug and status messages.
     */
    private final StatusLogger statusLogger = new StatusLogger(LoggerFactory.getLogger(MilestoneSolidifier.class));

    /**
     * Holds a reference to the SnapshotManager which allows us to check if milestones are still relevant.
     */
    private SnapshotManager snapshotManager;

    /**
     * Holds a reference to the TransactionValidator which allows us to issue solidity checks.
     */
    private TransactionValidator transactionValidator;

    /**
     * List of unsolid milestones where we collect the milestones that shall be solidified.
     */
    private ConcurrentHashMap<Hash, Integer> unsolidMilestonesPool = new ConcurrentHashMap<>();

    private HashSet<Hash> milestonesToSolidify = new HashSet<>();

    private Hash oldestMilestoneMarker = null;

    /**
     * Holds the amount of solidity checks issued for the earliest milestone.
     */
    private int earliestUnsolidMilestoneSolidificationAttempts = 0;

    /**
     * Holds a reference to the {@link ThreadIdentifier} for the solidification thread.
     *
     * Using a {@link ThreadIdentifier} for spawning the thread allows the {@link ThreadUtils} to spawn exactly one
     * thread for this instance even when we call the {@link #start()} method multiple times.
     */
    private final ThreadIdentifier solidificationThreadIdentifier = new ThreadIdentifier("Milestone Solidifier");

    /**
     * Constructor of the class.
     *
     * It simply stores the passed in parameters to be able to access them later on.
     *
     * @param snapshotManager SnapshotManager instance that is used by the node
     * @param transactionValidator TransactionValidator instance that is used by the node
     */
    public MilestoneSolidifier(SnapshotManager snapshotManager, TransactionValidator transactionValidator) {
        this.snapshotManager = snapshotManager;
        this.transactionValidator = transactionValidator;
    }

    private void determineOldestMilestoneMarker() {
        oldestMilestoneMarker = null;
        for (Hash currentHash : milestonesToSolidify) {
            if (oldestMilestoneMarker == null || unsolidMilestonesPool.get(currentHash) > unsolidMilestonesPool.get(oldestMilestoneMarker)) {
                oldestMilestoneMarker = currentHash;
            }
        }
    }

    private void addToSolidificationQueue(Hash milestoneHash) {
        synchronized (this) {
            // if the the candidate is already selected -> abort
            if (milestonesToSolidify.contains(milestoneHash)) {
                return;
            }

            // if there is enough space -> just add and update the oldest pointer
            if (milestonesToSolidify.size() < SOLIDIFICATION_QUEUE_SIZE) {
                milestonesToSolidify.add(milestoneHash);

                if (oldestMilestoneMarker == null || unsolidMilestonesPool.get(milestoneHash) > unsolidMilestonesPool.get(oldestMilestoneMarker)) {
                    oldestMilestoneMarker = milestoneHash;
                }
            }

            // otherwise replace the oldest milestone if this one is younger
            else if (unsolidMilestonesPool.get(milestoneHash) < unsolidMilestonesPool.get(oldestMilestoneMarker)) {
                milestonesToSolidify.remove(oldestMilestoneMarker);
                milestonesToSolidify.add(milestoneHash);

                determineOldestMilestoneMarker();
            }
        }
    }

    /**
     * This method adds new milestones to our internal map of unsolid ones.
     *
     * It first checks if the passed in milestone is relevant for our node by checking against the initialSnapshot. If
     * it deems the milestone relevant it checks if the passed in milestone appeared earlier than the current one and
     * updates the interval properties accordingly.
     *
     * @param milestoneHash Hash of the milestone that shall be solidified
     * @param milestoneIndex index of the milestone that shall be solidified
     */
    public void add(Hash milestoneHash, int milestoneIndex) {
        if (
            !unsolidMilestonesPool.containsKey(milestoneHash) &&
            milestoneIndex > snapshotManager.getInitialSnapshot().getIndex()
        ) {
            unsolidMilestonesPool.put(milestoneHash, milestoneIndex);

            if(oldestMilestoneMarker == null || milestoneIndex < unsolidMilestonesPool.get(oldestMilestoneMarker)) {
                addToSolidificationQueue(milestoneHash);
            }
        }
    }

    /**
     * This method starts the solidification {@link Thread} that asynchronously solidifies the milestones.
     *
     * This method is thread safe since we use a {@link ThreadIdentifier} to address the {@link Thread}. The
     * {@link ThreadUtils} take care of only launching exactly one {@link Thread} that is not terminated.
     */
    public void start() {
        ThreadUtils.spawnThread(this::milestoneSolidificationThread, solidificationThreadIdentifier);
    }

    /**
     * This method shuts down the solidification thread.
     *
     * It does not actively terminate the thread but sets the isInterrupted flag. Since we use a {@link ThreadIdentifier}
     * to address the {@link Thread}, this method is thread safe.
     */
    public void shutdown() {
        ThreadUtils.stopThread(solidificationThreadIdentifier);
    }

    /**
     * This method contains the logic for the milestone solidification, that gets executed in a separate {@link Thread}.
     *
     * It periodically updates and checks the unsolid milestones by invoking {@link #processSolidificationQueue()}.
     *
     * To allow for faster processing it only "waits" for another check if the current solidification task was not
     * finished (otherwise it immediately "continues" with the next one).
     */
    private void milestoneSolidificationThread() {
        while(!Thread.interrupted()) {
            processSolidificationQueue();

            ThreadUtils.sleep(SOLIDIFICATION_INTERVAL);
        }
    }

    /**
     * This method checks if the current unsolid milestone has become solid or irrelevant and advances to the next one
     * if that is the case.
     *
     * It is getting called by the solidification thread in regular intervals.
     */
    private void processSolidificationQueue() {
        synchronized (this) {
            // process milestones and remove finished ones
            for (Iterator<Hash> iterator = milestonesToSolidify.iterator(); iterator.hasNext(); ) {
                Hash currentHash = iterator.next();

                if (
                    unsolidMilestonesPool.get(currentHash) <= snapshotManager.getInitialSnapshot().getIndex() ||
                    isSolid(currentHash)
                ) {
                    unsolidMilestonesPool.remove(currentHash);
                    iterator.remove();

                    if (currentHash.equals(oldestMilestoneMarker)) {
                        oldestMilestoneMarker = null;
                    }
                }
            }
        }

        // fill up our queue again
        Hash nextSolidificationCandidate;
        while (milestonesToSolidify.size() < SOLIDIFICATION_QUEUE_SIZE && (nextSolidificationCandidate = getNextSolidificationCandidate()) != null) {
            addToSolidificationQueue(nextSolidificationCandidate);
        }

        if(oldestMilestoneMarker == null && milestonesToSolidify.size() >= 1) {
            determineOldestMilestoneMarker();
        }
    }

    /**
     * This method returns the earliest seen Milestone from the internal Map of unsolid milestones.
     *
     * If no unsolid milestone was found it returns an Entry with the Hash being null and the index being
     * Integer.MAX_VALUE.
     *
     * @return the Map.Entry holding the earliest milestone or a default Map.Entry(null, Integer.MAX_VALUE)
     */
    private Hash getNextSolidificationCandidate() {
        Map.Entry<Hash, Integer> nextSolidificationCandidate = null;
        for (Map.Entry<Hash, Integer> milestone : unsolidMilestonesPool.entrySet()) {
            if (
                !milestonesToSolidify.contains(milestone.getKey()) && (
                    nextSolidificationCandidate == null ||
                    milestone.getValue() < nextSolidificationCandidate.getValue()
                )
            ) {
                nextSolidificationCandidate = milestone;
            }
        }

        return nextSolidificationCandidate == null ? null : nextSolidificationCandidate.getKey();
    }

    /**
     * This method performs the actual solidity check on the selected milestone.
     *
     * It first checks if there is a milestone that has to be solidified and then issues the corresponding solidity
     * check. In addition to issuing the solidity checks, it dumps a log message to keep the node operator informed
     * about the progress of solidification.
     *
     * We limit the amount of transactions that may be processed during the solidity check, since we want to solidify
     * from the oldest milestone to the newest one and not "block" the solidification with a very recent milestone that
     * needs to traverse huge chunks of the tangle. If we fail to solidify a milestone for a certain amount of tries, we
     * increase the amount of transactions that may be processed by using an exponential binary backoff strategy. The
     * main goal of this is to give the solidification enough "resources" to discover the previous milestone (if it ever
     * gets stuck because of a very long path to the previous milestone) while at the same time allowing fast solidity
     * checks in "normal conditions".
     *
     * @return true if there are no unsolid milestones that have to be processed or if the earliest milestone is solid
     */
    private boolean isSolid(Hash hash) {
        if (unsolidMilestonesPool.size() > 1) {
            statusLogger.status("Solidifying milestone #" + unsolidMilestonesPool.get(hash) + " [" + milestonesToSolidify.size() + " / " + unsolidMilestonesPool.size() + "]");
        }

        try {
            return transactionValidator.checkSolidity(hash, true, SOLIDIFICATION_TRANSACTIONS_LIMIT * 4);
        } catch (Exception e) {
            statusLogger.error("Error while solidifying milestone #" + unsolidMilestonesPool.get(hash), e);

            return false;
        }
    }
}
