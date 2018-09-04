package com.iota.iri.service.milestone;

import com.iota.iri.TransactionValidator;
import com.iota.iri.model.Hash;
import com.iota.iri.service.snapshot.SnapshotManager;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Comparator.comparingDouble;

/**
 * This class implements the logic for solidifying unsolid milestones.
 *
 * It manages a map of unsolid milestones to collect all milestones that have to be solidifed. It then periodically
 * issues checkSolidity calls on the earliest milestone to solidify it.
 *
 * To save resources and make the call a little bit more efficient, we cache the earliest milestone in protected class
 * properties so the relatively expensive task of having to search for the earliest milestone in the map only has to be
 * performed after the earliest milestone has become solid or irrelevant for our node.
 */
public class MilestoneSolidifier {
    /**
     * Defines the interval in which solidity checks are issued (in milliseconds).
     */
    protected static int SOLIDIFICATION_INTERVAL = 1000;

    /**
     * Holds a reference to the SnapshotManager which allows us to check if milestones are still relevant.
     */
    protected SnapshotManager snapshotManager;

    /**
     * Holds a reference to the TransactionValidator which allows us to issue solidity checks.
     */
    protected TransactionValidator transactionValidator;

    /**
     * List of unsolid milestones where we collect the milestones that shall be solidified.
     */
    protected ConcurrentHashMap<Hash, Integer> unsolidMilestones = new ConcurrentHashMap<>();

    /**
     * The earliest unsolid milestone hash which is the one that is actively tried to get solidified.
     */
    protected Hash earliestMilestoneHash = null;

    /**
     * The earliest unsolid milestone index which is the one that is actively tried to get solidified.
     */
    protected int earliestMilestoneIndex = Integer.MAX_VALUE;

    /**
     * A flag indicating if the solidifier thread is running.
     */
    protected boolean running = false;

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

    /**
     * This method allows to add new unsolid milestones to our internal map.
     *
     * It first checks if the passed in milestone is relevant for our node by checking against the initialSnapshot. If
     * it deems the milestone relevant it checks if the passed in milestone appeared earlier than the current one and
     * updates the interval properties accordingly.
     *
     * @param milestoneHash Hash of the milestone that shall be solidified
     * @param milestoneIndex index of the milestone that shall be solidified
     */
    public void add(Hash milestoneHash, int milestoneIndex) {
        if(milestoneIndex > snapshotManager.getInitialSnapshot().getIndex()) {
            if(milestoneIndex < earliestMilestoneIndex) {
                earliestMilestoneHash = milestoneHash;
                earliestMilestoneIndex = milestoneIndex;
            }

            unsolidMilestones.put(milestoneHash, milestoneIndex);
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
    public Map.Entry<Hash, Integer> getEarliestUnsolidMilestoneEntry() {
        try {
            return Collections.min(unsolidMilestones.entrySet(), comparingDouble(Map.Entry::getValue));
        } catch (NoSuchElementException e) {
            return new AbstractMap.SimpleEntry<>(null, Integer.MAX_VALUE);
        }
    }

    /**
     * This method starts the solidification thread that periodically updates and checks the unsolid milestones.
     *
     * To allow for faster processing it only "waits" for another check if the current solidification task was not
     * finished (otherwise it immediately "continues" with the next one).
     */
    public void start() {
        running = true;

        new Thread(() -> {
            while(running) {
                if(processSolidificationTask() && running) {
                    continue;
                }

                try { Thread.sleep(SOLIDIFICATION_INTERVAL); } catch (InterruptedException e) { /* just stop */ }
            }
        }, "Milestone Solidifier").start();
    }

    /**
     * This method shuts down the solidification thread.
     *
     * It does not actively terminate the thread but sets the running flag to false which will cause the thread to
     * terminate.
     */
    public void shutdown() {
        running = false;
    }

    /**
     * This method removes the current earliest Milestone from the map and sets the internal pointers to the next
     * earliest one.
     *
     * It is used to cycle through the unsolid milestones as they become solid or irrelevant for our node.
     */
    protected void nextEarliestMilestone() {
        unsolidMilestones.remove(earliestMilestoneHash);

        Map.Entry<Hash, Integer> nextEarliestMilestone = getEarliestUnsolidMilestoneEntry();

        earliestMilestoneHash = nextEarliestMilestone.getKey();
        earliestMilestoneIndex = nextEarliestMilestone.getValue();
    }

    /**
     * This method performs the actual solidity check on the selected milestone.
     *
     * It first checks if there is a milestone that has to be solidified and then issues the corresponding solidity
     * check. In addition to issuing the solidity checks, it dumps a log message to keep the node operator informed
     * about the progress of solidification.
     *
     * @return true if there are no unsolid milestones that have to be processed or if the earliest milestone is solid
     */
    protected boolean earliestMilestoneIsSolid() {
        if(earliestMilestoneHash == null) {
            return true;
        }

        System.out.println("Solidifying Milestone #" + earliestMilestoneIndex + " (" + earliestMilestoneHash.toString() + ") [" + unsolidMilestones.size() + " left]");

        try {
            return transactionValidator.checkSolidity(earliestMilestoneHash, true);
        } catch (Exception e) {
            // dump error

            return false;
        }
    }

    /**
     * This method checks if the current unsolid milestone has become solid or irrelevant and advances to the next one
     * if that is the case.
     *
     * It is getting called by the solidification thread in regular intervals.
     *
     * @return true if the current solidification task was successfull and the next milestone is due or false otherwise
     */
    protected boolean processSolidificationTask() {
        if(earliestMilestoneHash != null && (
            earliestMilestoneIndex <= snapshotManager.getInitialSnapshot().getIndex() ||
            earliestMilestoneIsSolid()
        )) {
            nextEarliestMilestone();

            return true;
        }

        return false;
    }
}
