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

public class MilestoneSolidifier {
    private SnapshotManager snapshotManager;

    private TransactionValidator transactionValidator;

    private ConcurrentHashMap<Hash, Integer> unsolidMilestones = new ConcurrentHashMap<>();

    private Hash earliestMilestoneHash = null;

    private int earliestMilestoneIndex = Integer.MAX_VALUE;

    private boolean running = false;

    public MilestoneSolidifier(SnapshotManager snapshotManager, TransactionValidator transactionValidator) {
        this.snapshotManager = snapshotManager;
        this.transactionValidator = transactionValidator;
    }

    public Map.Entry<Hash, Integer> getEarliestUnsolidMilestoneEntry() {
        try {
            return Collections.min(unsolidMilestones.entrySet(), comparingDouble(Map.Entry::getValue));
        } catch (NoSuchElementException e) {
            return new AbstractMap.SimpleEntry<>(null, Integer.MAX_VALUE);
        }
    }

    /**
     * This method discards the current earliest milestone and advances to the next one.
     *
     */
    public void nextEarliestMilestone() {
        unsolidMilestones.remove(earliestMilestoneHash);

        Map.Entry<Hash, Integer> nextEarliestMilestone = getEarliestUnsolidMilestoneEntry();

        earliestMilestoneHash = nextEarliestMilestone.getKey();
        earliestMilestoneIndex = nextEarliestMilestone.getValue();
    }

    public boolean earliestMilestoneIsSolid() {
        System.out.println("Solidifying Milestone #" + earliestMilestoneIndex + " (" + earliestMilestoneHash.toString() + ") [" + unsolidMilestones.size() + " left]");

        try {
            return transactionValidator.checkSolidity(earliestMilestoneHash, true);
        } catch (Exception e) {
            // dump error

            return false;
        }
    }

    public void start() {
        running = true;

        new Thread(() -> {
            while(running) {
                if(earliestMilestoneHash != null && (
                    earliestMilestoneIndex <= snapshotManager.getInitialSnapshot().getIndex() ||
                    earliestMilestoneIsSolid()
                )) {
                    nextEarliestMilestone();

                    continue;
                }

                try { Thread.sleep(1000); } catch (InterruptedException e) { e.printStackTrace(); }
            }
        }, "Milestone Solidifier").start();
    }

    public void shutdown() {
        running = false;
    }

    public MilestoneSolidifier add(Hash milestoneHash, int milestoneIndex) {
        if(milestoneIndex > snapshotManager.getInitialSnapshot().getIndex()) {
            if(milestoneIndex < earliestMilestoneIndex) {
                earliestMilestoneHash = milestoneHash;
                earliestMilestoneIndex = milestoneIndex;
            }

            unsolidMilestones.put(milestoneHash, milestoneIndex);
        }

        return this;
    }
}
