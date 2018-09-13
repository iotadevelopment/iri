package com.iota.iri.service.snapshot;

import com.iota.iri.model.Hash;

import java.util.HashMap;
import java.util.Map;

/**
 * Instances of this class represent a collection of balance changes that can be applied to modify the ledger state.
 */
public class SnapshotStateDiff {
    /**
     * Holding a map of addresses associated to their corresponding balance change.
     */
    protected final Map<Hash, Long> diff;

    /**
     * The constructor of this class makes a copy of the provided map and stores it in its internal property.
     *
     * This allows us to work with the provided balance changes without having to worry about modifications of the
     * passed in map that happens outside of the SnapshotStateDiff logic.
     *
     * @param diff map with the addresses and their balance changes
     */
    public SnapshotStateDiff(Map<Hash, Long> diff) {
        this.diff = new HashMap<>(diff);
    }

    /**
     * This method checks if the diff is consistent.
     *
     * Consistent means that the sum of all changes is exactly 0, since IOTAs can only be moved from one address to
     * another, but not created and not destroyed.
     *
     * @return true if the sum of all balances is 0
     */
    public boolean isConsistent() {
        return diff.entrySet().stream().map(Map.Entry::getValue).reduce(Math::addExact).orElse(0L).equals(0L);
    }
}
