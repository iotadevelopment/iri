package com.iota.iri.service.snapshot;

import com.iota.iri.model.Hash;

import java.util.HashMap;
import java.util.Map;

public class SnapshotStateDiff {
    /**
     * Underlying Map storing the balances of addresses.
     */
    protected final Map<Hash, Long> diff;

    public SnapshotStateDiff(Map<Hash, Long> diff) {
        this.diff = new HashMap<>(diff);
    }

    public boolean isConsistent() {
        return diff.entrySet().stream().map(Map.Entry::getValue).reduce(Math::addExact).orElse(0L).equals(0L);
    }
}
