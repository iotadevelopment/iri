package com.iota.iri.service.snapshot;

import com.iota.iri.model.Hash;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class Snapshot {
    /**
     * Holds a reference to the state of this snapshot.
     */
    private SnapshotState state;

    /**
     * Holds a reference to the metadata of this snapshot.
     */
    private SnapshotMetaData metaData;

    /**
     *
     */
    public final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    /**
     * Constructor of the Snapshot class.
     *
     * It takes a
     *
     * @param state
     * @param metaData
     */
    public Snapshot(SnapshotState state, SnapshotMetaData metaData) {
        this.state = state;
        this.metaData = metaData;
    }

    /**
     * This method is the getter of the metadata object.
     *
     * It simply returns the stored private property.
     *
     * @return metadata of this snapshot
     */
    public SnapshotMetaData metaData() {
        return metaData;
    }

    // OLD STUFF (NOT CLEANED UP) //////////////////////////////////////////////////////////////////////////////////////

    private static final Logger log = LoggerFactory.getLogger(Snapshot.class);


    /*
    public static void checkInitialSnapshotIsConsistent(Map<Hash, Long> initialState) {
        if (!isConsistent(initialState)) {
            log.error("Initial Snapshot inconsistent.");
            System.exit(-1);
        }
    }
    */

    /*
    public static void checkStateHasCorrectSupply(Map<Hash, Long> initialState) {
        long stateValue = initialState.values().stream().reduce(Math::addExact).orElse(Long.MAX_VALUE);
        if (stateValue != TransactionViewModel.SUPPLY) {
            log.error("Transaction resolves to incorrect ledger balance: {}", TransactionViewModel.SUPPLY - stateValue);
            System.exit(-1);
        }
    }
    */

    public Snapshot clone() {
        return new Snapshot(state.clone(), metaData.clone());
    }

    public Long getBalance(Hash hash) {
        Long l;
        readWriteLock.readLock().lock();
        l = state.getBalance(hash);
        readWriteLock.readLock().unlock();
        return l;
    }

    public Map<Hash, Long> patchedDiff(Map<Hash, Long> diff) {
        Map<Hash, Long> patch;
        readWriteLock.readLock().lock();
        patch = diff.entrySet().stream().map(hashLongEntry ->
            new HashMap.SimpleEntry<>(hashLongEntry.getKey(), state.getOrDefault(hashLongEntry.getKey(), 0L) + hashLongEntry.getValue())
        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        readWriteLock.readLock().unlock();
        return patch;
    }

    public void apply(Map<Hash, Long> patch, int newIndex) {
        if (!patch.entrySet().stream().map(Map.Entry::getValue).reduce(Math::addExact).orElse(0L).equals(0L)) {
            throw new RuntimeException("Diff is not consistent.");
        }
        readWriteLock.writeLock().lock();
        patch.entrySet().stream().forEach(hashLongEntry -> {
            if (state.computeIfPresent(hashLongEntry.getKey(), (hash, aLong) -> hashLongEntry.getValue() + aLong) == null) {
                state.putIfAbsent(hashLongEntry.getKey(), hashLongEntry.getValue());
            }
        });
        metaData.milestoneIndex(newIndex);
        readWriteLock.writeLock().unlock();
    }
}
