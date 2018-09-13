package com.iota.iri.service.snapshot;

import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.controllers.StateDiffViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.storage.Tangle;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * This class represents a "snapshot" of the ledger at a given time.
 *
 * A complete snapshot of the ledger consists out of the current {@link SnapshotState} which holds the state and its
 * {@link SnapshotMetaData} which holds several information about the snapshot like its timestamp, its corresponding
 * milestone index and so on.
 */
public class Snapshot {
    /**
     * Lock object allowing to block access to this object from different threads.
     */
    protected final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    /**
     * Holds a reference to the state of this snapshot.
     */
    protected final SnapshotState state;

    /**
     * Holds a reference to the metadata of this snapshot.
     */
    protected final SnapshotMetaData metaData;

    /**
     * Constructor of the Snapshot class.
     *
     * It simply saves the passed parameters in its private properties.
     *
     * @param state the state of the Snapshot containing all its balances
     * @param metaData the metadata of the Snapshot containing its milestone index and other properties
     */
    public Snapshot(SnapshotState state, SnapshotMetaData metaData) {
        this.state = state;
        this.metaData = metaData;
    }

    /**
     * Locks the complete Snapshot object for read access.
     *
     * It sets the corresponding locks in all child objects and therefore locks the whole object in its current state.
     * This is used to synchronize the access from different Threads, if both members need to be read.
     *
     * A more fine-grained control over the locks can be achieved by invoking the lock methods in the child objects
     * themselves.
     */
    public void lockRead() {
        readWriteLock.readLock().lock();
    }

    /**
     * Locks the complete Snapshot object for write access.
     *
     * It sets the corresponding locks in all child objects and therefore locks the whole object in its current state.
     * This is used to synchronize the access from different Threads, if both members need to be modified.
     *
     * A more fine-grained control over the locks can be achieved by invoking the lock methods in the child objects
     * themselves.
     */
    public void lockWrite() {
        readWriteLock.writeLock().lock();
    }

    /**
     * Unlocks the complete Snapshot object from read blocks.
     *
     * It sets the corresponding unlocks in all child objects and therefore unlocks the whole object. This is used to
     * synchronize the access from different Threads, if both members needed to be read.
     *
     * A more fine-grained control over the locks can be achieved by invoking the lock methods in the child objects
     * themselves.
     */
    public void unlockRead() {
        readWriteLock.readLock().unlock();
    }

    /**
     * Unlocks the complete Snapshot object from write blocks.
     *
     * It sets the corresponding unlocks in all child objects and therefore unlocks the whole object. This is used to
     * synchronize the access from different Threads, if both members needed to be modified.
     *
     * A more fine-grained control over the locks can be achieved by invoking the lock methods in the child objects
     * themselves.
     */
    public void unlockWrite() {
        readWriteLock.writeLock().unlock();
    }

    /**
     * This method creates a deep clone of the Snapshot object.
     *
     * It can be used to make a copy of the object, that then can be modified without affecting the original object.
     *
     * @return deep copy of the original object
     */
    public Snapshot clone() {
        // lock the object for reading
        lockRead();

        // create the clone
        try {
            return new Snapshot(state.clone(), metaData.clone());
        }

        // unlock the object
        finally {
            unlockRead();
        }
    }

    // UTILITY METHODS /////////////////////////////////////////////////////////////////////////////////////////////////

    public Hash getInitialHash() {
        lockRead();

        try {
            return metaData.initialHash;
        } finally {
            unlockRead();
        }
    }

    public int getInitialIndex() {
        lockRead();

        try {
            return metaData.initialIndex;
        } finally {
            unlockRead();
        }
    }

    public long getInitialTimestamp() {
        lockRead();

        try {
            return metaData.initialTimestamp;
        } finally {
            unlockRead();
        }
    }

    public Hash getHash() {
        lockRead();

        try {
            return this.metaData.getHash();
        } finally {
            unlockRead();
        }
    }

    public void setHash(Hash hash) {
        lockRead();

        try {
            metaData.setHash(hash);
        } finally {
            unlockRead();
        }
    }

    /**
     * This method updates both - the state and the index - in a single call.
     *
     * It first locks both child objects and then performs the corresponding updates. It is used by the MilestoneTracker
     * to update the state after a milestone appeared.
     *
     * @param diff change in the state
     * @param newIndex new milestone index
     */
    public void update(SnapshotStateDiff diff, int newIndex, Hash newTransactionHash) {
        // check the diff before we apply the update
        if(!diff.isConsistent()) {
            throw new IllegalStateException("the snapshot state diff is not consistent");
        }

        // prevent other threads to write to this object while we do the updates
        lockWrite();

        // apply our changes without locking the underlying members (we already locked globally)
        try {
            state.applyStateDiff(diff);
            metaData.setIndex(newIndex);
            metaData.setHash(newTransactionHash);
            metaData.setTimestamp(System.currentTimeMillis() / 1000L);
        }

        // unlock the access to this object once we are done updating
        finally {
            unlockWrite();
        }
    }

    /**
     * This method rolls back the latest milestones until it reaches the state that the snapshot had before applying
     * the milestone indicated by the given parameter.
     *
     * After checking the validity of the parameters we simply roll back the last milestone until we reach a point that
     *
     * @param targetMilestoneIndex
     * @param tangle
     */
    public void rollBackMilestones(int targetMilestoneIndex, Tangle tangle) throws SnapshotException {
        if(targetMilestoneIndex <= getInitialIndex()) {
            throw new SnapshotException("the target milestone index is lower than the initial snapshot index - cannot revert back to an unknown milestone");
        }

        if(targetMilestoneIndex > getIndex()) {
            throw new SnapshotException("the target milestone index is higher than the current one - consider using replayMilestones instead");
        }

        lockWrite();

        try {
            while (targetMilestoneIndex <= getIndex() && rollbackLastMilestone(tangle)) {
                /* do nothing but rollback */
            }
        } finally {
            unlockWrite();
        }
    }

    private HashSet<Integer> skippedMilestones = new HashSet<>();

    public boolean rollbackLastMilestone(Tangle tangle) throws SnapshotException {
        lockWrite();

        try {
            if(getIndex() == getInitialIndex()) {
                return false;
            }

            // revert the last balance changes
            StateDiffViewModel stateDiffViewModel = StateDiffViewModel.load(tangle, getHash());
            if(stateDiffViewModel != null && !stateDiffViewModel.isEmpty()) {
                SnapshotStateDiff snapshotStateDiff = new SnapshotStateDiff(
                    stateDiffViewModel.getDiff().entrySet().stream().map(
                        hashLongEntry -> new HashMap.SimpleEntry<>(
                            hashLongEntry.getKey(), -1 * hashLongEntry.getValue()
                        )
                    ).collect(
                        Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)
                    )
                );

                if (!snapshotStateDiff.isConsistent()) {
                    throw new SnapshotException("the StateDiff belonging to milestone #" + getIndex() + " (" + getHash().toString() + ") is inconsistent");
                } else if (!state.patchedState(snapshotStateDiff).isConsistent()) {
                    throw new SnapshotException("Snapshot would be inconsistent after applying patch belonging to milestone #" + getIndex() + " (" + getHash().toString() + ")");
                }

                state.applyStateDiff(snapshotStateDiff);
            }

            // jump skipped milestones
            int currentIndex = getIndex();
            while(skippedMilestones.remove(--currentIndex)) {
                /* do nothing */
            }

            // check if we arrived at the start
            if(currentIndex <= getInitialIndex()) {
                metaData.setIndex(getInitialIndex());
                metaData.setHash(getInitialHash());
                metaData.setTimestamp(getInitialTimestamp());

                return true;
            }

            // otherwise set metadata of the previous milestone
            MilestoneViewModel currentMilestone = MilestoneViewModel.get(tangle, currentIndex);
            metaData.setIndex(currentMilestone.index());
            metaData.setHash(currentMilestone.getHash());
            metaData.setTimestamp(TransactionViewModel.fromHash(tangle, currentMilestone.getHash()).getTimestamp());

            return true;
        } catch (Exception e) {
            throw new SnapshotException("failed to rollback last milestone", e);
        } finally {
            unlockWrite();
        }
    }

    public void replayMilestones(int targetMilestoneIndex, Tangle tangle) throws SnapshotException {
        lockWrite();

        try {
            for (int currentMilestoneIndex = getIndex() + 1; currentMilestoneIndex <= targetMilestoneIndex; currentMilestoneIndex++) {
                MilestoneViewModel currentMilestone = MilestoneViewModel.get(tangle, currentMilestoneIndex);
                if (currentMilestone != null) {
                    StateDiffViewModel stateDiffViewModel = StateDiffViewModel.load(tangle, currentMilestone.getHash());
                    if(stateDiffViewModel != null && !stateDiffViewModel.isEmpty()) {
                        state.applyStateDiff(new SnapshotStateDiff(stateDiffViewModel.getDiff()));
                    }

                    metaData.setIndex(currentMilestone.index());
                    metaData.setHash(currentMilestone.getHash());
                    TransactionViewModel currentMilestoneTransaction = TransactionViewModel.fromHash(tangle, currentMilestone.getHash());
                    if(currentMilestoneTransaction != null && currentMilestoneTransaction.getType() != TransactionViewModel.PREFILLED_SLOT) {
                        metaData.setTimestamp(currentMilestoneTransaction.getTimestamp());
                    }
                } else {
                    skippedMilestones.add(currentMilestoneIndex);
                }
            }
        } catch (Exception e) {
            throw new SnapshotException("failed to completely replay the the state of the ledger", e);
        } finally {
            unlockWrite();
        }
    }

    /**
     * This is a utility method for retrieving the solid entry points.
     *
     * Even tho the solid entry points are not directly stored in this object, we offer the ability to read them from
     * the Snapshot itself, without having to retrieve the metadata first. This is mainly to keep the code more
     * readable, without having to manually traverse the necessary references.
     *
     * @return set of transaction hashes that shall be considered solid when being referenced
     */
    public HashMap<Hash, Integer> getSolidEntryPoints() {
        lockRead();

        try {
            return metaData.getSolidEntryPoints();
        } finally {
            unlockRead();
        }
    }

    public void setSeenMilestones(HashMap<Hash, Integer> seenMilestones) {
        lockWrite();

        try {
            metaData.setSeenMilestones(seenMilestones);
        } finally {
            unlockWrite();
        }
    }

    public HashMap<Hash, Integer> getSeenMilestones() {
        lockRead();

        try {
            return metaData.getSeenMilestones();
        } finally {
            unlockRead();
        }
    }

    public boolean hasSolidEntryPoint(Hash solidEntrypoint) {
        return metaData.solidEntryPoints.containsKey(solidEntrypoint);
    }

    /**
     * This is a utility method for determining if a given hash is a solid entry point.
     *
     * Even tho the balance is not directly stored in this object, we offer the ability to read the balance from the
     * Snapshot itself, without having to retrieve the state first. This is mainly to keep the code more readable,
     * without having to manually traverse the necessary references.
     *
     * @param transactionHash hash of the referenced transaction that shall be checked
     * @return true if it is a solid entry point and false otherwise
     */
    public boolean isSolidEntryPoint(Hash transactionHash) {
        lockRead();

        try {
            return metaData.hasSolidEntryPoint(transactionHash);
        } finally {
            unlockRead();
        }
    }

    public int getSolidEntryPointIndex(Hash solidEntrypoint) {
        lockRead();

        try {
            return metaData.getSolidEntryPointIndex(solidEntrypoint);
        } finally {
            unlockRead();
        }
    }

    /**
     * This is a utility method for determining the balance of an address.
     *
     * Even tho the balance is not directly stored in this object, we offer the ability to read the balance from the
     * Snapshot itself, without having to retrieve the state first. This is mainly to keep the code more readable,
     * without having to manually traverse the necessary references.
     *
     * @param hash address that we want to check
     * @return the balance of the given address
     */
    public long getBalance(Hash hash) {
        lockRead();

        try {
            return state.getBalance(hash);
        } finally {
            unlockRead();
        }
    }

    /**
     * This is a utility method for determining the index of the snapshot, with optionally locking the underlying
     * metadata object.
     *
     * Even though the index is not directly stored in this object, we offer the ability to read it from the Snapshot
     * itself, without having to retrieve the metadata first. This is mainly to keep the code more readable, without
     * having to manually traverse the necessary references.
     *
     * @return the milestone index of the snapshot
     */
    public int getIndex() {
        return metaData.getIndex();
    }

    /**
     * This is a utility method for determining the index of the snapshot, with optionally locking the underlying
     * metadata object.
     *
     * Even though the index is not directly stored in this object, we offer the ability to read it from the Snapshot
     * itself, without having to retrieve the metadata first. This is mainly to keep the code more readable, without
     * having to manually traverse the necessary references.
     *
     * @return the milestone index of the snapshot
     */
    public void setIndex(int index) {
        lockWrite();

        try {
            metaData.setIndex(index);
        } finally {
            unlockWrite();
        }
    }

    /**
     * This is a utility method for determining the timestamp of the snapshot, with optionally locking the underlying
     * metadata object first.
     *
     * Even though the timestamp is not directly stored in this object, we offer the ability to read it from the
     * Snapshot itself, without having to retrieve the metadata first. This is mainly to keep the code more readable,
     * without having to manually traverse the necessary references.
     *
     * @return the timestamp when the snapshot was updated or created
     */
    public long getTimestamp() {
        lockRead();

        try {
            return metaData.getTimestamp();
        } finally {
            unlockRead();
        }
    }

    /**
     * This is a utility method for determining the timestamp of the snapshot, with optionally locking the underlying
     * metadata object first.
     *
     * Even though the timestamp is not directly stored in this object, we offer the ability to read it from the
     * Snapshot itself, without having to retrieve the metadata first. This is mainly to keep the code more readable,
     * without having to manually traverse the necessary references.
     *
     * @return the timestamp when the snapshot was updated or created
     */
    public void setTimestamp(long timestamp) {
        lockWrite();

        try {
            metaData.setTimestamp(timestamp);
        } finally {
            unlockWrite();
        }
    }

    public void setSolidEntryPoints(HashMap<Hash, Integer> solidEntryPoints) {
        lockWrite();

        try {
            metaData.setSolidEntryPoints(solidEntryPoints);
        } finally {
            unlockWrite();
        }
    }

    public boolean isConsistent() {
        lockRead();

        try {
            return state.isConsistent();
        } finally {
            unlockRead();
        }
    }

    public boolean hasCorrectSupply() {
        lockRead();

        try {
            return state.hasCorrectSupply();
        } finally {
            unlockRead();
        }
    }

    public SnapshotState patchedState(SnapshotStateDiff snapshotStateDiff) {
        lockRead();

        try {
            return state.patchedState(snapshotStateDiff);
        } finally {
            unlockRead();
        }
    }
}
