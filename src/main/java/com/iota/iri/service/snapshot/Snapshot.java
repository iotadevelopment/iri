package com.iota.iri.service.snapshot;

import com.iota.iri.model.Hash;

import java.util.HashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Snapshot {
    // CORE FUNCTIONALITY //////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Lock object allowing to block access to this object from different threads.
     */
    public final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    /**
     * Holds a reference to the state of this snapshot.
     */
    private final SnapshotState state;

    /**
     * Holds a reference to the metadata of this snapshot.
     */
    private final SnapshotMetaData metaData;

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

        state.lockRead();
        metaData.lockRead();
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

        state.lockWrite();
        metaData.lockWrite();
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

        state.unlockRead();
        metaData.unlockRead();
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

        state.unlockWrite();
        metaData.unlockWrite();
    }

    /**
     * Getter of the metadata object.
     *
     * It simply returns the stored private property.
     *
     * @return metadata of this snapshot
     */
    public SnapshotMetaData getMetaData() {
        lockRead();

        try {
            return metaData;
        } finally {
            unlockRead();
        }
    }

    /**
     * Getter of the state object.
     *
     * It simply returns the stored private property.
     *
     * @return metadata of this snapshot
     */
    public SnapshotState getState() {
        return state;
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

    public Hash getHash() {
        lockRead();

        try {
            return this.getMetaData().getHash();
        } finally {
            unlockRead();
        }
    }

    /**
     * This method updates both - the balances and the index - in a single call.
     *
     * It first locks both child objects and then performs the corresponding updates. It is used by the MilestoneTracker
     * to update the balances after a milestone appeared.
     *
     * @param diff change in the balances
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
            state.applyStateDiff(diff, false);
            metaData.setIndex(newIndex, false);
            metaData.setTimestamp(System.currentTimeMillis() / 1000L, false);
        }

        // unlock the access to this object once we are done updating
        finally {
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
        return getMetaData().getSolidEntryPoints();
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
        return getMetaData().hasSolidEntryPoint(transactionHash);
    }

    public int getSolidEntryPointIndex(Hash solidEntrypoint) {
        return getMetaData().getSolidEntryPointIndex(solidEntrypoint);
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
        return state.getBalance(hash);
    }

    /**
     * This is a utility method for determining the index of the snapshot, with locking the underlying metadata object
     * first.
     *
     * Even though the index is not directly stored in this object, we offer the ability to read it from the Snapshot
     * itself, without having to retrieve the metadata first. This is mainly to keep the code more readable, without
     * having to manually traverse the necessary references.
     *
     * @return the milestone index of the snapshot
     */
    public int getIndex() {
        return getIndex(true);
    }

    /**
     * This is a utility method for determining the index of the snapshot, with optionally locking the underlying
     * metadata object.
     *
     * Even though the index is not directly stored in this object, we offer the ability to read it from the Snapshot
     * itself, without having to retrieve the metadata first. This is mainly to keep the code more readable, without
     * having to manually traverse the necessary references.
     *
     * @param lock if set to true the metadata object will be read-locked for other threads
     * @return the milestone index of the snapshot
     */
    public int getIndex(boolean lock) {
        return metaData.getIndex(lock);
    }

    /**
     * This is a utility method for determining the timestamp of the snapshot, with locking the underlying metadata
     * object first.
     *
     * Even though the timestamp is not directly stored in this object, we offer the ability to read it from the
     * Snapshot itself, without having to retrieve the metadata first. This is mainly to keep the code more readable,
     * without having to manually traverse the necessary references.
     *
     * @return the timestamp when the snapshot was updated or created
     */
    public long getTimestamp() {
        return getTimestamp(true);
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
    public long getTimestamp(boolean lock) {
        return metaData.getTimestamp(lock);
    }
}
