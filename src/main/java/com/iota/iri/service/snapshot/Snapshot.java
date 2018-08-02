package com.iota.iri.service.snapshot;

import com.iota.iri.model.Hash;

public class Snapshot {
    // CORE FUNCTIONALITY //////////////////////////////////////////////////////////////////////////////////////////////

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
     * Getter of the metadata object.
     *
     * It simply returns the stored private property.
     *
     * @return metadata of this snapshot
     */
    public SnapshotMetaData getMetaData() {
        return metaData;
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
     * Locks the complete Snapshot object for read access.
     *
     * It sets the corresponding locks in all child objects and therefore locks the whole object in its current state.
     * This is used to synchronize the access from different Threads, if both members need to be read.
     *
     * A more fine-grained control over the locks can be achieved by invoking the lock methods in the child objects
     * themselves.
     */
    public void lockRead() {
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
        state.unlockWrite();
        metaData.unlockWrite();
    }

    /**
     * This method creates a deep clone of the Snapshot object.
     *
     * It can be used to make a copy of the object, that then can be modified without affecting the original object.
     *
     * @return deep copy of the original object
     */
    public Snapshot clone() {
        return new Snapshot(state.clone(), metaData.clone());
    }

    // UTILITY METHODS /////////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * This method updates both - the balances and the index - in a single call.
     *
     * It first locks both child objects and then performs the corresponding updates. It is used by the MilestoneTracker
     * to update the balances after a milestone appeared.
     *
     * @param diff change in the balances
     * @param newIndex new milestone index
     */
    public void update(SnapshotStateDiff diff, int newIndex) {
        // check the diff before we apply the update
        if(!diff.isConsistent()) {
            throw new IllegalStateException("the snapshot state diff is not consistent");
        }

        // prevent other methods to write to this object while we do the updates
        lockWrite();

        // apply our changes without locking the underlying members (we already locked globally)
        state.apply(diff, false);
        metaData.milestoneIndex(newIndex, false);

        // unlock the access to this object once we are done updating
        unlockWrite();
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
    public Long getBalance(Hash hash) {
        return state.getBalance(hash);
    }

    /**
     * This is a utility method for determining the index of the snapshot.
     *
     * Even though the index is not directly stored in this object, we offer the ability to read it from the Snapshot
     * itself, without having to retrieve the metadata first. This is mainly to keep the code more readable, without
     * having to manually traverse the necessary references.
     *
     * @return the milestone index of the snapshot
     */
    public int getIndex() {
        return metaData.milestoneIndex();
    }
}
