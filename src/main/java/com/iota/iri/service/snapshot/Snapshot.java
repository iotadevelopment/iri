package com.iota.iri.service.snapshot;

import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.controllers.StateDiffViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.storage.Tangle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * This class represents a "snapshot" of the ledger at a given time.
 *
 * A complete snapshot of the ledger consists out of the current {@link SnapshotState} which holds the balances and its
 * {@link SnapshotMetaData} which holds several information about the snapshot like its timestamp, its corresponding
 * milestone index and so on.
 */
public class Snapshot {
    /**
     * Logger for this class allowing us to dump debug and status messages.
     */
    private static final Logger log = LoggerFactory.getLogger(Snapshot.class);

    /**
     * Holds a reference to the state of this snapshot.
     */
    protected final SnapshotState state;

    /**
     * Holds a reference to the metadata of this snapshot.
     */
    protected final SnapshotMetaData metaData;

    /**
     * Holds a set of milestones indexes that were skipped while advancing the Snapshot state.
     *
     * It is used to be able to identify which milestones have to be rolled back, even when additional milestones have
     * become known in the mean time.
     *
     * @see #rollbackLastMilestone(Tangle)
     */
    private HashSet<Integer> skippedMilestones = new HashSet<>();

    /**
     * Lock object allowing to block access to this object from different threads.
     */
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    // SNAPSHOT SPECIFIC METHODS ///////////////////////////////////////////////////////////////////////////////////////

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
     * This is used to synchronize the access from different Threads.
     */
    public void lockRead() {
        readWriteLock.readLock().lock();
    }

    /**
     * Unlocks the complete Snapshot object from read blocks.
     *
     * This is used to synchronize the access from different Threads.
     */
    public void unlockRead() {
        readWriteLock.readLock().unlock();
    }

    /**
     * Locks the complete Snapshot object for write access.
     *
     * This is used to synchronize the access from different Threads.
     */
    public void lockWrite() {
        readWriteLock.writeLock().lock();
    }

    /**
     * Unlocks the complete Snapshot object from write blocks.
     *
     * This is used to synchronize the access from different Threads.
     */
    public void unlockWrite() {
        readWriteLock.writeLock().unlock();
    }

    /**
     * This method applies the balance changes that are introduced by future milestones to the current Snapshot.
     *
     * It iterates over the milestone indexes starting from the current index to the target index and applies all found
     * milestone balances. If it can not find a milestone for a certain index it keeps track of this by adding it to
     * the {@link #skippedMilestones}, which allows us to revert the changes even if the missing milestone was received
     * and processed in the mean time. If the application of changes fails, we restore the state of the snapshot to the
     * one it had before the application attempt so this method only modifies the Snapshot if it succeeds.
     *
     * Note: the changes done by this method can be reverted by using {@link #rollBackMilestones(int, Tangle)}
     *
     * @param targetMilestoneIndex the index of the milestone that should be rolled back
     * @param tangle Tangle object which acts as a database interface
     */
    public void replayMilestones(int targetMilestoneIndex, Tangle tangle) throws SnapshotException {
        lockWrite();

        SnapshotMetaData metaDataBeforeChanges = metaData.clone();
        SnapshotState stateBeforeChanges = state.clone();

        try {
            for (int currentMilestoneIndex = getIndex() + 1; currentMilestoneIndex <= targetMilestoneIndex; currentMilestoneIndex++) {
                MilestoneViewModel currentMilestone = MilestoneViewModel.get(tangle, currentMilestoneIndex);
                if (currentMilestone != null) {
                    StateDiffViewModel stateDiffViewModel = StateDiffViewModel.load(tangle, currentMilestone.getHash());
                    if(!stateDiffViewModel.isEmpty()) {
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
            state.update(stateBeforeChanges);
            metaData.update(metaDataBeforeChanges);

            throw new SnapshotException("failed to replay the the state of the ledger", e);
        } finally {
            unlockWrite();
        }
    }

    /**
     * This method rolls back the latest milestones until it reaches the state that the snapshot had before applying
     * the milestone indicated by the given parameter.
     *
     * After checking the validity of the parameters we simply call {@link #rollbackLastMilestone(Tangle)} multiple
     * times until we are done. If the rollback fails, we restore the state of the snapshot to the one it had before
     * the rollback attempt so this method only modifies the Snapshot if it succeeds.
     *
     * Note: this method is used to reverse the changes introduced by {@link #replayMilestones(int, Tangle)}
     *
     * @param targetMilestoneIndex the index of the milestone that should be rolled back
     * @param tangle Tangle object which acts as a database interface
     */
    public void rollBackMilestones(int targetMilestoneIndex, Tangle tangle) throws SnapshotException {
        if(targetMilestoneIndex <= getInitialIndex()) {
            throw new SnapshotException("the target milestone index is lower than the initial snapshot index - cannot revert back to an unknown milestone");
        }

        if(targetMilestoneIndex > getIndex()) {
            throw new SnapshotException("the target milestone index is higher than the current one - consider using replayMilestones instead");
        }

        lockWrite();

        SnapshotMetaData metaDataBeforeChanges = metaData.clone();
        SnapshotState stateBeforeChanges = state.clone();

        try {
            boolean rollbackSuccessful = true;
            while (targetMilestoneIndex <= getIndex() && rollbackSuccessful) {
                rollbackSuccessful = rollbackLastMilestone(tangle);
            }

            if(targetMilestoneIndex > getIndex()) {
                throw new SnapshotException("failed to reach the target milestone index when rolling back the milestones");
            }
        } catch(SnapshotException e) {
            state.update(stateBeforeChanges);
            metaData.update(metaDataBeforeChanges);

            throw e;
        } finally {
            unlockWrite();
        }
    }

    /**
     * This method creates a deep clone of the Snapshot object.
     *
     * It can be used to make a copy of the object, that then can be modified without affecting the original object.
     *
     * @return deep copy of the original object
     */
    @Override
    public Snapshot clone() {
        lockRead();

        try {
            return new Snapshot(state.clone(), metaData.clone());
        } finally {
            unlockRead();
        }
    }

    /**
     * This method reverts the changes caused by the last milestone that was applied to this snapshot.
     *
     * It first checks if we didn't arrive at the initial index yet and then reverts the balance changes that were
     * caused by the last milestone. Then it checks if any milestones were skipped while applying the last milestone and
     * determines the {@link SnapshotMetaData} that this Snapshot had before and restores it.
     *
     * @param tangle Tangle object which acts as a database interface
     * @return true if the snapshot was rolled back or false otherwise
     * @throws SnapshotException if anything goes wrong while accessing the database
     */
    private boolean rollbackLastMilestone(Tangle tangle) throws SnapshotException {
        lockWrite();

        try {
            if(getIndex() == getInitialIndex()) {
                return false;
            }

            // revert the last balance changes
            StateDiffViewModel stateDiffViewModel = StateDiffViewModel.load(tangle, getHash());
            if(!stateDiffViewModel.isEmpty()) {
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
                    throw new SnapshotException("the Snapshot would be inconsistent after applying patch belonging to milestone #" + getIndex() + " (" + getHash().toString() + ")");
                }

                state.applyStateDiff(snapshotStateDiff);
            }

            // jump skipped milestoness
            int currentIndex = getIndex() - 1;
            while(skippedMilestones.remove(currentIndex)) {
                currentIndex--;
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

    // THREAD-SAFE SNAPSHOTSTATE METHODS ///////////////////////////////////////////////////////////////////////////////

    /**
     * This method does the same as {@link SnapshotState#getBalance(Hash)} but automatically manages the locks necessary
     * for making this method thread-safe.
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
     * This method does the same as {@link SnapshotState#hasCorrectSupply()} but automatically manages the locks
     * necessary for making this method thread-safe.
     */
    public boolean hasCorrectSupply() {
        lockRead();

        try {
            return state.hasCorrectSupply();
        } finally {
            unlockRead();
        }
    }

    /**
     * This method does the same as {@link SnapshotState#isConsistent()} but automatically manages the locks necessary
     * for making this method thread-safe.
     */
    public boolean isConsistent() {
        lockRead();

        try {
            return state.isConsistent();
        } finally {
            unlockRead();
        }
    }

    /**
     * This method does the same as {@link SnapshotState#patchedState(SnapshotStateDiff)} but automatically manages the
     * locks necessary for making this method thread-safe.
     */
    public SnapshotState patchedState(SnapshotStateDiff snapshotStateDiff) {
        lockRead();

        try {
            return state.patchedState(snapshotStateDiff);
        } finally {
            unlockRead();
        }
    }

    // THREAD-SAFE SNAPSHOTMETADATA METHODS ////////////////////////////////////////////////////////////////////////////

    /**
     * This method does the same as {@link SnapshotMetaData#setHash(Hash)} but automatically manages the locks necessary
     * for making this method thread-safe.
     */
    public void setHash(Hash hash) {
        lockWrite();

        try {
            metaData.setHash(hash);
        } finally {
            unlockWrite();
        }
    }

    /**
     * This method does the same as {@link SnapshotMetaData#getHash()} but automatically manages the locks necessary for
     * making this method thread-safe.
     */
    public Hash getHash() {
        lockRead();

        try {
            return this.metaData.getHash();
        } finally {
            unlockRead();
        }
    }

    /**
     * This method does the same as {@link SnapshotMetaData#getInitialHash()} but automatically manages the locks
     * necessary for making this method thread-safe.
     */
    public Hash getInitialHash() {
        lockRead();

        try {
            return metaData.getInitialHash();
        } finally {
            unlockRead();
        }
    }

    /**
     * This method does the same as {@link SnapshotMetaData#setIndex(int)} but automatically manages the locks necessary
     * for making this method thread-safe.
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
     * This method does the same as {@link SnapshotMetaData#getIndex()} but automatically manages the locks necessary
     * for making this method thread-safe.
     */
    public int getIndex() {
        lockRead();

        try {
            return metaData.getIndex();
        } finally {
            unlockRead();
        }
    }

    /**
     * This method does the same as {@link SnapshotMetaData#getInitialIndex()} but automatically manages the locks
     * necessary for making this method thread-safe.
     */
    public int getInitialIndex() {
        lockRead();

        try {
            return metaData.getInitialIndex();
        } finally {
            unlockRead();
        }
    }

    /**
     * This method does the same as {@link SnapshotMetaData#setTimestamp(long)} but automatically manages the locks
     * necessary for making this method thread-safe.
     */
    public void setTimestamp(long timestamp) {
        lockWrite();

        try {
            metaData.setTimestamp(timestamp);
        } finally {
            unlockWrite();
        }
    }

    /**
     * This method does the same as {@link SnapshotMetaData#getTimestamp()}} but automatically manages the locks
     * necessary for making this method thread-safe.
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
     * This method does the same as {@link SnapshotMetaData#getInitialTimestamp()} but automatically manages the locks
     * necessary for making this method thread-safe.
     */
    private long getInitialTimestamp() {
        lockRead();

        try {
            return metaData.getInitialTimestamp();
        } finally {
            unlockRead();
        }
    }

    /**
     * This method does the same as {@link SnapshotMetaData#setSolidEntryPoints(HashMap)} but automatically manages the
     * locks necessary for making this method thread-safe.
     */
    public void setSolidEntryPoints(HashMap<Hash, Integer> solidEntryPoints) {
        lockWrite();

        try {
            metaData.setSolidEntryPoints(solidEntryPoints);
        } finally {
            unlockWrite();
        }
    }

    /**
     * This method does the same as {@link SnapshotMetaData#getSolidEntryPoints()}} but automatically manages the locks
     * necessary for making this method thread-safe.
     */
    public HashMap<Hash, Integer> getSolidEntryPoints() {
        lockRead();

        try {
            return metaData.getSolidEntryPoints();
        } finally {
            unlockRead();
        }
    }

    /**
     * This method does the same as {@link SnapshotMetaData#hasSolidEntryPoint(Hash)} but automatically manages the
     * locks necessary for making this method thread-safe.
     */
    public boolean hasSolidEntryPoint(Hash transactionHash) {
        lockRead();

        try {
            return metaData.hasSolidEntryPoint(transactionHash);
        } finally {
            unlockRead();
        }
    }

    /**
     * This method does the same as {@link SnapshotMetaData#getSolidEntryPointIndex(Hash)} but automatically manages the
     * locks necessary for making this method thread-safe.
     */
    public int getSolidEntryPointIndex(Hash solidEntrypoint) {
        lockRead();

        try {
            return metaData.getSolidEntryPointIndex(solidEntrypoint);
        } finally {
            unlockRead();
        }
    }

    /**
     * This method does the same as {@link SnapshotMetaData#setSeenMilestones(HashMap)} but automatically manages the
     * locks necessary for making this method thread-safe.
     */
    public void setSeenMilestones(HashMap<Hash, Integer> seenMilestones) {
        lockWrite();

        try {
            metaData.setSeenMilestones(seenMilestones);
        } finally {
            unlockWrite();
        }
    }

    /**
     * This method does the same as {@link SnapshotMetaData#getSeenMilestones()} but automatically manages the locks
     * necessary for making this method thread-safe.
     */
    public HashMap<Hash, Integer> getSeenMilestones() {
        lockRead();

        try {
            return metaData.getSeenMilestones();
        } finally {
            unlockRead();
        }
    }

}
