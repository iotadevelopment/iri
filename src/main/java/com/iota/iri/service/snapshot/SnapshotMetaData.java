package com.iota.iri.service.snapshot;

import com.iota.iri.model.Hash;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

public class SnapshotMetaData implements Cloneable {
    // CORE FUNCTIONALITY //////////////////////////////////////////////////////////////////////////////////////////////

    /**
     * Lock object allowing to block access to this object from different threads.
     */
    public final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    protected Hash initialHash;

    /**
     * Holds the transaction hash of the milestone.
     */
    private Hash hash;

    protected int initialIndex;

    /**
     * Holds the current index of this snapshot.
     *
     * The initial snapshot has its index set to the start index.
     */
    private int index;

    protected long initialTimestamp;

    /**
     * Holds the timestamp when this snapshot was created or updated the last time.
     */
    private long timestamp;

    /**
     * Set of transaction hashes that were cut off when creating the snapshot.
     *
     * When we try to solidify transactions, we stop and consider the transaction solid if it references a transaction
     * in this Set.
     */
    private HashMap<Hash, Integer> solidEntryPoints;

    private HashMap<Hash, Integer> seenMilestones;

    /**
     * This method retrieves the meta data of a snapshot from a file.
     *
     * It is used by local snapshots to determine the relevant information about the saved snapshot.
     *
     * @param filePath path to the snapshot metadata file
     * @return SnapshotMetaData instance holding all the relevant details about the snapshot
     * @throws FileNotFoundException if the metadata file does not exist
     * @throws IOException if the metadata file is not readable
     * @throws IllegalArgumentException if the metadata file exists but is malformed
     */
    public static SnapshotMetaData fromFile(String filePath) throws FileNotFoundException, IOException, IllegalArgumentException {
        return fromFile(new File(filePath));
    }

    /**
     * This method retrieves the meta data of a snapshot from a file.
     *
     * It is used by local snapshots to determine the relevant information about the saved snapshot.
     *
     * @param snapshotMetaDataFile File object with the path to the snapshot metadata file
     * @return SnapshotMetaData instance holding all the relevant details about the snapshot
     * @throws FileNotFoundException if the metadata file does not exist
     * @throws IOException if the metadata file is not readable
     * @throws IllegalArgumentException if the metadata file exists but is malformed
     */
    public static SnapshotMetaData fromFile(File snapshotMetaDataFile) throws FileNotFoundException, IOException, IllegalArgumentException {
        // create a read for our file
        BufferedReader reader = new BufferedReader(
            new InputStreamReader(
                new BufferedInputStream(
                    new FileInputStream(snapshotMetaDataFile)
                )
            )
        );

        // create variables to store the read values
        Hash hash;
        int index;
        long timestamp;
        int solidEntryPointsSize;
        int seenMilestonesSize;

        // read the hash
        String line;
        if((line = reader.readLine()) != null) {
            hash = new Hash(line);
        } else {
            throw new IllegalArgumentException("invalid or malformed snapshot metadata file at " + snapshotMetaDataFile.getAbsolutePath());
        }

        // read the index
        if((line = reader.readLine()) != null) {
            index = Integer.parseInt(line);
        } else {
            throw new IllegalArgumentException("invalid or malformed snapshot metadata file at " + snapshotMetaDataFile.getAbsolutePath());
        }

        // read the timestamp
        if((line = reader.readLine()) != null) {
            timestamp = Long.parseLong(line);
        } else {
            throw new IllegalArgumentException("invalid or malformed snapshot metadata file at " + snapshotMetaDataFile.getAbsolutePath());
        }

        // read the solid entry points size
        if((line = reader.readLine()) != null) {
            solidEntryPointsSize = Integer.parseInt(line);
        } else {
            throw new IllegalArgumentException("invalid or malformed snapshot metadata file at " + snapshotMetaDataFile.getAbsolutePath());
        }

        // read the solid entry points size
        if((line = reader.readLine()) != null) {
            seenMilestonesSize = Integer.parseInt(line);
        } else {
            throw new IllegalArgumentException("invalid or malformed snapshot metadata file at " + snapshotMetaDataFile.getAbsolutePath());
        }

        // read the solid entry points from our file
        HashMap<Hash, Integer> solidEntryPoints = new HashMap<>();
        for(int i = 0; i < solidEntryPointsSize; i++) {
            if((line = reader.readLine()) != null) {
                String[] parts = line.split(";", 2);
                if(parts.length >= 2) {
                    solidEntryPoints.put(new Hash(parts[0]), Integer.parseInt(parts[1]));
                }
            } else {
                throw new IllegalArgumentException("invalid or malformed snapshot metadata file at " + snapshotMetaDataFile.getAbsolutePath());
            }
        }

        // read the seen milestones
        HashMap<Hash, Integer> seenMilestones = new HashMap<>();
        for(int i = 0; i < seenMilestonesSize; i++) {
            if((line = reader.readLine()) != null) {
                String[] parts = line.split(";", 2);
                if(parts.length >= 2) {
                    seenMilestones.put(new Hash(parts[0]), Integer.parseInt(parts[1]));
                }
            } else {
                throw new IllegalArgumentException("invalid or malformed snapshot metadata file at " + snapshotMetaDataFile.getAbsolutePath());
            }
        }

        // close the reader
        reader.close();

        // create and return our SnapshotMetaData object
        return new SnapshotMetaData(hash, index, timestamp, solidEntryPoints, seenMilestones);
    }

    /**
     * Constructor of the SnapshotMetaData.
     *
     * It simply stores the passed in parameters for later use.
     *
     * @param  hash transaction hash representing
     * @param index index of the Snapshot that this metadata belongs to
     * @param solidEntryPoints Set of transaction hashes that were cut off when creating the snapshot
     */
    public SnapshotMetaData(Hash hash, int index, Long timestamp, HashMap<Hash, Integer> solidEntryPoints, HashMap<Hash, Integer> seenMilestones) {
        // store our parameters
        this.initialHash = hash;
        this.hash = hash;
        this.initialIndex = index;
        this.index = index;
        this.initialTimestamp = timestamp;
        this.timestamp = timestamp;
        this.solidEntryPoints = solidEntryPoints;
        this.seenMilestones = seenMilestones;
    }

    /**
     * Locks the metadata object for read access.
     *
     * This is used to synchronize the access from different Threads.
     */
    public void lockRead() {
        readWriteLock.readLock().lock();
    }

    /**
     * Locks the metadata object for write access.
     *
     * This is used to synchronize the access from different Threads.
     */
    public void lockWrite() {
        readWriteLock.writeLock().lock();
    }

    /**
     * Unlocks the object from read blocks.
     *
     * This is used to synchronize the access from different Threads.
     */
    public void unlockRead() {
        readWriteLock.readLock().unlock();
    }

    /**
     * Unlocks the object from write blocks.
     *
     * This is used to synchronize the access from different Threads.
     */
    public void unlockWrite() {
        readWriteLock.writeLock().unlock();
    }

    /**
     * This method is the setter of the milestone hash.
     *
     * It simply stores the passed value in the private property, with locking the object first.
     *
     * @param hash transaction hash of the milestone
     */
    public void setHash(Hash hash) {
        lockWrite();

        this.hash = hash;

        unlockWrite();
    }

    /**
     * This method is the getter of the milestone hash.
     *
     * It simply returns the stored private property, with locking the object first.
     *
     * @return transaction hash of the milestone
     */
    public Hash getHash() {
        lockRead();

        try {
            return hash;
        } finally {
            unlockRead();
        }
    }

    /**
     * This method is the setter of the milestone index.
     *
     * It simply stores the passed value in the private property, with locking the object first.
     *
      * @param index milestone index that shall be set
     */
    public void setIndex(int index) {
        setIndex(index, true);
    }

    /**
     * This method is the setter of the milestone index.
     *
     * It simply stores the passed value in the private property, with optionally locking the object first.
     *
     * @param index index that shall be set
     * @param lock boolean indicating if the object should be locked for other threads while writing to it
     */
    public void setIndex(int index, boolean lock) {
        // prevent other threads to write to this object while we do the updates
        if(lock) {
            lockWrite();
        }

        // apply our changes
        this.index = index;

        // unlock the access to this object once we are done updating
        if(lock) {
            unlockWrite();
        }
    }

    /**
     * This method is the getter of the index.
     *
     * It simply returns the stored private property, with locking the object first.
     *
     * @return current index of this metadata
     */
    public int getIndex() {
        return getIndex(true);
    }

    /**
     * This method is the getter of the index.
     *
     * It simply returns the stored private property, with optionally locking the object first.
     *
     * @return current index of this metadata
     */
    public int getIndex(boolean lock) {
        // lock the object for read access
        if(lock) {
            lockRead();
        }

        // return our value
        try {
            return this.index;
        }

        // unlock the object from read blocks
        finally {
            if(lock) {
                unlockRead();
            }
        }
    }

    /**
     * This method is the setter of the timestamp.
     *
     * It simply stores the passed value in the private property, with locking the object first.
     *
     * @param timestamp timestamp when the snapshot was created or updated
     */
    public void setTimestamp(long timestamp) {
        setTimestamp(timestamp, true);
    }

    /**
     * This method is the setter of the timestamp.
     *
     * It simply stores the passed value in the private property, with optionally locking the object first.
     *
     * @param timestamp timestamp when the snapshot was created or updated
     * @param lock boolean indicating if the object should be locked for other threads while writing to it
     */
    protected void setTimestamp(long timestamp, boolean lock) {
        // prevent other threads to write to this object while we do the updates
        if(lock) {
            lockWrite();
        }

        // apply our changes
        this.timestamp = timestamp;

        // unlock the access to this object once we are done updating
        if(lock) {
            unlockWrite();
        }
    }

    /**
     * This method is the getter of the timestamp.
     *
     * It simply returns the stored private property, with locking the object first.
     *
     * @return timestamp when the snapshot was created or updated
     */
    public long getTimestamp() {
        return getTimestamp(true);
    }

    /**
     * This method is the getter of the timestamp.
     *
     * It simply returns the stored private property, with optionally locking the object first.
     *
     * @return timestamp when the snapshot was created or updated
     */
    public long getTimestamp(boolean lock) {
        // lock the object for read access
        if(lock) {
            lockRead();
        }

        // return our value
        try {
            return this.timestamp;
        }

        // unlock the object from read blocks
        finally {
            if(lock) {
                unlockRead();
            }
        }
    }

    /**
     * This method performs a member check on the underlying solid entry points.
     *
     * It can be used to check if a transaction referenced by a hash can be considered solid. For nodes that do not use
     * local snapshots this set consists of the NULL_HASH only.
     *
     * @param solidEntrypoint hash that shall be checked
     * @return true if the hash is a solid entry point and false otherwise
     */
    public boolean hasSolidEntryPoint(Hash solidEntrypoint) {
        return solidEntryPoints.containsKey(solidEntrypoint);
    }

    public int getSolidEntryPointIndex(Hash solidEntrypoint) {
        return solidEntryPoints.get(solidEntrypoint);
    }

    /**
     * This method is the getter of the solid entry points.
     *
     * It simply returns the stored private property.
     *
     * @return set of transaction hashes that shall be considered solid when being referenced
     */
    public HashMap<Hash, Integer> getSolidEntryPoints() {
        return solidEntryPoints;
    }

    /**
     * This method is the setter of the solid entry points.
     *
     * It simply stores the passed value in the private property, with locking the object first.
     *
     * @param solidEntryPoints set of solid entry points that shall be stored
     */
    public void setSolidEntryPoints(HashMap<Hash, Integer> solidEntryPoints) {
        setSolidEntryPoints(solidEntryPoints, true);
    }

    /**
     * This method is the setter of the solid entry points.
     *
     * It simply stores the passed value in the private property, with optionally locking the object first.
     *
     * @param solidEntryPoints set of solid entry points that shall be stored
     */
    public void setSolidEntryPoints(HashMap<Hash, Integer> solidEntryPoints, boolean lock) {
        // prevent other threads to write to this object while we do the updates
        if(lock) {
            lockWrite();
        }

        // apply our changes
        this.solidEntryPoints = solidEntryPoints;

        // unlock the access to this object once we are done updating
        if(lock) {
            unlockWrite();
        }
    }

    /**
     * This method is the getter of the seen milestones.
     *
     * It simply returns the stored private property.
     *
     * @return set of milestones that were known
     */
    public HashMap<Hash, Integer> getSeenMilestones() {
        return seenMilestones;
    }

    /**
     * This method is the setter of the solid entry points.
     *
     * It simply stores the passed value in the private property, with locking the object first.
     *
     * @param seenMilestones set of solid entry points that shall be stored
     */
    public void setSeenMilestones(HashMap<Hash, Integer> seenMilestones) {
        setSeenMilestones(seenMilestones, true);
    }

    /**
     * This method is the setter of the seen milestones.
     *
     * It simply stores the passed value in the private property, with optionally locking the object first.
     *
     * @param seenMilestones set of solid entry points that shall be stored
     */
    public void setSeenMilestones(HashMap<Hash, Integer> seenMilestones, boolean lock) {
        // prevent other threads to write to this object while we do the updates
        if(lock) {
            lockWrite();
        }

        // apply our changes
        this.seenMilestones = seenMilestones;

        // unlock the access to this object once we are done updating
        if(lock) {
            unlockWrite();
        }
    }

    /**
     * This method writes a file containing a serialized version of this metadata object.
     *
     * It can be used to store the current values and read them on a later point in time. It is used by the local
     * snapshot manager to generate and maintain the snapshot files.
     *
     * @param filePath path to the snapshot metadata file
     * @return return a file handle to the generated file
     * @throws IOException if something goes wrong while writing to the file
     */
    public File writeFile(String filePath) throws IOException {
        return writeFile(new File(filePath));
    }

    /**
     * This method writes a file containing a serialized version of this metadata object.
     *
     * It can be used to store the current values and read them on a later point in time. It is used by the local
     * snapshot manager to generate and maintain the snapshot files.
     *
     * @param metaDataFile File handle to the snapshot metadata file
     * @return return a file handle to the generated file
     * @throws IOException if something goes wrong while writing to the file
     */
    public File writeFile(File metaDataFile) throws IOException {
        Files.write(
            Paths.get(metaDataFile.getAbsolutePath()),
            () -> Stream.concat(
                Stream.of(
                    hash.toString(),
                    String.valueOf(index),
                    String.valueOf(timestamp),
                    String.valueOf(solidEntryPoints.size()),
                    String.valueOf(seenMilestones.size())
                ),
                Stream.concat(
                    solidEntryPoints.entrySet().stream().<CharSequence>map(entry -> entry.getKey().toString() + ";" + entry.getValue()),
                    seenMilestones.entrySet().stream().<CharSequence>map(entry -> entry.getKey().toString() + ";" + entry.getValue())
                )
            ).iterator()
        );

        return metaDataFile;
    }

    /**
     * This method creates a deep clone of the SnapshotMetaData object.
     *
     * It can be used to make a copy of the object, that then can be modified without affecting the original object.
     *
     * @return deep copy of the original object
     */
    public SnapshotMetaData clone() {
        return new SnapshotMetaData(new Hash(hash.toString()), index, timestamp, (HashMap) solidEntryPoints.clone(), (HashMap) seenMilestones.clone());
    }
}
