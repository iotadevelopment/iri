package com.iota.iri.service.snapshot;

import com.iota.iri.model.Hash;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Iterator;
import java.util.stream.Stream;

public class SnapshotMetaData implements Cloneable {
    /**
     * Holds the current milestone index of this snapshot.
     *
     * The initial snapshot has its milestoneIndex set to the milestoneStartIndex.
     */
    public int milestoneIndex;

    /**
     * Set of transaction hashes that were cut off when creating the snapshot.
     *
     * When we try to solidify transactions, we stop and consider the transaction solid if it references a transaction
     * in this Set.
     */
    public HashSet<Hash> solidEntryPoints;

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

        // create a variable to store the read milestoneIndex
        int milestoneIndex;

        // read the milestoneIndex
        String line;
        if((line = reader.readLine()) != null) {
            milestoneIndex = Integer.parseInt(line);
        } else {
            throw new IllegalArgumentException("invalid or malformed snapshot metadata file at " + snapshotMetaDataFile.getAbsolutePath());
        }

        // read the solid entry points from our file
        HashSet<Hash> solidEntryPoints = new HashSet<Hash>();
        while((line = reader.readLine()) != null) {
            solidEntryPoints.add(new Hash(line));
        }

        // close the reader
        reader.close();

        // create and return our SnapshotMetaData object
        return new SnapshotMetaData(milestoneIndex, solidEntryPoints);
    }

    /**
     * Constructor of the SnapshotMetaData.
     *
     * It simply stores the passed in parameters for later use.
     *
     * @param milestoneIndex milestone index of the Snapshot that this metadata belongs to
     * @param solidEntryPoints Set of transaction hashes that were cut off when creating the snapshot
     */
    public SnapshotMetaData(int milestoneIndex, HashSet<Hash> solidEntryPoints) {
        // store our parameters
        this.milestoneIndex = milestoneIndex;
        this.solidEntryPoints = solidEntryPoints;
    }

    public void writeFile(String filePath) throws IOException {
        writeFile(new File(filePath));
    }

    public void writeFile(File metaDataFile) throws IOException {
        Files.write(
            Paths.get(metaDataFile.getAbsolutePath()),
            () -> Stream.concat(
                Stream.of(String.valueOf(milestoneIndex)),
                solidEntryPoints.stream().<CharSequence>map(entry -> entry.toString())
            ).iterator()
        );
    }

    /**
     * This method creates a deep clone of the SnapshotMetaData object.
     *
     * It can be used to make a copy of the object, that then can be modified without affecting the original object.
     *
     * @return deep copy of the original object
     */
    public SnapshotMetaData clone() {
        return new SnapshotMetaData(milestoneIndex, (HashSet) solidEntryPoints.clone());
    }
}
