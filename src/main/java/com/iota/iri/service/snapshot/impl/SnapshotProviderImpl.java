package com.iota.iri.service.snapshot.impl;

import com.iota.iri.SignedFiles;
import com.iota.iri.conf.SnapshotConfig;
import com.iota.iri.model.Hash;
import com.iota.iri.service.snapshot.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

/**
 * Implements the basic contract of the {@link SnapshotProvider} interface.
 */
public class SnapshotProviderImpl implements SnapshotProvider {
    /**
     * Public key that is used to verify the builtin snapshot signature.
     */
    private static final String SNAPSHOT_PUBKEY = "TTXJUGKTNPOOEXSTQVVACENJOQUROXYKDRCVK9LHUXILCLABLGJTIPNF9REWHOIMEUKWQLUOKD9CZUYAC";

    /**
     * Public key depth that is used to verify the builtin snapshot signature.
     */
    private static final int SNAPSHOT_PUBKEY_DEPTH = 6;

    /**
     * Snapshot index that is used to verify the builtin snapshot signature.
     */
    private static final int SNAPSHOT_INDEX = 9;

    /**
     * Logger for this class allowing us to dump debug and status messages.
     */
    private static final Logger log = LoggerFactory.getLogger(SnapshotProviderImpl.class);

    /**
     * Holds a cached version of the builtin snapshot.
     *
     * Note: The builtin snapshot is embedded in the iri.jar and will not change. To speed up tests that need the
     *       snapshot multiple times while creating their own version of the SnapshotManager, we cache the instance here
     *       so they don't have to rebuild it from the scratch every time (massively speeds up the unit tests).
     */
    private static SnapshotImpl builtinSnapshot = null;

    /**
     * Holds Snapshot related configuration parameters.
     */
    private final SnapshotConfig config;

    /**
     * Internal property for the value returned by {@link SnapshotProvider#getInitialSnapshot()}.
     */
    private Snapshot initialSnapshot;

    /**
     * Internal property for the value returned by {@link SnapshotProvider#getLatestSnapshot()}.
     */
    private Snapshot latestSnapshot;

    /**
     * Creates a data provider for the two {@link Snapshot} instances that are relevant for the node.
     *
     * It provides access to the two relevant {@link Snapshot} instances:
     *
     *     - the initial {@link Snapshot} (the starting point of the ledger based on the last global or local Snapshot)
     *     - the latest {@link Snapshot} (the state of the ledger after applying all changes up till the latest
     *       confirmed milestone)
     *
     * @param config Snapshot related configuration parameters
     * @throws SnapshotException if anything goes wrong while trying to read the snapshots
     */
    SnapshotProviderImpl(SnapshotConfig config) throws SnapshotException {
        this.config = config;

        loadSnapshots();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Snapshot getInitialSnapshot() {
        return initialSnapshot;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Snapshot getLatestSnapshot() {
        return latestSnapshot;
    }

    /**
     * Loads the snapshots that are provided by this data provider.
     *
     * We first check if a valid local {@link Snapshot} exists by trying to load it. If we fail to load the local
     * {@link Snapshot}, we fall back to the builtin one.
     *
     * After the {@link #initialSnapshot} was successfully loaded we create a copy of it that will act as the "working
     * copy" that will keep track of the latest changes that get applied while the node operates and sees new confirmed
     * transactions.
     *
     * @throws SnapshotException if anything goes wrong while loading the snapshots
     */
    private void loadSnapshots() throws SnapshotException {
        initialSnapshot = loadLocalSnapshot();
        if (initialSnapshot == null) {
            initialSnapshot = loadBuiltInSnapshot();
        }

        latestSnapshot = new SnapshotImpl(initialSnapshot);
    }

    private SnapshotImpl loadLocalSnapshot() {
        try {
            // if local snapshots are enabled
            if (config.getLocalSnapshotsEnabled()) {
                // load the remaining configuration parameters
                String basePath = config.getLocalSnapshotsBasePath();

                // create a file handle for our snapshot file
                File localSnapshotFile = new File(basePath + ".snapshot.state");

                // create a file handle for our snapshot metadata file
                File localSnapshotMetadDataFile = new File(basePath + ".snapshot.meta");

                // if the local snapshot files exists -> load them
                if (
                        localSnapshotFile.exists() &&
                                localSnapshotFile.isFile() &&
                                localSnapshotMetadDataFile.exists() &&
                                localSnapshotMetadDataFile.isFile()
                        ) {
                    // retrieve the state to our local snapshot
                    SnapshotState snapshotState = SnapshotStateImpl.fromFile(localSnapshotFile.getAbsolutePath());

                    // check the supply of the snapshot state
                    if (!snapshotState.hasCorrectSupply()) {
                        throw new IllegalStateException("the snapshot state file has an invalid supply");
                    }

                    // check the consistency of the snapshot state
                    if (!snapshotState.isConsistent()) {
                        throw new IllegalStateException("the snapshot state file is not consistent");
                    }

                    // retrieve the meta data to our local snapshot
                    SnapshotMetaDataImpl snapshotMetaData = SnapshotMetaDataImpl.fromFile(localSnapshotMetadDataFile);

                    log.info("Resumed from local snapshot #" + snapshotMetaData.getIndex() + " ...");

                    // return our Snapshot
                    return new SnapshotImpl(snapshotState, snapshotMetaData);
                }
            }
        } catch (Exception e) {
            log.error("No valid Local Snapshot file found", e);
        }

        // otherwise just return null
        return null;
    }

    private SnapshotImpl loadBuiltInSnapshot() throws SnapshotException {
        if (builtinSnapshot == null) {
            try {
                if (!config.isTestnet() && !SignedFiles.isFileSignatureValid(
                        config.getSnapshotFile(),
                        config.getSnapshotSignatureFile(),
                        SNAPSHOT_PUBKEY,
                        SNAPSHOT_PUBKEY_DEPTH,
                        SNAPSHOT_INDEX
                )) {
                    throw new SnapshotException("the snapshot signature is invalid");
                }
            } catch (IOException e) {
                throw new SnapshotException("failed to validate the signature of the builtin snapshot file", e);
            }

            SnapshotState snapshotState = SnapshotStateImpl.fromFile(config.getSnapshotFile());
            if (!snapshotState.hasCorrectSupply()) {
                throw new IllegalStateException("the snapshot state file has an invalid supply");
            }
            if (!snapshotState.isConsistent()) {
                throw new IllegalStateException("the snapshot state file is not consistent");
            }

            HashMap<Hash, Integer> solidEntryPoints = new HashMap<>();
            solidEntryPoints.put(Hash.NULL_HASH, config.getMilestoneStartIndex());

            builtinSnapshot = new SnapshotImpl(
                    snapshotState,
                    new SnapshotMetaDataImpl(
                            Hash.NULL_HASH,
                            config.getMilestoneStartIndex(),
                            config.getSnapshotTime(),
                            solidEntryPoints,
                            new HashMap<>()
                    )
            );
        }

        return new SnapshotImpl(builtinSnapshot);
    }
}
