package com.iota.iri.service.snapshot;

import com.iota.iri.SignedFiles;
import com.iota.iri.conf.Configuration;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;

import com.iota.iri.utils.IotaIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class Snapshot {
    /**
     * Holds a reference to the metadata that belongs to this snapshot.
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
     * @param initialState
     * @param metaData
     */
    public Snapshot(Map<Hash, Long> initialState, SnapshotMetaData metaData) {
        this.state = new HashMap<>(initialState);
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
    public static String SNAPSHOT_PUBKEY = "TTXJUGKTNPOOEXSTQVVACENJOQUROXYKDRCVK9LHUXILCLABLGJTIPNF9REWHOIMEUKWQLUOKD9CZUYAC";
    public static int SNAPSHOT_PUBKEY_DEPTH = 6;
    public static int SNAPSHOT_INDEX = 6;
    public static int SPENT_ADDRESSES_INDEX = 7;
    private static Snapshot initialSnapshot;





    public static Snapshot init(Configuration configuration) throws IOException {
        // read the config vars for the built in snapshot files
        boolean testnet = configuration.booling(Configuration.DefaultConfSettings.TESTNET);
        String snapshotPath = configuration.string(Configuration.DefaultConfSettings.SNAPSHOT_FILE);
        String snapshotSigPath = configuration.string(Configuration.DefaultConfSettings.SNAPSHOT_SIGNATURE_FILE);

        //This is not thread-safe (and it is ok)
        if (initialSnapshot == null) {
            if (!testnet && !SignedFiles.isFileSignatureValid(snapshotPath, snapshotSigPath, SNAPSHOT_PUBKEY,
                    SNAPSHOT_PUBKEY_DEPTH, SNAPSHOT_INDEX)) {
                throw new RuntimeException("Snapshot signature failed.");
            }
            Map<Hash, Long> initialState = initInitialState(snapshotPath);
            initialSnapshot = new Snapshot(
                initialState,
                new SnapshotMetaData(
                    testnet ? 0 : configuration.integer(Configuration.DefaultConfSettings.MILESTONE_START_INDEX),
                    new HashSet<Hash>(Collections.singleton(Hash.NULL_HASH))
                )
            );
            checkStateHasCorrectSupply(initialState);
            checkInitialSnapshotIsConsistent(initialState);

        }
        return initialSnapshot;
    }

    private static InputStream getSnapshotStream(String snapshotPath) throws FileNotFoundException {
        InputStream inputStream = Snapshot.class.getResourceAsStream(snapshotPath);
        //if resource doesn't exist, read from file system
        if (inputStream == null) {
            inputStream = new FileInputStream(snapshotPath);
        }

        return inputStream;
    }

    private static void checkInitialSnapshotIsConsistent(Map<Hash, Long> initialState) {
        if (!isConsistent(initialState)) {
            log.error("Initial Snapshot inconsistent.");
            System.exit(-1);
        }
    }

    private static void checkStateHasCorrectSupply(Map<Hash, Long> initialState) {
        long stateValue = initialState.values().stream().reduce(Math::addExact).orElse(Long.MAX_VALUE);
        if (stateValue != TransactionViewModel.SUPPLY) {
            log.error("Transaction resolves to incorrect ledger balance: {}", TransactionViewModel.SUPPLY - stateValue);
            System.exit(-1);
        }
    }

    private static Map<Hash, Long> initInitialState(String snapshotFile) {
        String line;
        Map<Hash, Long> state = new HashMap<>();
        BufferedReader reader = null;
        try {
            InputStream snapshotStream = getSnapshotStream(snapshotFile);
            BufferedInputStream bufferedInputStream = new BufferedInputStream(snapshotStream);
            reader = new BufferedReader(new InputStreamReader(bufferedInputStream));
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(";", 2);
                if (parts.length >= 2) {
                    String key = parts[0];
                    String value = parts[1];
                    state.put(new Hash(key), Long.valueOf(value));
                }
            }
        } catch (IOException e) {
            //syso is left until logback is fixed
            System.out.println("Failed to load snapshot.");
            log.error("Failed to load snapshot.", e);
            System.exit(-1);
        }
        finally {
            IotaIOUtils.closeQuietly(reader);
        }
        return state;
    }

    protected final Map<Hash, Long> state;

    public Snapshot clone() {
        return new Snapshot(state, metaData.clone());
    }

    public Long getBalance(Hash hash) {
        Long l;
        readWriteLock.readLock().lock();
        l = state.get(hash);
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

    public static boolean isConsistent(Map<Hash, Long> state) {
        final Iterator<Map.Entry<Hash, Long>> stateIterator = state.entrySet().iterator();
        while (stateIterator.hasNext()) {

            final Map.Entry<Hash, Long> entry = stateIterator.next();
            if (entry.getValue() <= 0) {

                if (entry.getValue() < 0) {
                    log.info("Skipping negative value for address: " + entry.getKey() + ": " + entry.getValue());
                    return false;
                }

                stateIterator.remove();
            }
            //////////// --Coo only--
                /*
                 * if (entry.getValue() > 0) {
                 *
                 * System.out.ln("initialState.put(new Hash(\"" + entry.getKey()
                 * + "\"), " + entry.getValue() + "L);"); }
                 */
            ////////////
        }
        return true;
    }
}
