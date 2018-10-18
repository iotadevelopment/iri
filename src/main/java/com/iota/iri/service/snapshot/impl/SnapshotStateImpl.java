package com.iota.iri.service.snapshot.impl;

import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.model.HashFactory;
import com.iota.iri.service.snapshot.SnapshotException;
import com.iota.iri.service.snapshot.SnapshotState;
import com.iota.iri.service.snapshot.SnapshotStateDiff;
import com.iota.iri.utils.IotaIOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Implements the basic contract of the {@link SnapshotState} interface.
 */
public class SnapshotStateImpl implements SnapshotState {
    /**
     * Logger for this class (used to emit debug messages).
     */
    private static final Logger log = LoggerFactory.getLogger(SnapshotStateImpl.class);

    /**
     * Holds the balances of the addresses.
     */
    private final Map<Hash, Long> balances;

    /**
     * This method reads the balances from the given file and creates the corresponding SnapshotState.
     *
     * The format of the file is pairs of "address;balance" separated by newlines. It simply reads the file line by
     * line, adding the corresponding values to the map.
     *
     * @param snapshotStateFilePath location of the snapshot state file
     * @return the unserialized version of the state file
     */
    protected static SnapshotState fromFile(String snapshotStateFilePath) throws SnapshotException {
        BufferedReader reader = null;
        try {
            InputStream snapshotStream = SnapshotImpl.class.getResourceAsStream(snapshotStateFilePath);
            if (snapshotStream == null) {
                snapshotStream = new FileInputStream(snapshotStateFilePath);
            }
            reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(snapshotStream)));

            Map<Hash, Long> state = new HashMap<>();

            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(";", 2);
                if (parts.length >= 2) {
                    state.put(HashFactory.ADDRESS.create(parts[0]), Long.valueOf(parts[1]));
                } else {
                    throw new SnapshotException("malformed snapshot state file at " + snapshotStateFilePath);
                }
            }

            return new SnapshotStateImpl(state);
        } catch (IOException e) {
            throw new SnapshotException("failed to read the snapshot file at " + snapshotStateFilePath, e);
        } finally {
            IotaIOUtils.closeQuietly(reader);
        }
    }

    /**
     * Creates a deep clone of the passed in {@link SnapshotState}.
     *
     * @param snapshotState the object that shall be cloned
     */
    public SnapshotStateImpl(SnapshotState snapshotState) {
        this(snapshotState.getBalances());
    }

    /**
     * Creates a {@link SnapshotState} from the passed in {@link Map} by storing the mapping in its private property.
     *
     * While most of the other methods are public, this constructor is protected since we do not want to allow the
     * manual creation of {@link SnapshotState}'s outside of the snapshot logic.
     *
     * @param balances map with the addresses associated to their balance
     */
    private SnapshotStateImpl(Map<Hash, Long> balances) {
        this.balances = balances;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long getBalance(Hash address) {
        return this.balances.get(address);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<Hash, Long> getBalances() {
        return new HashMap<>(balances);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isConsistent() {
        return getInconsistentAddresses().size() == 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasCorrectSupply() {
        long supply = balances.values()
                .stream()
                .reduce(Math::addExact)
                .orElse(Long.MAX_VALUE);

        return supply == TransactionViewModel.SUPPLY;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void update(SnapshotState newState) {
        balances.clear();
        balances.putAll(newState.getBalances());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void applyStateDiff(SnapshotStateDiff diff) throws SnapshotException {
        if (!diff.isConsistent()) {
            throw new SnapshotException("cannot apply an inconsistent SnapshotStateDiff");
        }

        diff.getBalanceChanges().forEach((addressHash, balance) -> {
            System.out.println(addressHash);
            System.out.println(balance);
            if (balances.computeIfPresent(addressHash, (hash, aLong) -> balance + aLong) == null) {
                balances.putIfAbsent(addressHash, balance);
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SnapshotState patchedState(SnapshotStateDiff snapshotStateDiff) {
        Map<Hash, Long> patchedBalances = snapshotStateDiff.getBalanceChanges()
                .entrySet()
                .stream()
                .map(hashLongEntry -> new HashMap.SimpleEntry<>(hashLongEntry.getKey(),
                        balances.getOrDefault(hashLongEntry.getKey(), 0L) + hashLongEntry.getValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        return new SnapshotStateImpl(patchedBalances);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeToDisk(String snapshotPath) throws SnapshotException {
        try {
            Files.write(
                    Paths.get(snapshotPath),
                    () -> balances.entrySet()
                            .stream()
                            .filter(entry -> entry.getValue() != 0)
                            .<CharSequence>map(entry -> entry.getKey() + ";" + entry.getValue())
                            .sorted()
                            .iterator()
            );
        } catch (IOException e) {
            throw new SnapshotException("failed to write the snapshot state file at " + snapshotPath, e);
        }
    }

    /**
     * Returns all addresses that have a negative balance.
     *
     * While this should never happen with the state belonging to the snapshot itself, it can still happen for the
     * differential states that are getting created by {@link #patchedState(SnapshotStateDiff)} for the exact reason of
     * checking their consistency.
     *
     * @return a map of the inconsistent addresses (negative balance) and their actual balance
     */
    private HashMap<Hash, Long> getInconsistentAddresses() {
        HashMap<Hash, Long> result = new HashMap<>();
        balances.forEach((key, value) -> {
            if (value < 0) {
                log.info("negative value for address " + key + ": " + value);

                result.put(key, value);
            }
        });

        return result;
    }
}
