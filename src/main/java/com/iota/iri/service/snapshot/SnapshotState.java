package com.iota.iri.service.snapshot;

import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.utils.IotaIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class represents the "state" of the ledger at a given time, which means how many IOTA are available on a certain
 * address.
 *
 * It can either be a full ledger state which is used by the Snapshots or a "differential" State which carries the
 * balance changes from one State to the next one which is used when updating the ledger state between two milestones.
 */
public class SnapshotState {
    /**
     * Logger for this class (used to emit debug messages).
     */
    protected static final Logger log = LoggerFactory.getLogger(SnapshotState.class);

    /**
     * Underlying Map storing the balances of the addresses.
     */
    protected final Map<Hash, Long> balances;

    /**
     * This method reads the balances from the given file and creates the corresponding SnapshotState.
     *
     * The format of the file is pairs of "<address>;<balance>" separated by newlines. It simply reads the file line by
     * line, adding the corresponding values to the map.
     *
     * @param snapshotStateFilePath
     * @return
     */
    protected static SnapshotState fromFile(String snapshotStateFilePath) {
        String line;
        Map<Hash, Long> state = new HashMap<>();
        BufferedReader reader = null;
        try {
            InputStream snapshotStream = Snapshot.class.getResourceAsStream(snapshotStateFilePath);
            if(snapshotStream == null) {
                snapshotStream = new FileInputStream(snapshotStateFilePath);
            }

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

        return new SnapshotState(state);
    }

    /**
     * The constructor of this class makes a copy of the provided map and stores it in its internal property.
     *
     * This allows us to work with the provided balances without having to worry about modifications of the passed in
     * map that happens outside of the SnapshotState logic.
     *
     * While most of the other methods are public, the constructor is protected since we do not want to allow the
     * creation of SnapshotState's outside of the snapshot logic.
     *
     * @param initialState map with the addresses and their balances
     */
    protected SnapshotState(Map<Hash, Long> initialState) {
        this.balances = new HashMap<>(initialState);
    }

    /**
     * This method creates a SnapshotState that contains the resulting balances of only the addresses that were modified
     * by the given diff.
     *
     * It can be used to check if the modifications by a {@link SnapshotStateDiff} will result in a consistent State
     * where all modified addresses are still positive. Even though this State can be consistent, it will most probably
     * not return true if we call {@link #hasCorrectSupply()}, since the unmodified addresses are missing.
     *
     * @param snapshotStateDiff the balance patches that we want to apply
     * @return a differential SnapshotState that contains the resulting balances of all modified addresses
     */
    public SnapshotState patchedState(SnapshotStateDiff snapshotStateDiff) {
        return new SnapshotState(snapshotStateDiff.diff.entrySet().stream().map(
            hashLongEntry -> new HashMap.SimpleEntry<>(
                hashLongEntry.getKey(),
                balances.getOrDefault(hashLongEntry.getKey(), 0L) + hashLongEntry.getValue()
            )
        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    /**
     * This method applies the given {@link SnapshotStateDiff} to the current balances.
     *
     * Since this method actually changes the balances of this snapshot the passed in diff should manually be checked
     * for consistency before.
     *
     * @param diff the balance changes that should be applied to this state.
     */
    public void applyStateDiff(SnapshotStateDiff diff) {
        diff.diff.entrySet().stream().forEach(hashLongEntry -> {
            if(balances.computeIfPresent(hashLongEntry.getKey(), (hash, aLong) -> hashLongEntry.getValue() + aLong) == null) {
                balances.putIfAbsent(hashLongEntry.getKey(), hashLongEntry.getValue());
            }
        });
    }

    /**
     * This method returns all addresses that have a negative balance.
     *
     * While this should never happen with the state belonging to the snapshot itself, it can still happen for the
     * differential states that are getting created by {@link #patchedState(SnapshotStateDiff)} for the exact reason of
     * checking their consistency.
     *
     * @return a map of the inconsistent addresses (negative balance) and their actual balance (empty if consistent)
     */
    public HashMap<Hash, Long> getInconsistentAddresses() {
        HashMap<Hash, Long> result = new HashMap<Hash, Long>();

        final Iterator<Map.Entry<Hash, Long>> stateIterator = balances.entrySet().iterator();
        while(stateIterator.hasNext()) {
            final Map.Entry<Hash, Long> entry = stateIterator.next();
            if(entry.getValue() <= 0) {
                if(entry.getValue() < 0) {
                    log.info("skipping negative value for address " + entry.getKey() + ": " + entry.getValue());

                    result.put(entry.getKey(), entry.getValue());
                }

                stateIterator.remove();
            }
        }

        return result;
    }

    /**
     * This method checks if the state is consistent.
     *
     * Consistent means that there are no addresses with a negative value.
     *
     * @return true if the state is consistent and false otherwise
     */
    public boolean isConsistent() {
        return getInconsistentAddresses().size() == 0;
    }

    /**
     * This method checks if the state of the ledger has the correct supply.
     *
     * It first calculates a sum of the balances of all addresses and then checks if that supply equals the expected
     * value.
     *
     * It doesn't make sense to call this functions on "differential" states that are used to check the
     * consistency of patches but the snapshot state itself should always stay consistent.
     *
     * @return true if the supply is correct and false otherwise
     */
    public boolean hasCorrectSupply() {
        long supply = balances.values().stream().reduce(Math::addExact).orElse(Long.MAX_VALUE);

        if(supply != TransactionViewModel.SUPPLY) {
            log.error("the supply differs from the expected supply by: {}", TransactionViewModel.SUPPLY - supply);

            return false;
        }

        return true;
    }

    /**
     * This method returns the balance for an address.
     *
     * @param address address that shall be checked
     * @return balance of the address or null if the address is unkown
     */
    public Long getBalance(Hash address) {
        return this.balances.get(address);
    }

    protected void update(SnapshotState newState) {
        balances.clear();
        balances.putAll(newState.balances);
    }

    /**
     * This method creates a deep clone of the current state.
     *
     * The created clone can be modified without affecting the original state.
     *
     * @return a deep copy of this object
     */
    protected SnapshotState clone() {
        return new SnapshotState(this.balances);
    }

    /**
     * This method dumps the current state to a file.
     *
     * It is used by local snapshots to persist the in memory states and allow IRI to resume from the local snapshot.
     *
     * @param snapshotPath location of the file that shall be written
     * @throws IOException if anything goes wrong while writing the file
     */
    protected void writeFile(String snapshotPath) throws IOException {
        // try to write the file
        Files.write(Paths.get(snapshotPath), () -> balances.entrySet().stream().filter(
            entry -> entry.getValue() != 0
        ).<CharSequence>map(
            entry -> entry.getKey() + ";" + entry.getValue()
        ).sorted().iterator());
    }
}
