package com.iota.iri.service.snapshot;

import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.utils.IotaIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

public class SnapshotState {
    /**
     * Logger for this class (used to emit debug messages).
     */
    private static final Logger log = LoggerFactory.getLogger(SnapshotState.class);

    /**
     * Underlying Map storing the balances of addresses.
     */
    protected final Map<Hash, Long> state;

    /**
     *
     */
    public final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();

    public static SnapshotState fromFile(String snapshotStateFilePath) {
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

    public SnapshotState(Map<Hash, Long> initialState) {
        this.state = new HashMap<>(initialState);
    }

    public void lockRead() {
        readWriteLock.readLock().lock();
    }

    public void lockWrite() {
        readWriteLock.writeLock().lock();
    }

    public void unlockRead() {
        readWriteLock.readLock().unlock();
    }

    public void unlockWrite() {
        readWriteLock.writeLock().unlock();
    }

    public SnapshotState patchedState(SnapshotStateDiff snapshotStateDiff) {
        lockRead();

        Map<Hash, Long> patch = snapshotStateDiff.diff.entrySet().stream().map(
            hashLongEntry -> new HashMap.SimpleEntry<>(
                hashLongEntry.getKey(),
                state.getOrDefault(hashLongEntry.getKey(), 0L) + hashLongEntry.getValue()
            )
        ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        unlockRead();

        return new SnapshotState(patch);
    }

    public void apply(SnapshotStateDiff diff) {
        apply(diff, true);
    }

    /**
     * This method allows us to apply
     *
     * @param diff
     * @param lock
     */
    protected void apply(SnapshotStateDiff diff, boolean lock) {
        if(lock) {
            lockWrite();
        }

        diff.diff.entrySet().stream().forEach(hashLongEntry -> {
            if (state.computeIfPresent(hashLongEntry.getKey(), (hash, aLong) -> hashLongEntry.getValue() + aLong) == null) {
                state.putIfAbsent(hashLongEntry.getKey(), hashLongEntry.getValue());
            }
        });

        if(lock) {
            unlockWrite();
        }
    }

    public boolean isConsistent() {
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
        }

        return true;
    }

    public boolean hasCorrectSupply() {
        // block reading access
        lockRead();

        // calculate the sum of all balances
        long supply = state.values().stream().reduce(Math::addExact).orElse(Long.MAX_VALUE);

        // unblock reading access
        unlockRead();

        // if the sum differs from the expected supply -> dump an error and return false ...
        if(supply != TransactionViewModel.SUPPLY) {
            log.error("Supply differs from the expected supply by: {}", TransactionViewModel.SUPPLY - supply);

            return false;
        }

        // ... otherwise return true
        return true;
    }

    public Long getBalance(Hash address) {
        lockRead();

        Long balance = this.state.get(address);

        unlockRead();

        return balance;
    }

    public SnapshotState clone() {
        return new SnapshotState(this.state);
    }
}
