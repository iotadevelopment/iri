package com.iota.iri.service.snapshot;

import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.utils.IotaIOUtils;
import com.iota.iri.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
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
        // lock the object while we are reading
        lockRead();

        try {
            // construct a SnapshotState that only contains the resulting balances affected by the given diff
            Map<Hash, Long> patch = snapshotStateDiff.diff.entrySet().stream().map(
                hashLongEntry -> new HashMap.SimpleEntry<>(
                    hashLongEntry.getKey(),
                    state.getOrDefault(hashLongEntry.getKey(), 0L) + hashLongEntry.getValue()
                )
            ).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            return new SnapshotState(patch);
        }

        finally {
            unlockRead();
        }
    }

    public void applyStateDiff(SnapshotStateDiff diff) {
        applyStateDiff(diff, true);
    }

    /**
     * This method allows us to apply
     *
     * @param diff
     * @param lock
     */
    protected void applyStateDiff(SnapshotStateDiff diff, boolean lock) {
        // optionally lock while we are reading
        if(lock) {
            lockWrite();
        }

        //
        try {
            diff.diff.entrySet().stream().forEach(hashLongEntry -> {
                if(state.computeIfPresent(hashLongEntry.getKey(), (hash, aLong) -> hashLongEntry.getValue() + aLong) == null) {
                    state.putIfAbsent(hashLongEntry.getKey(), hashLongEntry.getValue());
                }
            });
        }

        // optionally unlock when we are done
        finally {
            if(lock) {
                unlockWrite();
            }
        }
    }

    public HashMap<Hash, Long> getInconsistentAddresses() {
        // lock while we are reading
        lockRead();

        // create variable for our result
        HashMap<Hash, Long> result = new HashMap<Hash, Long>();

        // cycle through our balances and check if they are positive (dump a message if sth is not consistent)
        try {
            final Iterator<Map.Entry<Hash, Long>> stateIterator = state.entrySet().iterator();
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

        // unlock when we are done
        finally {
            unlockRead();
        }
    }

    public boolean isConsistent() {
        return getInconsistentAddresses().size() == 0;
    }

    public boolean hasCorrectSupply() {
        // lock while we are reading
        lockRead();

        try {
            // calculate the sum of all balances
            long supply = state.values().stream().reduce(Math::addExact).orElse(Long.MAX_VALUE);

            // if the sum differs from the expected supply -> dump an error and return false ...
            if(supply != TransactionViewModel.SUPPLY) {
                log.error("the supply differs from the expected supply by: {}", TransactionViewModel.SUPPLY - supply);

                return false;
            }

            // ... otherwise return true
            return true;
        }

        // unlock when we are done
        finally {
            unlockRead();
        }
    }

    public Long getBalance(Hash address) {
        // lock while we are reading
        lockRead();

        // return the result
        try {
            return this.state.get(address);
        }

        // unlock when we are done
        finally {
            unlockRead();
        }
    }

    public SnapshotState clone() {
        // lock the object while we read
        lockRead();

        // create our clone
        try {
            return new SnapshotState(this.state);
        }

        // unlock when we are done
        finally {
            unlockRead();
        }
    }

    public void writeFile(String snapshotPath) throws IOException {
        // lock the object while we read the information
        lockRead();

        // try to write the file
        try {
            Files.write(Paths.get(snapshotPath), () -> state.entrySet().stream().filter(
                entry -> entry.getValue() != 0
            ).<CharSequence>map(
                entry -> entry.getKey() + ";" + entry.getValue()
            ).sorted().iterator());
        }

        // unlock the object when we are done
        finally {
            unlockRead();
        }
    }
}
