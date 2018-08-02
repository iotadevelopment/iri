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

public class SnapshotState {
    /**
     * Logger for this class (used to emit debug messages).
     */
    private static final Logger log = LoggerFactory.getLogger(SnapshotState.class);

    /**
     * Underlying Map storing the balances of addresses.
     */
    protected final Map<Hash, Long> state;

    public SnapshotState(Map<Hash, Long> initialState) {
        this.state = new HashMap<>(initialState);
    }

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
        // calculate the sum of all balances
        long supply = state.values().stream().reduce(Math::addExact).orElse(Long.MAX_VALUE);

        // if the sum differs from the expected supply -> dump an error and return false ...
        if(supply != TransactionViewModel.SUPPLY) {
            log.error("Supply differs from the expected supply by: {}", TransactionViewModel.SUPPLY - supply);

            return false;
        }

        // ... otherwise return true
        return true;
    }

    public Long getBalance(Hash address) {
        return this.state.get(address);
    }

    public SnapshotState clone() {
        return new SnapshotState(this.state);
    }
}
