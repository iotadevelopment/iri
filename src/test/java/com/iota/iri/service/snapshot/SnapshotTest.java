package com.iota.iri.service.snapshot;

import com.iota.iri.conf.Configuration;
import com.iota.iri.model.Hash;
import com.iota.iri.storage.Tangle;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertFalse;

public class SnapshotTest {
    private static Snapshot initSnapshot;

    @BeforeClass
    public static void beforeClass() {
        Configuration configuration = new Configuration();

        configuration.put(Configuration.DefaultConfSettings.SNAPSHOT_FILE, Configuration.MAINNET_SNAPSHOT_FILE);
        configuration.put(Configuration.DefaultConfSettings.SNAPSHOT_SIGNATURE_FILE, Configuration.MAINNET_SNAPSHOT_SIG_FILE);
        configuration.put(Configuration.DefaultConfSettings.TESTNET, "false");

        try {
            SnapshotManager snapshotManager = new SnapshotManager(new Tangle(), configuration);

            initSnapshot = snapshotManager.getInitialSnapshot();
        } catch (IOException e) {
            throw new UncheckedIOException("Problem initiating snapshot", e);
        }
    }

    @Test
    public void getState() {
        //Assert.assertTrue(latestSnapshot.getState().equals(Snapshot.initialState));
    }

    @Test
    public void isConsistent() {
        Assert.assertTrue("Initial confirmed should be consistent", initSnapshot.getState().isConsistent());
    }

    @Test
    public void patch() {
        Map.Entry<Hash, Long> firstOne = initSnapshot.getState().state.entrySet().iterator().next();
        Hash someHash = new Hash("PSRQPWWIECDGDDZXHGJNMEVJNSVOSMECPPVRPEVRZFVIZYNNXZNTOTJOZNGCZNQVSPXBXTYUJUOXYASLS");
        Map<Hash, Long> diff = new HashMap<>();
        diff.put(firstOne.getKey(), -firstOne.getValue());
        diff.put(someHash, firstOne.getValue());
        Assert.assertNotEquals(0, diff.size());
        Assert.assertTrue("The ledger should be consistent", initSnapshot.getState().patchedState(new SnapshotStateDiff(diff)).isConsistent());
    }

    @Test
    public void applyShouldFail() {
        Snapshot latestSnapshot = initSnapshot.clone();
        Map<Hash, Long> badMap = new HashMap<>();
        badMap.put(new Hash("PSRQPWWIECDGDDZEHGJNMEVJNSVOSMECPPVRPEVRZFVIZYNNXZNTOTJOZNGCZNQVSPXBXTYUJUOXYASLS"), 100L);
        badMap.put(new Hash("ESRQPWWIECDGDDZEHGJNMEVJNSVOSMECPPVRPEVRZFVIZYNNXZNTOTJOZNGCZNQVSPXBXTYUJUOXYASLS"), -100L);
        assertFalse("should be inconsistent", latestSnapshot.getState().patchedState(new SnapshotStateDiff(badMap)).isConsistent());
    }

    private Map<Hash, Long> getModifiedMap() {
        Hash someHash = new Hash("PSRQPWWIECDGDDZXHGJNMEVJNSVOSMECPPVRPEVRZFVIZYNNXZNTOTJOZNGCZNQVSPXBXTYUJUOXYASLS");
        Map<Hash, Long> newMap;
        newMap = new HashMap<>();
        Iterator<Map.Entry<Hash, Long>> iterator = newMap.entrySet().iterator();
        Map.Entry<Hash, Long> entry;
        if(iterator.hasNext()) {
            entry = iterator.next();
            Long value = entry.getValue();
            Hash hash = entry.getKey();
            newMap.put(hash, 0L);
            newMap.put(someHash, value);
        }
        return newMap;
    }
}