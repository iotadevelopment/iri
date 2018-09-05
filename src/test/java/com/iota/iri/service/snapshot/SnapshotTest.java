package com.iota.iri.service.snapshot;

import com.iota.iri.conf.IotaConfig;
import com.iota.iri.conf.MainnetConfig;
import com.iota.iri.controllers.TipsViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.storage.Tangle;
import com.iota.iri.storage.rocksDB.RocksDBPersistenceProvider;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class SnapshotTest {
    private static final TemporaryFolder dbFolder = new TemporaryFolder();

    private static final TemporaryFolder logFolder = new TemporaryFolder();

    private static Snapshot initSnapshot;

    private static Tangle tangle;

    @BeforeClass
    public static void setup() throws Exception {
        tangle.addPersistenceProvider(
            new RocksDBPersistenceProvider(
                dbFolder.getRoot().getAbsolutePath(),
                logFolder.getRoot().getAbsolutePath(),
                1000
            )
        );
        tangle.init();

        IotaConfig config = new MainnetConfig();
        SnapshotManager snapshotManager = new SnapshotManager(tangle, new TipsViewModel(), config);
        initSnapshot = snapshotManager.getInitialSnapshot();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        tangle.shutdown();
        dbFolder.delete();
        logFolder.delete();
    }

    @Test
    public void rollBackMilestonesTest() {
        //Snapshot exampleSnapshot = new Snapshot()
    }

    private SnapshotState generateSnapshotState() {
        return null;
    }

    @Test
    public void cloneTest() {
        Hash addressWithBalance1 = Hash.NULL_HASH;
        Hash addressWithBalance2 = new Hash("JRDWYYXTVDETRZEVIKQMWZTECODXFYYYYFPPKWCJDYSMIFCKKPAVZEUXTNSVGDOGXIYTFTXATHBQLJGGC");

        HashMap<Hash, Long> originalBalances = new HashMap<>();
        originalBalances.put(addressWithBalance1, 1337L);
        originalBalances.put(addressWithBalance2, 31337L);

        Hash solidEntryPoint1Address = new Hash("SYHFAJFCXSEGCIYFNQQEBUSPGYRPQUWLXQKPDYESIZFSEZPJRHZPZHYKFSDTZSVB9ZB9SRDNIOYQ99999");
        Hash solidEntryPoint2Address = new Hash("HFJDRRJLSHNWZSDWGMQCWTRKHEX9BRIOTCBSKVFDMPRLNPKFHCTXLNBCFYVNIYYKKQMOFIIELBDC99999");

        HashMap<Hash, Integer> solidEntryPoints = new HashMap<>();
        solidEntryPoints.put(solidEntryPoint1Address, 1);
        solidEntryPoints.put(solidEntryPoint2Address, 2);

        Hash seenMilestoneTransactionHash1 = new Hash("OBWNKDXYWLWEAPYKHMBFPAYHLKUAHZFR9WDZJOLDKDOXIDTLFCWLAVCNTAZBCMUQRSHSPLNNVZLJA9999");
        Hash seenMilestoneTransactionHash2 = new Hash("GSIWHLPOFYWBQLICTAWAAAJ9JOA9NYTPX9DWLZSWRAMQPZJLFZITXTQTNZYOWJTKOVGDLAOQNDUDA9999");

        HashMap<Hash, Integer> seenMilestones = new HashMap<>();
        seenMilestones.put(seenMilestoneTransactionHash1, 3);
        seenMilestones.put(seenMilestoneTransactionHash2, 4);

        SnapshotState originalSnapshotState = new SnapshotState(originalBalances);
        int originalSnapshotIndex = 12;
        long originalSnapshotTimestamp = System.currentTimeMillis() / 1000L;

        Hash originalSnapshotHash = new Hash("FZLZCSBEXOG9ADBVFYFTBHKIZROJUENNOASNPEXEIBDZ9U9SFZJDKHFJ9BVJUZW9RXBNLJWHH9AL99999");

        Snapshot originalSnapshot = new Snapshot(
            originalSnapshotState,
            new SnapshotMetaData(
                originalSnapshotHash,
                originalSnapshotIndex,
                originalSnapshotTimestamp,
                solidEntryPoints,
                seenMilestones
            )
        );

        Snapshot clonedSnapshot = originalSnapshot.clone();

        // modify all original values //////////////////////////////////////////////////////////////////////////////////

        HashMap<Hash, Long> balanceChanges = new HashMap<>();
        balanceChanges.put(addressWithBalance1, -1000L);
        balanceChanges.put(addressWithBalance2, 1000L);
        originalSnapshot.update(new SnapshotStateDiff(balanceChanges), 8, Hash.NULL_HASH);
        originalSnapshot.getMetaData().setTimestamp(originalSnapshotTimestamp + 10);

        // check if the cloned values are still the unmodified original values /////////////////////////////////////////

        assertEquals(clonedSnapshot.getIndex(), originalSnapshotIndex);
        assertEquals(clonedSnapshot.getHash(), originalSnapshotHash);
        assertEquals(clonedSnapshot.getTimestamp(), originalSnapshotTimestamp);
        assertEquals(clonedSnapshot.getBalance(addressWithBalance1), 1337L);
        assertEquals(clonedSnapshot.getBalance(addressWithBalance2), 31337L);
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