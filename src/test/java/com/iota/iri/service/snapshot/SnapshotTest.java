package com.iota.iri.service.snapshot;

import com.iota.iri.TransactionTestUtils;
import com.iota.iri.conf.IotaConfig;
import com.iota.iri.conf.MainnetConfig;
import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.controllers.StateDiffViewModel;
import com.iota.iri.controllers.TipsViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.model.*;
import com.iota.iri.storage.Tangle;
import com.iota.iri.storage.rocksDB.RocksDBPersistenceProvider;
import com.iota.iri.utils.Pair;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SnapshotTest {
    private static final MainnetConfig config = new MainnetConfig();

    private static final TemporaryFolder dbFolder = new TemporaryFolder();

    private static final TemporaryFolder logFolder = new TemporaryFolder();

    private static Snapshot initSnapshot;

    private static Tangle tangle;

    private static SnapshotManager snapshotManager;

    private static Hash INITIAL_SNAPSHOT_HASH = Hash.NULL_HASH;

    private static int INITIAL_SNAPSHOT_INDEX = config.getMilestoneStartIndex();

    private static long INITIAL_SNAPSHOT_TIMESTAMP = System.currentTimeMillis() / 1000L;

    enum AddressesWithBalance {
        ADDRESS_1("JRDWYYXTVDETRZEVIKQMWZTECODXFYYYYFPPKWCJDYSMIFCKKPAVZEUXTNSVGDOGXIYTFTXATHBQLJGGC", 31337L),
        ADDRESS_2("YYFPPKWCJDYSMIFCKKPAVZEUXTNSVGDOGXIYTFTXATHBQLJGGCJRDWYYXTVDETRZEVIKQMWZTECODXFYY", 1234L),
        ADDRESS_3("VZEUXTNSVGDOGXIYTFTXATHBQYYFPPKWCJDYSMIFCKKPALJGGCJRDWYYXTVDETRZEVIKQMWZTECODXFYY", 4284L);

        private final Hash addressHash;

        private final long addressBalance;

        AddressesWithBalance(String addressTrytes, long balance) {
            this.addressHash = new Hash(addressTrytes);
            this.addressBalance = balance;
        }

        public Hash getHash() {
            return addressHash;
        }

        public long getBalance() {
            return addressBalance;
        }
    }

    enum Milestones {
        MILESTONE_1(
            "COSJRQMXDPQVZHDF9MVSDQF9TCNQAIJIGUEYFMEL9MOG9LNAUAXCFDRVY9APH99UCDNLABCLWFQ9A9999",
            config.getMilestoneStartIndex() + 1,
            INITIAL_SNAPSHOT_TIMESTAMP + 100,
            new Pair<>(AddressesWithBalance.ADDRESS_1.getHash(), -1400L),
            new Pair<>(AddressesWithBalance.ADDRESS_2.getHash(), 1000L),
            new Pair<>(AddressesWithBalance.ADDRESS_3.getHash(), 400L)
        ),

        MILESTONE_2(
            "DNLMVSDQCOSJRQMXDPF9TCNQAIJIGUQVZHDF9CFDRVY9APH99UCEYFMEL9MOG9LNAUAXABCLWFQ9A9999",
            config.getMilestoneStartIndex() + 2,
            INITIAL_SNAPSHOT_TIMESTAMP + 9300,
            new Pair<>(AddressesWithBalance.ADDRESS_1.getHash(), 80L),
            new Pair<>(AddressesWithBalance.ADDRESS_3.getHash(), -80L)
        ),

        MILESTONE_3(
            "QVZHDF9CFDRVY9APH99UCDNLMVSDQCOSJRQMXDPF9TCNQAIJIGUEYFMEL9MOG9LNAUAXABCLWFQ9A9999",
            config.getMilestoneStartIndex() + 3,
            INITIAL_SNAPSHOT_TIMESTAMP + 1300,
            new Pair<>(AddressesWithBalance.ADDRESS_1.getHash(), 200L),
            new Pair<>(AddressesWithBalance.ADDRESS_2.getHash(), -200L)
        ),

        MILESTONE_4(
            "F9MVSDQF9TCNQAIJIGUEYFMCOSJRQMXDPQVZHDEL9MOG9LNAUAXCFDRVY9APH99UCDNLABCLWFQ9A9999",
            config.getMilestoneStartIndex() + 4,
            INITIAL_SNAPSHOT_TIMESTAMP + 700,
            new Pair<>(AddressesWithBalance.ADDRESS_1.getHash(), 133L),
            new Pair<>(AddressesWithBalance.ADDRESS_3.getHash(), -133L)
        );

        private final Hash hash;

        private final int index;

        private final long timestamp;

        private final HashMap<Hash, Long> balanceChanges = new HashMap<>();

        Milestones(String hashTrytes, int index, long timestamp, Pair<Hash, Long>... balanceChanges) {
            this.hash = new Hash(hashTrytes);
            this.index = index;
            this.timestamp = timestamp;

            for (Pair<Hash, Long> balanceChange : balanceChanges) {
                this.balanceChanges.put(balanceChange.low, balanceChange.hi);
            }
        }

        public Hash getHash() {
            return this.hash;
        }

        public int getIndex() {
            return this.index;
        }

        public long getTimestamp() {
            return this.timestamp;
        }

        public HashMap<Hash, Long> getBalanceChanges() {
            return this.balanceChanges;
        }
    }

    @BeforeClass
    public static void setup() throws Exception {
        dbFolder.create();
        logFolder.create();

        tangle = new Tangle();
        tangle.addPersistenceProvider(
            new RocksDBPersistenceProvider(
                dbFolder.getRoot().getAbsolutePath(),
                logFolder.getRoot().getAbsolutePath(),
                1000
            )
        );
        tangle.init();
        snapshotManager = new SnapshotManager(tangle, new TipsViewModel(), config);
        IotaConfig config = new MainnetConfig();
        SnapshotManager snapshotManager = new SnapshotManager(tangle, new TipsViewModel(), config);
        initSnapshot = snapshotManager.getInitialSnapshot();
    }

    private static void createMilestonesInDatabase(Milestones... milestones) throws Exception {
        for (Milestones currentMilestone: milestones) {
            createMilestoneInDatabase(currentMilestone.getIndex(), currentMilestone.getHash(), currentMilestone.getTimestamp(), currentMilestone.getBalanceChanges());
        }
    }

    private static void createMilestoneInDatabase(int milestoneIndex, Hash transactionHash, long timestamp, HashMap<Hash, Long> balanceChanges) throws Exception {
        new MilestoneViewModel(milestoneIndex, transactionHash).store(tangle);
        new StateDiffViewModel(balanceChanges, transactionHash).store(tangle);

        TransactionViewModel transaction = TransactionTestUtils.createTransactionWithTrytes("FAKEMILESTONETX", transactionHash);
        TransactionTestUtils.setTimestamp(transaction, timestamp);
        transaction.store(tangle, snapshotManager);
    }

    private static void removeMilestonesFromDatabase(Milestones... milestones) throws Exception {
        for (Milestones currentMilestone: milestones) {
            removeMilestoneFromDatabase(currentMilestone.getIndex(), currentMilestone.getHash());
        }
    }

    /**
     * This method is a utility method that allows us to remove created fake milestones from the database.
     *
     * @param milestoneIndex
     * @param transactionHash
     * @throws Exception
     */
    private static void removeMilestoneFromDatabase(int milestoneIndex, Hash transactionHash) throws Exception {
        tangle.delete(new Milestone().getClass(), new IntegerIndex(milestoneIndex));
        tangle.delete(new StateDiff().getClass(), transactionHash);
        tangle.delete(new Transaction().getClass(), transactionHash);

        MilestoneViewModel.clear(milestoneIndex);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        tangle.shutdown();
        dbFolder.delete();
        logFolder.delete();
    }

    @Test
    public void concurrencyTest() throws Exception {
        createMilestonesInDatabase(Milestones.MILESTONE_1);

        long scanStart = System.currentTimeMillis();

        for (int i = 0; i < 100000; i++) {
            TransactionViewModel.fromHash(tangle, Milestones.MILESTONE_1.getHash()).setSnapshot(tangle, snapshotManager, i);
            assertEquals(TransactionViewModel.fromHash(tangle, Milestones.MILESTONE_1.getHash()).snapshotIndex(), i);
        }

        System.out.println(System.currentTimeMillis() - scanStart);

        // clean up
        removeMilestonesFromDatabase(Milestones.MILESTONE_1);
    }

    @Test
    public void rollbackLastMilestoneTest() throws Exception {
        // generate our starting milestone
        Snapshot testSnapshot = getTestSnapshot();

        // initialize the database with some milestones
        createMilestonesInDatabase(Milestones.MILESTONE_1, Milestones.MILESTONE_4);

        // replay the two milestones
        testSnapshot.replayMilestones(Milestones.MILESTONE_4.getIndex(), tangle);

        // create some milestones to check if additional milestones get handled correctly
        createMilestonesInDatabase(Milestones.MILESTONE_2, Milestones.MILESTONE_3);

        // rollback milestone #4
        assertTrue(testSnapshot.rollbackLastMilestone(tangle));

        // check if values are correct after the rollback
        assertEquals(testSnapshot.getIndex(), Milestones.MILESTONE_1.getIndex());
        assertEquals(testSnapshot.getHash(), Milestones.MILESTONE_1.getHash());
        assertEquals(testSnapshot.getTimestamp(), Milestones.MILESTONE_1.getTimestamp());
        assertEquals(testSnapshot.getBalance(AddressesWithBalance.ADDRESS_1.getHash()), AddressesWithBalance.ADDRESS_1.getBalance() - 1400L);
        assertEquals(testSnapshot.getBalance(AddressesWithBalance.ADDRESS_2.getHash()), AddressesWithBalance.ADDRESS_2.getBalance() + 1000L);
        assertEquals(testSnapshot.getBalance(AddressesWithBalance.ADDRESS_3.getHash()), AddressesWithBalance.ADDRESS_3.getBalance() + 400L);

        // rollback milestone #1
        assertTrue(testSnapshot.rollbackLastMilestone(tangle));

        // check if values are correct after the rollback
        assertEquals(testSnapshot.getIndex(), INITIAL_SNAPSHOT_INDEX);
        assertEquals(testSnapshot.getHash(), INITIAL_SNAPSHOT_HASH);
        assertEquals(testSnapshot.getTimestamp(), INITIAL_SNAPSHOT_TIMESTAMP);
        assertEquals(testSnapshot.getBalance(AddressesWithBalance.ADDRESS_1.getHash()), AddressesWithBalance.ADDRESS_1.getBalance());
        assertEquals(testSnapshot.getBalance(AddressesWithBalance.ADDRESS_2.getHash()), AddressesWithBalance.ADDRESS_2.getBalance());
        assertEquals(testSnapshot.getBalance(AddressesWithBalance.ADDRESS_3.getHash()), AddressesWithBalance.ADDRESS_3.getBalance());

        // rollback non existent milestone
        assertFalse(testSnapshot.rollbackLastMilestone(tangle));

        // clean up
        removeMilestonesFromDatabase(Milestones.MILESTONE_1, Milestones.MILESTONE_2, Milestones.MILESTONE_3, Milestones.MILESTONE_4);
    }

    @Test
    public void rollBackMilestonesTest() throws Exception {
        // generate our starting milestone
        Snapshot testSnapshot = getTestSnapshot();

        // initialize the database with some milestones
        createMilestonesInDatabase(Milestones.MILESTONE_1, Milestones.MILESTONE_3, Milestones.MILESTONE_4);

        // apply the changes
        testSnapshot.replayMilestones(Milestones.MILESTONE_4.getIndex(), tangle);

        // check if the starting values are correct
        assertEquals(testSnapshot.getIndex(), Milestones.MILESTONE_4.getIndex());
        assertEquals(testSnapshot.getHash(), Milestones.MILESTONE_4.getHash());
        assertEquals(testSnapshot.getTimestamp(), Milestones.MILESTONE_4.getTimestamp());
        assertEquals(testSnapshot.getBalance(AddressesWithBalance.ADDRESS_1.getHash()), AddressesWithBalance.ADDRESS_1.getBalance() - 1067L);
        assertEquals(testSnapshot.getBalance(AddressesWithBalance.ADDRESS_2.getHash()), AddressesWithBalance.ADDRESS_2.getBalance() + 800L);
        assertEquals(testSnapshot.getBalance(AddressesWithBalance.ADDRESS_3.getHash()), AddressesWithBalance.ADDRESS_3.getBalance() + 267L);

        // create a previously unknown milestone in the applied range before we revert -> should be ignored by the reversion
        createMilestonesInDatabase(Milestones.MILESTONE_2);

        // revert the changes
        testSnapshot.rollBackMilestones(Milestones.MILESTONE_1.getIndex(), tangle);

        // check if the values were rolled back
        assertEquals(testSnapshot.getIndex(), INITIAL_SNAPSHOT_INDEX);
        assertEquals(testSnapshot.getHash(), INITIAL_SNAPSHOT_HASH);
        assertEquals(testSnapshot.getTimestamp(), INITIAL_SNAPSHOT_TIMESTAMP);
        assertEquals(testSnapshot.getBalance(AddressesWithBalance.ADDRESS_1.getHash()), AddressesWithBalance.ADDRESS_1.getBalance());
        assertEquals(testSnapshot.getBalance(AddressesWithBalance.ADDRESS_2.getHash()), AddressesWithBalance.ADDRESS_2.getBalance());
        assertEquals(testSnapshot.getBalance(AddressesWithBalance.ADDRESS_3.getHash()), AddressesWithBalance.ADDRESS_3.getBalance());

        // clean up
        removeMilestonesFromDatabase(Milestones.MILESTONE_1, Milestones.MILESTONE_2, Milestones.MILESTONE_3, Milestones.MILESTONE_4);
    }

    @Test
    public void replayMilestonesTest() throws Exception {
        // generate our starting milestone
        Snapshot testSnapshot = getTestSnapshot();

        // initialize the database with some milestones
        createMilestonesInDatabase(Milestones.MILESTONE_1, Milestones.MILESTONE_3, Milestones.MILESTONE_4);

        // check if the starting values are correct
        assertEquals(testSnapshot.getIndex(), INITIAL_SNAPSHOT_INDEX);
        assertEquals(testSnapshot.getHash(), INITIAL_SNAPSHOT_HASH);
        assertEquals(testSnapshot.getTimestamp(), INITIAL_SNAPSHOT_TIMESTAMP);
        for (AddressesWithBalance currentAddress : AddressesWithBalance.values()) {
            assertEquals(testSnapshot.getBalance(currentAddress.getHash()), currentAddress.getBalance());
        }

        // apply the changes
        testSnapshot.replayMilestones(Milestones.MILESTONE_3.getIndex(), tangle);

        // check if the values have changed
        assertEquals(testSnapshot.getIndex(), Milestones.MILESTONE_3.getIndex());
        assertEquals(testSnapshot.getHash(), Milestones.MILESTONE_3.getHash());
        assertEquals(testSnapshot.getTimestamp(), Milestones.MILESTONE_3.getTimestamp());
        assertEquals(testSnapshot.getBalance(AddressesWithBalance.ADDRESS_1.getHash()), AddressesWithBalance.ADDRESS_1.getBalance() - 1200L);
        assertEquals(testSnapshot.getBalance(AddressesWithBalance.ADDRESS_2.getHash()), AddressesWithBalance.ADDRESS_2.getBalance() + 800L);
        assertEquals(testSnapshot.getBalance(AddressesWithBalance.ADDRESS_3.getHash()), AddressesWithBalance.ADDRESS_3.getBalance() + 400L);

        // create a previously unknown milestone which should be ignored since its not in range anymore
        createMilestonesInDatabase(Milestones.MILESTONE_2);

        // apply additional changes
        testSnapshot.replayMilestones(Milestones.MILESTONE_4.getIndex(), tangle);

        // check if the values have changed
        assertEquals(testSnapshot.getIndex(), Milestones.MILESTONE_4.getIndex());
        assertEquals(testSnapshot.getHash(), Milestones.MILESTONE_4.getHash());
        assertEquals(testSnapshot.getTimestamp(), Milestones.MILESTONE_4.getTimestamp());
        assertEquals(testSnapshot.getBalance(AddressesWithBalance.ADDRESS_1.getHash()), AddressesWithBalance.ADDRESS_1.getBalance() - 1067L);
        assertEquals(testSnapshot.getBalance(AddressesWithBalance.ADDRESS_2.getHash()), AddressesWithBalance.ADDRESS_2.getBalance() + 800L);
        assertEquals(testSnapshot.getBalance(AddressesWithBalance.ADDRESS_3.getHash()), AddressesWithBalance.ADDRESS_3.getBalance() + 267L);

        // clean up
        removeMilestonesFromDatabase(Milestones.MILESTONE_1, Milestones.MILESTONE_2, Milestones.MILESTONE_3, Milestones.MILESTONE_4);
    }

    private SnapshotState generateSnapshotState() {
        return null;
    }

    @Test
    public void cloneTest() {
        Snapshot originalSnapshot = getTestSnapshot();
        Snapshot clonedSnapshot = originalSnapshot.clone();

        // read all start values ///////////////////////////////////////////////////////////////////////////////////////

        final int originalSnapshotIndex = originalSnapshot.getIndex();
        final Hash originalSnapshotHash = originalSnapshot.getHash();
        final long originalSnapshotTimestamp = originalSnapshot.getTimestamp();

        // modify all original values //////////////////////////////////////////////////////////////////////////////////

        HashMap<Hash, Long> balanceChanges = new HashMap<>();
        balanceChanges.put(AddressesWithBalance.ADDRESS_1.getHash(), -1000L);
        balanceChanges.put(AddressesWithBalance.ADDRESS_2.getHash(), 1000L);
        originalSnapshot.update(new SnapshotStateDiff(balanceChanges), 8, Hash.NULL_HASH);
        originalSnapshot.getMetaData().setTimestamp(originalSnapshotTimestamp + 10);

        // check if the cloned values are still the unmodified original values /////////////////////////////////////////

        assertEquals(clonedSnapshot.getIndex(), originalSnapshotIndex);
        assertEquals(clonedSnapshot.getHash(), originalSnapshotHash);
        assertEquals(clonedSnapshot.getTimestamp(), originalSnapshotTimestamp);
        for(AddressesWithBalance currentBalance : AddressesWithBalance.values()) {
            assertEquals(clonedSnapshot.getBalance(currentBalance.getHash()), currentBalance.getBalance());
        }
    }

    private Snapshot getTestSnapshot() {
        HashMap<Hash, Long> originalBalances = new HashMap<>();
        for(AddressesWithBalance currentBalance : AddressesWithBalance.values()) {
            originalBalances.put(currentBalance.getHash(), currentBalance.getBalance());
        }

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

        return new Snapshot(
                new SnapshotState(originalBalances),
                new SnapshotMetaData(
                        INITIAL_SNAPSHOT_HASH,
                        INITIAL_SNAPSHOT_INDEX,
                        INITIAL_SNAPSHOT_TIMESTAMP,
                        solidEntryPoints,
                        seenMilestones
                )
        );
    }

    @Test
    public void getState() {
        //Assert.assertTrue(latestSnapshot.getState().equals(Snapshot.initialState));
    }

    @Test
    public void isConsistent() {
        Assert.assertTrue("Initial confirmed should be consistent", initSnapshot.isConsistent());
    }

    @Test
    public void patch() {
        Map.Entry<Hash, Long> firstOne = initSnapshot.state.balances.entrySet().iterator().next();
        Hash someHash = new Hash("PSRQPWWIECDGDDZXHGJNMEVJNSVOSMECPPVRPEVRZFVIZYNNXZNTOTJOZNGCZNQVSPXBXTYUJUOXYASLS");
        Map<Hash, Long> diff = new HashMap<>();
        diff.put(firstOne.getKey(), -firstOne.getValue());
        diff.put(someHash, firstOne.getValue());
        Assert.assertNotEquals(0, diff.size());
        Assert.assertTrue("The ledger should be consistent", initSnapshot.patchedState(new SnapshotStateDiff(diff)).isConsistent());
    }

    @Test
    public void applyShouldFail() {
        Snapshot latestSnapshot = initSnapshot.clone();
        Map<Hash, Long> badMap = new HashMap<>();
        badMap.put(new Hash("PSRQPWWIECDGDDZEHGJNMEVJNSVOSMECPPVRPEVRZFVIZYNNXZNTOTJOZNGCZNQVSPXBXTYUJUOXYASLS"), 100L);
        badMap.put(new Hash("ESRQPWWIECDGDDZEHGJNMEVJNSVOSMECPPVRPEVRZFVIZYNNXZNTOTJOZNGCZNQVSPXBXTYUJUOXYASLS"), -100L);
        assertFalse("should be inconsistent", latestSnapshot.patchedState(new SnapshotStateDiff(badMap)).isConsistent());
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