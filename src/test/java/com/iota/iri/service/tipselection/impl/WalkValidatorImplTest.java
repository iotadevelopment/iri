package com.iota.iri.service.tipselection.impl;

import com.iota.iri.LedgerValidator;
import com.iota.iri.MilestoneTracker;
import com.iota.iri.TransactionTestUtils;
import com.iota.iri.TransactionValidator;
import com.iota.iri.conf.Configuration;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.controllers.TransactionViewModelTest;
import com.iota.iri.model.Hash;
import com.iota.iri.service.snapshot.SnapshotManager;
import com.iota.iri.storage.Tangle;
import com.iota.iri.storage.rocksDB.RocksDBPersistenceProvider;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.HashSet;

@RunWith(MockitoJUnitRunner.class)
public class WalkValidatorImplTest {

    private static final TemporaryFolder dbFolder = new TemporaryFolder();
    private static final TemporaryFolder logFolder = new TemporaryFolder();
    private static Tangle tangle;
    @Mock
    private LedgerValidator ledgerValidator;
    @Mock
    private TransactionValidator transactionValidator;
    @Mock
    private MilestoneTracker milestoneTracker;

    private static SnapshotManager snapshotManager;

    @AfterClass
    public static void tearDown() throws Exception {
        tangle.shutdown();
        dbFolder.delete();
        WalkValidatorImpl.failedBelowMaxDepthCache.clear();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        Configuration configuration = new Configuration();
        tangle = new Tangle();
        snapshotManager = new SnapshotManager(tangle, configuration);
        dbFolder.create();
        logFolder.create();
        tangle.addPersistenceProvider(new RocksDBPersistenceProvider(dbFolder.getRoot().getAbsolutePath(), logFolder
                .getRoot().getAbsolutePath(), 1000));
        tangle.init();
    }

    @Test
    public void shouldPassValidation() throws Exception {
        int depth = 15;
        TransactionViewModel tx = TransactionTestUtils.createBundleHead(0);
        tx.updateSolid(true);
        tx.store(tangle);
        Hash hash = tx.getHash();
        Mockito.when(ledgerValidator.updateDiff(new HashSet<>(), new HashMap<>(), hash))
                .thenReturn(true);
        snapshotManager.getLatestSnapshot().getMetaData().setIndex(depth);

        WalkValidatorImpl walkValidator = new WalkValidatorImpl(tangle, ledgerValidator, transactionValidator,
                milestoneTracker, snapshotManager, depth, Integer.parseInt(Configuration.BELOW_MAX_DEPTH_LIMIT) ,
                Integer.parseInt(Configuration.WALK_VALIDATOR_CACHE));
        Assert.assertTrue("Validation failed", walkValidator.isValid(hash));
    }

    @Test
    public void failOnTxType() throws Exception {
        int depth = 15;
        TransactionViewModel tx = TransactionTestUtils.createBundleHead(0);
        tx.store(tangle);
        Hash hash = tx.getTrunkTransactionHash();
        tx.updateSolid(true);
        Mockito.when(ledgerValidator.updateDiff(new HashSet<>(), new HashMap<>(), hash))
                .thenReturn(true);
        snapshotManager.getLatestSnapshot().getMetaData().setIndex(depth);

        WalkValidatorImpl walkValidator = new WalkValidatorImpl(tangle, ledgerValidator, transactionValidator,
                milestoneTracker, snapshotManager, depth, Integer.parseInt(Configuration.BELOW_MAX_DEPTH_LIMIT),
                Integer.parseInt(Configuration.WALK_VALIDATOR_CACHE));
        Assert.assertFalse("Validation succeded but should have failed since tx is missing", walkValidator.isValid(hash));
    }

    @Test
    public void failOnTxIndex() throws Exception {
        TransactionViewModel tx = TransactionTestUtils.createBundleHead(2);
        tx.store(tangle);
        Hash hash = tx.getHash();
        Mockito.when(transactionValidator.checkSolidity(hash, false))
                .thenReturn(true);
        Mockito.when(ledgerValidator.updateDiff(new HashSet<>(), new HashMap<>(), hash))
                .thenReturn(true);
        snapshotManager.getLatestSnapshot().getMetaData().setIndex(Integer.MAX_VALUE);

        WalkValidatorImpl walkValidator = new WalkValidatorImpl(tangle, ledgerValidator, transactionValidator,
                milestoneTracker, snapshotManager, 15, Integer.parseInt(Configuration.BELOW_MAX_DEPTH_LIMIT),
                Integer.parseInt(Configuration.WALK_VALIDATOR_CACHE));
        Assert.assertFalse("Validation succeded but should have failed since we are not on a tail", walkValidator.isValid(hash));
    }

    @Test
    public void failOnSolid() throws Exception {
        TransactionViewModel tx = TransactionTestUtils.createBundleHead(0);
        tx.store(tangle);
        Hash hash = tx.getHash();
        tx.updateSolid(false);
        Mockito.when(ledgerValidator.updateDiff(new HashSet<>(), new HashMap<>(), hash))
                .thenReturn(true);
        snapshotManager.getLatestSnapshot().getMetaData().setIndex(Integer.MAX_VALUE);

        WalkValidatorImpl walkValidator = new WalkValidatorImpl(tangle, ledgerValidator,transactionValidator,
                milestoneTracker, snapshotManager, 15, Integer.parseInt(Configuration.BELOW_MAX_DEPTH_LIMIT) ,
                Integer.parseInt(Configuration.WALK_VALIDATOR_CACHE));
        Assert.assertFalse("Validation succeded but should have failed since tx is not solid",
                walkValidator.isValid(hash));
    }

    @Test
    public void failOnBelowMaxDepthDueToOldMilestone() throws Exception {
        TransactionViewModel tx = TransactionTestUtils.createBundleHead(0);
        tx.store(tangle);
        tx.setSnapshot(tangle, 2);
        Hash hash = tx.getHash();
        tx.updateSolid(true);
        Mockito.when(ledgerValidator.updateDiff(new HashSet<>(), new HashMap<>(), hash))
                .thenReturn(true);
        snapshotManager.getLatestSnapshot().getMetaData().setIndex(Integer.MAX_VALUE);
        WalkValidatorImpl walkValidator = new WalkValidatorImpl(tangle, ledgerValidator, transactionValidator,
                milestoneTracker, snapshotManager, 15, Integer.parseInt(Configuration.BELOW_MAX_DEPTH_LIMIT),
                Integer.parseInt(Configuration.WALK_VALIDATOR_CACHE));
        Assert.assertFalse("Validation succeeded but should have failed tx is below max depth",
                walkValidator.isValid(hash));
    }

    @Test
    public void belowMaxDepthWithFreshMilestone() throws Exception {
        TransactionViewModel tx = TransactionTestUtils.createBundleHead(0);
        tx.store(tangle);
        tx.setSnapshot(tangle, 92);
        Hash hash = tx.getHash();
        for (int i = 0; i < 4 ; i++) {
            tx = new TransactionViewModel(TransactionViewModelTest.getRandomTransactionWithTrunkAndBranch(hash, hash), TransactionViewModelTest.getRandomTransactionHash());
            TransactionTestUtils.setLastIndex(tx,0);
            TransactionTestUtils.setCurrentIndex(tx,0);
            tx.updateSolid(true);
            hash = tx.getHash();
            tx.store(tangle);
        }
        Mockito.when(ledgerValidator.updateDiff(new HashSet<>(), new HashMap<>(), hash))
                .thenReturn(true);
        snapshotManager.getLatestSnapshot().getMetaData().setIndex(100);
        WalkValidatorImpl walkValidator = new WalkValidatorImpl(tangle, ledgerValidator, transactionValidator,
                milestoneTracker, snapshotManager, 15, Integer.parseInt(Configuration.BELOW_MAX_DEPTH_LIMIT) ,
                Integer.parseInt(Configuration.WALK_VALIDATOR_CACHE));
        Assert.assertTrue("Validation failed but should have succeeded since tx is above max depth",
                walkValidator.isValid(hash));
    }

    @Test
    public void failBelowMaxDepthWithFreshMilestoneDueToLongChain() throws Exception {
        final int maxAnalyzedTxs = Integer.parseInt(Configuration.BELOW_MAX_DEPTH_LIMIT);
        TransactionViewModel tx = TransactionTestUtils.createBundleHead(0);
        tx.store(tangle);
        tx.setSnapshot(tangle, 92);
        Hash hash = tx.getHash();
        for (int i = 0; i < maxAnalyzedTxs ; i++) {
            tx = new TransactionViewModel(TransactionViewModelTest.getRandomTransactionWithTrunkAndBranch(hash, hash),
                    TransactionViewModelTest.getRandomTransactionHash());
            TransactionTestUtils.setLastIndex(tx,0);
            TransactionTestUtils.setCurrentIndex(tx,0);
            hash = tx.getHash();
            tx.store(tangle);
        }
        Mockito.when(transactionValidator.checkSolidity(hash, false))
                .thenReturn(true);
        Mockito.when(ledgerValidator.updateDiff(new HashSet<>(), new HashMap<>(), hash))
                .thenReturn(true);
        snapshotManager.getLatestSnapshot().getMetaData().setIndex(100);
        WalkValidatorImpl walkValidator = new WalkValidatorImpl(tangle, ledgerValidator, transactionValidator,
                milestoneTracker, snapshotManager, 15, maxAnalyzedTxs,
                Integer.parseInt(Configuration.WALK_VALIDATOR_CACHE));
        Assert.assertFalse("Validation succeeded but should have failed since tx is below max depth",
                walkValidator.isValid(hash));
    }

    @Test
    public void belowMaxDepthOnGenesis() throws Exception {
        TransactionViewModel tx = null;
        final int maxAnalyzedTxs = Integer.parseInt(Configuration.BELOW_MAX_DEPTH_LIMIT);
        Hash hash = Hash.NULL_HASH;
        for (int i = 0; i < maxAnalyzedTxs - 2 ; i++) {
            tx = new TransactionViewModel(TransactionViewModelTest.getRandomTransactionWithTrunkAndBranch(hash, hash), TransactionViewModelTest.getRandomTransactionHash());
            TransactionTestUtils.setLastIndex(tx,0);
            TransactionTestUtils.setCurrentIndex(tx,0);
            tx.updateSolid(true);
            hash = tx.getHash();
            tx.store(tangle);
        }
        Mockito.when(ledgerValidator.updateDiff(new HashSet<>(), new HashMap<>(), tx.getHash()))
                .thenReturn(true);
        snapshotManager.getLatestSnapshot().getMetaData().setIndex(15);
        WalkValidatorImpl walkValidator = new WalkValidatorImpl(tangle, ledgerValidator, transactionValidator,
                milestoneTracker, snapshotManager, 15, maxAnalyzedTxs,
                Integer.parseInt(Configuration.WALK_VALIDATOR_CACHE));
        Assert.assertTrue("Validation failed but should have succeeded. We didn't exceed the maximal amount of" +
                        "transactions that may be analyzed.",
                walkValidator.isValid(tx.getHash()));
    }

    @Test
    public void failBelowMaxDepthOnGenesisDueToLongChain() throws Exception {
        final int maxAnalyzedTxs = Integer.parseInt(Configuration.BELOW_MAX_DEPTH_LIMIT);
        TransactionViewModel tx = null;
        Hash hash = Hash.NULL_HASH;
        for (int i = 0; i < maxAnalyzedTxs; i++) {
            tx = new TransactionViewModel(TransactionViewModelTest.getRandomTransactionWithTrunkAndBranch(hash, hash),
                    TransactionViewModelTest.getRandomTransactionHash());
            TransactionTestUtils.setLastIndex(tx,0);
            TransactionTestUtils.setCurrentIndex(tx,0);
            tx.store(tangle);
            hash = tx.getHash();
        }
        Mockito.when(transactionValidator.checkSolidity(tx.getHash(), false))
                .thenReturn(true);
        Mockito.when(ledgerValidator.updateDiff(new HashSet<>(), new HashMap<>(), tx.getHash()))
                .thenReturn(true);
        snapshotManager.getLatestSnapshot().getMetaData().setIndex(17);
        WalkValidatorImpl walkValidator = new WalkValidatorImpl(tangle, ledgerValidator, transactionValidator,
                milestoneTracker, snapshotManager, 15, maxAnalyzedTxs,
                Integer.parseInt(Configuration.WALK_VALIDATOR_CACHE));
        Assert.assertFalse("Validation succeeded but should have failed. We exceeded the maximal amount of" +
                        "transactions that may be analyzed.",
                walkValidator.isValid(tx.getHash()));
    }

    @Test
    public void failOnInconsistency() throws Exception {
        TransactionViewModel tx = TransactionTestUtils.createBundleHead(0);
        tx.store(tangle);
        Hash hash = tx.getHash();
        Mockito.when(transactionValidator.checkSolidity(hash, false))
                .thenReturn(true);
        Mockito.when(ledgerValidator.updateDiff(new HashSet<>(), new HashMap<>(), hash))
                .thenReturn(false);
        snapshotManager.getLatestSnapshot().getMetaData().setIndex(Integer.MAX_VALUE);

        WalkValidatorImpl walkValidator = new WalkValidatorImpl(tangle, ledgerValidator, transactionValidator,
                milestoneTracker, snapshotManager, 15, Integer.parseInt(Configuration.BELOW_MAX_DEPTH_LIMIT),
                Integer.parseInt(Configuration.WALK_VALIDATOR_CACHE));
        Assert.assertFalse("Validation succeded but should have failed due to inconsistent ledger state",
                walkValidator.isValid(hash));
    }
}