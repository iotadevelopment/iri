package com.iota.iri;

import com.iota.iri.conf.MainnetConfig;
import com.iota.iri.controllers.TipsViewModel;
import com.iota.iri.controllers.TransactionViewModel;
import com.iota.iri.hash.SpongeFactory;
import com.iota.iri.model.Hash;
import com.iota.iri.network.TransactionRequester;
import com.iota.iri.service.snapshot.SnapshotManager;
import com.iota.iri.storage.Tangle;
import com.iota.iri.storage.rocksDB.RocksDBPersistenceProvider;
import com.iota.iri.utils.Converter;
import com.iota.iri.zmq.MessageQ;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.util.HashMap;

import static com.iota.iri.controllers.TransactionViewModelTest.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Created by paul on 5/14/17. */
public class TransactionValidatorTest {

  private static final int MAINNET_MWM = 14;
  private static final TemporaryFolder dbFolder = new TemporaryFolder();
  private static final TemporaryFolder logFolder = new TemporaryFolder();
  private static Tangle tangle;
  private static SnapshotManager snapshotManager;
  private static MilestoneTracker milestoneTracker;
  private static TransactionValidator txValidator;

  @BeforeClass
  public static void setUp() throws Exception {
    dbFolder.create();
    logFolder.create();
    tangle = new Tangle();
    snapshotManager = new SnapshotManager(tangle, new MainnetConfig());
    tangle.addPersistenceProvider(
        new RocksDBPersistenceProvider(
            dbFolder.getRoot().getAbsolutePath(), logFolder.getRoot().getAbsolutePath(),1000));
    tangle.init();
    TipsViewModel tipsViewModel = new TipsViewModel();
    MessageQ messageQ = Mockito.mock(MessageQ.class);
    TransactionRequester txRequester = new TransactionRequester(tangle, snapshotManager, messageQ);
    MainnetConfig config = new MainnetConfig();
    txValidator = new TransactionValidator(tangle, snapshotManager, tipsViewModel, txRequester, messageQ, config);
    txValidator.setMwm(false, MAINNET_MWM);
    milestoneTracker = new MilestoneTracker(tangle, snapshotManager, txValidator, Mockito.mock(TransactionRequester.class), messageQ, config);
    txValidator.init(false, MAINNET_MWM, milestoneTracker);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    tangle.shutdown();
    dbFolder.delete();
    logFolder.delete();
  }

  @Test
  public void testMinMwm() throws InterruptedException {
    txValidator.init(false, 5, milestoneTracker);
    assertTrue(txValidator.getMinWeightMagnitude() == 13);
    txValidator.shutdown();
    txValidator.init(false, MAINNET_MWM, milestoneTracker);
  }

  @Test
  public void validateBytes() throws Exception {
    byte[] trits = getRandomTransactionTrits();
    Converter.copyTrits(0, trits, 0, trits.length);
    byte[] bytes = Converter.allocateBytesForTrits(trits.length);
    Converter.bytes(trits, bytes);
    txValidator.validateBytes(bytes, MAINNET_MWM);
  }

  @Test
  public void validateTrits() {
    byte[] trits = getRandomTransactionTrits();
    Converter.copyTrits(0, trits, 0, trits.length);
    txValidator.validateTrits(trits, MAINNET_MWM);
  }

  @Test(expected = RuntimeException.class)
  public void validateTritsWithInvalidMetadata() {
    byte[] trits = getRandomTransactionTrits();
    txValidator.validateTrits(trits, MAINNET_MWM);
  }

  @Test
  public void validateBytesWithNewCurl() throws Exception {
    byte[] trits = getRandomTransactionTrits();
    Converter.copyTrits(0, trits, 0, trits.length);
    byte[] bytes = Converter.allocateBytesForTrits(trits.length);
    Converter.bytes(trits, 0, bytes, 0, trits.length);
    txValidator.validateBytes(bytes, txValidator.getMinWeightMagnitude(), SpongeFactory.create(SpongeFactory.Mode.CURLP81));
  }

  @Test
  public void verifyTxIsSolid() throws Exception {
    TransactionViewModel tx = getTxWithBranchAndTrunk();
    assertTrue(txValidator.checkSolidity(tx.getHash(), false));
    assertTrue(txValidator.checkSolidity(tx.getHash(), true));
  }

  @Test
  public void verifyTxIsNotSolid() throws Exception {
    TransactionViewModel tx = getTxWithoutBranchAndTrunk();
    assertFalse(txValidator.checkSolidity(tx.getHash(), false));
    assertFalse(txValidator.checkSolidity(tx.getHash(), true));
  }

  @Test
  public void addSolidTransactionWithoutErrors() {
    byte[] trits = getRandomTransactionTrits();
    Converter.copyTrits(0, trits, 0, trits.length);
    txValidator.addSolidTransaction(Hash.calculate(SpongeFactory.Mode.CURLP81, trits));
  }

  private TransactionViewModel getTxWithBranchAndTrunk() throws Exception {
    TransactionViewModel tx, trunkTx, branchTx;
    String trytes = "999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999CFDEZBLZQYA9999999999999999999999999999999999999999999ZZWQHWD99C99999999C99999999CKWWDBWSCLMQULCTAAJGXDEMFJXPMGMAQIHDGHRBGEMUYNNCOK9YPHKEEFLFCZUSPMCJHAKLCIBQSGWAS999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999";

    byte[] trits = Converter.allocateTritsForTrytes(trytes.length());
    Converter.trits(trytes, trits, 0);
    trunkTx = new TransactionViewModel(trits, Hash.calculate(SpongeFactory.Mode.CURLP81, trits));
    branchTx = new TransactionViewModel(trits, Hash.calculate(SpongeFactory.Mode.CURLP81, trits));

    byte[] childTx = getRandomTransactionTrits();
    System.arraycopy(trunkTx.getHash().trits(), 0, childTx, TransactionViewModel.TRUNK_TRANSACTION_TRINARY_OFFSET, TransactionViewModel.TRUNK_TRANSACTION_TRINARY_SIZE);
    System.arraycopy(branchTx.getHash().trits(), 0, childTx, TransactionViewModel.BRANCH_TRANSACTION_TRINARY_OFFSET, TransactionViewModel.BRANCH_TRANSACTION_TRINARY_SIZE);
    tx = new TransactionViewModel(childTx, Hash.calculate(SpongeFactory.Mode.CURLP81, childTx));

    trunkTx.store(tangle, snapshotManager);
    branchTx.store(tangle, snapshotManager);
    tx.store(tangle, snapshotManager);

    return tx;
  }

    @Test
    public void testTransactionPropagation() throws Exception {
        TransactionViewModel leftChildLeaf = TransactionTestUtils.createTransactionWithTrytes("CHILDTX");
        leftChildLeaf.updateSolid(true);
        leftChildLeaf.store(tangle, snapshotManager);

        TransactionViewModel rightChildLeaf = TransactionTestUtils.createTransactionWithTrytes("CHILDTWOTX");
        rightChildLeaf.updateSolid(true);
        rightChildLeaf.store(tangle, snapshotManager);

        TransactionViewModel parent = TransactionTestUtils.createTransactionWithTrunkAndBranch("PARENT",
                leftChildLeaf.getHash(), rightChildLeaf.getHash());
        parent.updateSolid(false);
        parent.store(tangle, snapshotManager);

        TransactionViewModel parentSibling = TransactionTestUtils.createTransactionWithTrytes("PARENTLEAF");
        parentSibling.updateSolid(true);
        parentSibling.store(tangle, snapshotManager);

        TransactionViewModel grandParent = TransactionTestUtils.createTransactionWithTrunkAndBranch("GRANDPARENT", parent.getHash(),
                        parentSibling.getHash());
        grandParent.updateSolid(false);
        grandParent.store(tangle, snapshotManager);

        txValidator.addSolidTransaction(leftChildLeaf.getHash());
        while (!txValidator.isNewSolidTxSetsEmpty()) {
            txValidator.propagateSolidTransactions();
        }

        parent = TransactionViewModel.fromHash(tangle, snapshotManager, parent.getHash());
        Assert.assertTrue("Parent tx was expected to be solid", parent.isSolid());
        grandParent = TransactionViewModel.fromHash(tangle, snapshotManager, grandParent.getHash());
        Assert.assertTrue("Grandparent  was expected to be solid", grandParent.isSolid());
    }

  @Test
  public void testTransactionPropagationFailure() throws Exception {
    TransactionViewModel leftChildLeaf = new TransactionViewModel(getRandomTransactionTrits(), getRandomTransactionHash());
    leftChildLeaf.updateSolid(true);
    leftChildLeaf.store(tangle, snapshotManager);

    TransactionViewModel rightChildLeaf = new TransactionViewModel(getRandomTransactionTrits(), getRandomTransactionHash());
    rightChildLeaf.updateSolid(true);
    rightChildLeaf.store(tangle, snapshotManager);

    TransactionViewModel parent = new TransactionViewModel(getRandomTransactionWithTrunkAndBranch(leftChildLeaf.getHash(),
            rightChildLeaf.getHash()), getRandomTransactionHash());
    parent.updateSolid(false);
    parent.store(tangle, snapshotManager);

    TransactionViewModel parentSibling = new TransactionViewModel(getRandomTransactionTrits(), getRandomTransactionHash());
    parentSibling.updateSolid(false);
    parentSibling.store(tangle, snapshotManager);

    TransactionViewModel grandParent = new TransactionViewModel(getRandomTransactionWithTrunkAndBranch(parent.getHash(),
            parentSibling.getHash()), getRandomTransactionHash());
    grandParent.updateSolid(false);
    grandParent.store(tangle, snapshotManager);

    txValidator.milestone = milestoneTracker;
    txValidator.addSolidTransaction(leftChildLeaf.getHash());
    while (!txValidator.isNewSolidTxSetsEmpty()) {
      txValidator.propagateSolidTransactions();
    }

    parent = TransactionViewModel.fromHash(tangle, snapshotManager, parent.getHash());
    Assert.assertTrue("Parent tx was expected to be solid", parent.isSolid());
    grandParent = TransactionViewModel.fromHash(tangle, snapshotManager, grandParent.getHash());
    Assert.assertFalse("GrandParent tx was expected to be not solid", grandParent.isSolid());
  }

  private TransactionViewModel getTxWithoutBranchAndTrunk() throws Exception {
    byte[] trits = getRandomTransactionTrits();
    TransactionViewModel tx = new TransactionViewModel(trits, Hash.calculate(SpongeFactory.Mode.CURLP81, trits));

    tx.store(tangle, snapshotManager);

    return tx;
  }
}
