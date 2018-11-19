package com.iota.iri.service.tipselection.impl;

import com.iota.iri.conf.MainnetConfig;
import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.crypto.SpongeFactory;
import com.iota.iri.model.Hash;
import com.iota.iri.model.IntegerIndex;
import com.iota.iri.model.TransactionHash;
import com.iota.iri.service.milestone.LatestMilestoneTracker;
import com.iota.iri.service.milestone.impl.LatestMilestoneTrackerImpl;
import com.iota.iri.service.snapshot.SnapshotProvider;
import com.iota.iri.service.snapshot.impl.SnapshotProviderImpl;
import com.iota.iri.service.tipselection.EntryPointSelector;
import com.iota.iri.storage.Tangle;
import com.iota.iri.storage.rocksDB.RocksDBPersistenceProvider;

import com.iota.iri.zmq.MessageQ;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.MethodSorters;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class EntryPointSelectorImplTest {
    private static final TemporaryFolder dbFolder = new TemporaryFolder();
    private static final TemporaryFolder logFolder = new TemporaryFolder();
    private static Tangle tangle;

    private static SnapshotProvider snapshotProvider;

    private static LatestMilestoneTracker latestMilestoneTracker;

    @BeforeClass
    public static void setUp() throws Exception {
        tangle = new Tangle();
        MainnetConfig config = new MainnetConfig();
        snapshotProvider = new SnapshotProviderImpl().init(config);
        latestMilestoneTracker = new LatestMilestoneTrackerImpl().init(tangle, snapshotProvider, null, null, MessageQ.createWith(config), config);
        dbFolder.create();
        logFolder.create();
        tangle.addPersistenceProvider(new RocksDBPersistenceProvider(dbFolder.getRoot().getAbsolutePath(), logFolder
                                                                                                           .getRoot().getAbsolutePath(), 1000));
        tangle.init();
        MilestoneViewModel.clear();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        tangle.shutdown();
        snapshotProvider.shutdown();
        dbFolder.delete();
        logFolder.delete();
    }

    @Test
    public void testEntryPointAWithoutTangleData() throws Exception {
        mockMilestoneTrackerBehavior(0, Hash.NULL_HASH);

        EntryPointSelector entryPointSelector = new EntryPointSelectorImpl(tangle, snapshotProvider, latestMilestoneTracker);
        Hash entryPoint = entryPointSelector.getEntryPoint(10);

        Assert.assertEquals("The entry point should be the last tracked solid milestone", Hash.NULL_HASH, entryPoint);
    }

    @Test
    public void testEntryPointBWithTangleData() throws Exception {
        Hash milestoneHash = TransactionHash.calculate(SpongeFactory.Mode.CURLP81, new byte[]{1});
        mockTangleBehavior(milestoneHash);
        mockMilestoneTrackerBehavior(snapshotProvider.getInitialSnapshot().getIndex() + 1, Hash.NULL_HASH);

        EntryPointSelector entryPointSelector = new EntryPointSelectorImpl(tangle, snapshotProvider, latestMilestoneTracker);
        Hash entryPoint = entryPointSelector.getEntryPoint(10);

        Assert.assertEquals("The entry point should be the milestone in the Tangle", milestoneHash, entryPoint);
    }

    private void mockMilestoneTrackerBehavior(int latestSolidSubtangleMilestoneIndex, Hash latestSolidSubtangleMilestone) {
        snapshotProvider.getLatestSnapshot().setIndex(latestSolidSubtangleMilestoneIndex);
        snapshotProvider.getLatestSnapshot().setHash(latestSolidSubtangleMilestone);
        latestMilestoneTracker.setLatestMilestone(latestSolidSubtangleMilestone, latestSolidSubtangleMilestoneIndex);
    }

    private void mockTangleBehavior(Hash milestoneModelHash) throws Exception {
        com.iota.iri.model.persistables.Milestone milestoneModel = new com.iota.iri.model.persistables.Milestone();
        milestoneModel.index = new IntegerIndex(snapshotProvider.getInitialSnapshot().getIndex() + 1);
        milestoneModel.hash = milestoneModelHash;
        tangle.save(milestoneModel, milestoneModel.index);
    }
}
