package com.iota.iri.service.tipselection.impl;

import com.iota.iri.MilestoneTracker;
import com.iota.iri.controllers.MilestoneViewModel;
import com.iota.iri.conf.MainnetConfig;
import com.iota.iri.controllers.TipsViewModel;
import com.iota.iri.hash.SpongeFactory;
import com.iota.iri.model.Hash;
import com.iota.iri.model.IntegerIndex;
import com.iota.iri.service.snapshot.SnapshotManager;
import com.iota.iri.service.tipselection.EntryPointSelector;
import com.iota.iri.storage.Tangle;
import com.iota.iri.storage.rocksDB.RocksDBPersistenceProvider;

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

    @Mock
    private MilestoneTracker milestone;

    private static SnapshotManager snapshotManager;

    @BeforeClass
    public static void setUp() throws Exception {
        tangle = new Tangle();
        snapshotManager = new SnapshotManager(tangle, new TipsViewModel(), new MainnetConfig());
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
        dbFolder.delete();
        logFolder.delete();
    }

    @Test
    public void testEntryPointAWithoutTangleData() throws Exception {
        mockMilestoneTrackerBehavior(0, Hash.NULL_HASH);

        EntryPointSelector entryPointSelector = new EntryPointSelectorImpl(tangle, snapshotManager, milestone, new MainnetConfig());
        Hash entryPoint = entryPointSelector.getEntryPoint(10);

        Assert.assertEquals("The entry point should be the last tracked solid milestone", Hash.NULL_HASH, entryPoint);
    }

    @Test
    public void testEntryPointBWithTangleData() throws Exception {
        Hash milestoneHash = Hash.calculate(SpongeFactory.Mode.CURLP81, new byte[]{1});
        mockTangleBehavior(milestoneHash);
        mockMilestoneTrackerBehavior(0, Hash.NULL_HASH);

        EntryPointSelector entryPointSelector = new EntryPointSelectorImpl(tangle, snapshotManager, milestone, new MainnetConfig());
        Hash entryPoint = entryPointSelector.getEntryPoint(10);

        Assert.assertEquals("The entry point should be the milestone in the Tangle", milestoneHash, entryPoint);
    }

    private void mockMilestoneTrackerBehavior(int latestSolidSubtangleMilestoneIndex, Hash latestSolidSubtangleMilestone) {
        snapshotManager.getLatestSnapshot().getMetaData().setIndex(latestSolidSubtangleMilestoneIndex);
        snapshotManager.getLatestSnapshot().getMetaData().setHash(latestSolidSubtangleMilestone);
    }

    private void mockTangleBehavior(Hash milestoneModelHash) throws Exception {
        com.iota.iri.model.Milestone milestoneModel = new com.iota.iri.model.Milestone();
        milestoneModel.index = new IntegerIndex(0);
        milestoneModel.hash = milestoneModelHash;
        tangle.save(milestoneModel, new IntegerIndex(0));
    }
}