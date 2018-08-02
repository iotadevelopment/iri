package com.iota.iri.service.tipselection.impl;

import com.iota.iri.MilestoneTracker;
import com.iota.iri.conf.Configuration;
import com.iota.iri.hash.SpongeFactory;
import com.iota.iri.model.Hash;
import com.iota.iri.model.IntegerIndex;
import com.iota.iri.service.snapshot.SnapshotManager;
import com.iota.iri.service.tipselection.EntryPointSelector;
import com.iota.iri.storage.Indexable;
import com.iota.iri.storage.Persistable;
import com.iota.iri.storage.Tangle;
import com.iota.iri.storage.rocksDB.RocksDBPersistenceProvider;
import com.iota.iri.utils.Pair;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EntryPointSelectorImplTest {
    private static final TemporaryFolder dbFolder = new TemporaryFolder();
    private static final TemporaryFolder logFolder = new TemporaryFolder();
    private static Tangle tangle;

    @Mock
    private MilestoneTracker milestone;

    private static SnapshotManager snapshotManager;

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

    @AfterClass
    public static void tearDown() throws Exception {
        tangle.shutdown();
        dbFolder.delete();
    }

    @Test
    public void testEntryPointWithTangleData() throws Exception {
        Hash milestoneHash = Hash.calculate(SpongeFactory.Mode.CURLP81, new byte[]{1});
        mockTangleBehavior(milestoneHash);
        mockMilestoneTrackerBehavior(0, Hash.NULL_HASH);

        EntryPointSelector entryPointSelector = new EntryPointSelectorImpl(tangle, milestone, snapshotManager, false, 0);
        Hash entryPoint = entryPointSelector.getEntryPoint(10);

        Assert.assertEquals("The entry point should be the milestone in the Tangle", milestoneHash, entryPoint);
    }

    @Test
    public void testEntryPointWithoutTangleData() throws Exception {
        mockMilestoneTrackerBehavior(0, Hash.NULL_HASH);

        EntryPointSelector entryPointSelector = new EntryPointSelectorImpl(tangle, milestone, snapshotManager, false, 0);
        Hash entryPoint = entryPointSelector.getEntryPoint(10);

        Assert.assertEquals("The entry point should be the last tracked solid milestone", Hash.NULL_HASH, entryPoint);
    }


    private void mockMilestoneTrackerBehavior(int latestSolidSubtangleMilestoneIndex, Hash latestSolidSubtangleMilestone) {
        snapshotManager.getLatestSnapshot().getMetaData().setIndex(latestSolidSubtangleMilestoneIndex);
        milestone.latestSolidSubtangleMilestone = latestSolidSubtangleMilestone;
    }

    private void mockTangleBehavior(Hash milestoneModelHash) throws Exception {
        com.iota.iri.model.Milestone milestoneModel = new com.iota.iri.model.Milestone();
        milestoneModel.index = new IntegerIndex(0);
        milestoneModel.hash = milestoneModelHash;
        Pair<Indexable, Persistable> indexMilestoneModel = new Pair<>(new IntegerIndex(0), milestoneModel);
        tangle.save(milestoneModel, new IntegerIndex(0));
    }
}