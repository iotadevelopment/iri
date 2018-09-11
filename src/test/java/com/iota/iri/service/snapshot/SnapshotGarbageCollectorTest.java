package com.iota.iri.service.snapshot;

import com.iota.iri.conf.MainnetConfig;
import com.iota.iri.controllers.TipsViewModel;
import com.iota.iri.storage.Tangle;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class SnapshotGarbageCollectorTest {
    private static Tangle tangle;

    private static SnapshotManager snapshotManager;

    @BeforeClass
    public static void setUp() throws Exception {
        tangle = new Tangle();
        snapshotManager = new SnapshotManager(tangle, new TipsViewModel(), new MainnetConfig());
    }

    @Test
    public void testProcessCleanupJobs() throws SnapshotException {
        MainnetConfig mainnetConfig = new MainnetConfig();

        // add some jobs to our queue
        GarbageCollector snapshotGarbageCollector1 = new GarbageCollector(tangle, snapshotManager, new TipsViewModel()).reset();
        snapshotGarbageCollector1.addCleanupJob(mainnetConfig.getMilestoneStartIndex() + 10);
        snapshotGarbageCollector1.addCleanupJob(mainnetConfig.getMilestoneStartIndex() + 20);

        // process the jobs
        snapshotGarbageCollector1.processCleanupJobs();

        // after processing all the jobs we should have only 1 entry left indicating the success of the processing
        Assert.assertTrue("", snapshotGarbageCollector1.cleanupJobs.size() == 1);
        Assert.assertTrue(snapshotGarbageCollector1.cleanupJobs.getFirst().getStartingIndex() == mainnetConfig.getMilestoneStartIndex() + 20);
        Assert.assertTrue(snapshotGarbageCollector1.cleanupJobs.getFirst().getCurrentIndex() == mainnetConfig.getMilestoneStartIndex());

        // add some more jobs after the first processing
        snapshotGarbageCollector1.addCleanupJob(mainnetConfig.getMilestoneStartIndex() + 30);

        // process the jobs
        snapshotGarbageCollector1.processCleanupJobs();

        // after processing all the jobs we should have only 1 entry left indicating the success of the processing
        Assert.assertTrue("", snapshotGarbageCollector1.cleanupJobs.size() == 1);
        Assert.assertTrue(snapshotGarbageCollector1.cleanupJobs.getFirst().getStartingIndex() == mainnetConfig.getMilestoneStartIndex() + 30);
        Assert.assertTrue(snapshotGarbageCollector1.cleanupJobs.getFirst().getCurrentIndex() == mainnetConfig.getMilestoneStartIndex());
    }

    @Test
    public void testStatePersistence() throws SnapshotException {
        MainnetConfig mainnetConfig = new MainnetConfig();

        // add a job to our queue
        GarbageCollector snapshotGarbageCollector1 = new GarbageCollector(tangle, snapshotManager, new TipsViewModel()).reset();
        snapshotGarbageCollector1.addCleanupJob(12);

        // check if the restored cleanupJobs are the same as the saved ones
        GarbageCollector snapshotGarbageCollector2 = new GarbageCollector(tangle, snapshotManager, new TipsViewModel());
        Assert.assertTrue(snapshotGarbageCollector2.cleanupJobs.size() == 1);
        Assert.assertTrue(snapshotGarbageCollector2.cleanupJobs.getLast().getStartingIndex() == 12);
        Assert.assertTrue(snapshotGarbageCollector2.cleanupJobs.getLast().getCurrentIndex() == 12);

        // add another job to our queue
        snapshotGarbageCollector1.addCleanupJob(17);

        // check if the restored cleanupJobs are the same as the saved ones
        GarbageCollector snapshotGarbageCollector3 = new GarbageCollector(tangle, snapshotManager, new TipsViewModel());
        Assert.assertTrue(snapshotGarbageCollector3.cleanupJobs.size() == 1);
        Assert.assertTrue(snapshotGarbageCollector3.cleanupJobs.getLast().getStartingIndex() == 17);
        Assert.assertTrue(snapshotGarbageCollector3.cleanupJobs.getLast().getCurrentIndex() == 17);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        new GarbageCollector(tangle, snapshotManager, new TipsViewModel()).reset();
        tangle.shutdown();
    }
}
