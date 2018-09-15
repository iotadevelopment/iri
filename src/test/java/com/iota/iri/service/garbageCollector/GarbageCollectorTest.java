package com.iota.iri.service.garbageCollector;

import com.iota.iri.conf.MainnetConfig;
import com.iota.iri.controllers.TipsViewModel;
import com.iota.iri.service.garbageCollector.GarbageCollector;
import com.iota.iri.service.snapshot.SnapshotManager;
import com.iota.iri.storage.Tangle;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class GarbageCollectorTest {
    private static Tangle tangle;

    private static SnapshotManager snapshotManager;

    @BeforeClass
    public static void setUp() throws Exception {
        tangle = new Tangle();
        snapshotManager = new SnapshotManager(tangle, new TipsViewModel(), new MainnetConfig());
    }

    @Test
    public void testProcessCleanupJobs() throws GarbageCollectorException {
        MainnetConfig mainnetConfig = new MainnetConfig();

        // add some jobs to our queue
        GarbageCollector snapshotGarbageCollector1 = new GarbageCollector(tangle, snapshotManager, new TipsViewModel());
        snapshotGarbageCollector1.reset();
        snapshotGarbageCollector1.addMilestoneCleanupJob(mainnetConfig.getMilestoneStartIndex() + 10);
        snapshotGarbageCollector1.addMilestoneCleanupJob(mainnetConfig.getMilestoneStartIndex() + 20);

        // process the jobs
        snapshotGarbageCollector1.processCleanupJobs();

        // after processing all the jobs we should have only 1 entry left indicating the success of the processing
        Assert.assertTrue("", snapshotGarbageCollector1.milestoneCleanupJobs.size() == 1);
        Assert.assertTrue(snapshotGarbageCollector1.milestoneCleanupJobs.getFirst().getStartingIndex() == mainnetConfig.getMilestoneStartIndex() + 20);
        Assert.assertTrue(snapshotGarbageCollector1.milestoneCleanupJobs.getFirst().getCurrentIndex() == mainnetConfig.getMilestoneStartIndex());

        // add some more jobs after the first processing
        snapshotGarbageCollector1.addMilestoneCleanupJob(mainnetConfig.getMilestoneStartIndex() + 30);

        // process the jobs
        snapshotGarbageCollector1.processCleanupJobs();

        // after processing all the jobs we should have only 1 entry left indicating the success of the processing
        Assert.assertTrue("", snapshotGarbageCollector1.milestoneCleanupJobs.size() == 1);
        Assert.assertTrue(snapshotGarbageCollector1.milestoneCleanupJobs.getFirst().getStartingIndex() == mainnetConfig.getMilestoneStartIndex() + 30);
        Assert.assertTrue(snapshotGarbageCollector1.milestoneCleanupJobs.getFirst().getCurrentIndex() == mainnetConfig.getMilestoneStartIndex());
    }

    @Test
    public void testStatePersistence() throws GarbageCollectorException {
        MainnetConfig mainnetConfig = new MainnetConfig();

        // add a job to our queue
        GarbageCollector snapshotGarbageCollector1 = new GarbageCollector(tangle, snapshotManager, new TipsViewModel());
        snapshotGarbageCollector1.reset();
        snapshotGarbageCollector1.addMilestoneCleanupJob(12);

        // check if the restored milestoneCleanupJobs are the same as the saved ones
        GarbageCollector snapshotGarbageCollector2 = new GarbageCollector(tangle, snapshotManager, new TipsViewModel());
        Assert.assertTrue(snapshotGarbageCollector2.milestoneCleanupJobs.size() == 1);
        Assert.assertTrue(snapshotGarbageCollector2.milestoneCleanupJobs.getLast().getStartingIndex() == 12);
        Assert.assertTrue(snapshotGarbageCollector2.milestoneCleanupJobs.getLast().getCurrentIndex() == 12);

        // add another job to our queue
        snapshotGarbageCollector1.addMilestoneCleanupJob(17);

        // check if the restored milestoneCleanupJobs are the same as the saved ones
        GarbageCollector snapshotGarbageCollector3 = new GarbageCollector(tangle, snapshotManager, new TipsViewModel());
        Assert.assertTrue(snapshotGarbageCollector3.milestoneCleanupJobs.size() == 1);
        Assert.assertTrue(snapshotGarbageCollector3.milestoneCleanupJobs.getLast().getStartingIndex() == 17);
        Assert.assertTrue(snapshotGarbageCollector3.milestoneCleanupJobs.getLast().getCurrentIndex() == 17);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        new GarbageCollector(tangle, snapshotManager, new TipsViewModel()).reset();
        tangle.shutdown();
    }
}
