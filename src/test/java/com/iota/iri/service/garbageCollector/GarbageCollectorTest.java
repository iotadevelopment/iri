package com.iota.iri.service.garbageCollector;

import com.iota.iri.conf.MainnetConfig;
import com.iota.iri.controllers.TipsViewModel;
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
        snapshotGarbageCollector1.addJob(new MilestonePrunerJob(mainnetConfig.getMilestoneStartIndex() + 10));
        snapshotGarbageCollector1.addJob(new MilestonePrunerJob(mainnetConfig.getMilestoneStartIndex() + 20));

        // process the jobs
        snapshotGarbageCollector1.processCleanupJobs();

        // after processing all the jobs we should have only 1 entry left indicating the success of the processing
        Assert.assertTrue("", snapshotGarbageCollector1.getJobQueue(MilestonePrunerJob.class).size() == 1);
        Assert.assertTrue(((MilestonePrunerJob) snapshotGarbageCollector1.getJobQueue(MilestonePrunerJob.class).getFirst()).getStartingIndex() == mainnetConfig.getMilestoneStartIndex() + 20);
        Assert.assertTrue(((MilestonePrunerJob) snapshotGarbageCollector1.getJobQueue(MilestonePrunerJob.class).getFirst()).getCurrentIndex() == mainnetConfig.getMilestoneStartIndex());

        // add some more jobs after the first processing
        snapshotGarbageCollector1.addJob(new MilestonePrunerJob(mainnetConfig.getMilestoneStartIndex() + 30));

        // process the jobs
        snapshotGarbageCollector1.processCleanupJobs();

        // after processing all the jobs we should have only 1 entry left indicating the success of the processing
        Assert.assertTrue("", snapshotGarbageCollector1.getJobQueue(MilestonePrunerJob.class).size() == 1);
        Assert.assertTrue(((MilestonePrunerJob) snapshotGarbageCollector1.getJobQueue(MilestonePrunerJob.class).getFirst()).getStartingIndex() == mainnetConfig.getMilestoneStartIndex() + 30);
        Assert.assertTrue(((MilestonePrunerJob) snapshotGarbageCollector1.getJobQueue(MilestonePrunerJob.class).getFirst()).getCurrentIndex() == mainnetConfig.getMilestoneStartIndex());
    }

    @Test
    public void testStatePersistence() throws GarbageCollectorException {
        MainnetConfig mainnetConfig = new MainnetConfig();

        // add a job to our queue
        GarbageCollector snapshotGarbageCollector1 = new GarbageCollector(tangle, snapshotManager, new TipsViewModel());
        snapshotGarbageCollector1.reset();
        snapshotGarbageCollector1.addJob(new MilestonePrunerJob(12));

        // check if the restored milestoneCleanupJobs are the same as the saved ones
        GarbageCollector snapshotGarbageCollector2 = new GarbageCollector(tangle, snapshotManager, new TipsViewModel());
        Assert.assertTrue(snapshotGarbageCollector2.getJobQueue(MilestonePrunerJob.class).size() == 1);
        Assert.assertTrue(((MilestonePrunerJob) snapshotGarbageCollector2.getJobQueue(MilestonePrunerJob.class).getLast()).getStartingIndex() == 12);
        Assert.assertTrue(((MilestonePrunerJob) snapshotGarbageCollector2.getJobQueue(MilestonePrunerJob.class).getLast()).getTargetIndex() == mainnetConfig.getMilestoneStartIndex());
        Assert.assertTrue(((MilestonePrunerJob) snapshotGarbageCollector2.getJobQueue(MilestonePrunerJob.class).getLast()).getCurrentIndex() == 12);

        // add another job to our queue
        snapshotGarbageCollector1.addJob(new MilestonePrunerJob(17));

        // check if the restored milestoneCleanupJobs are the same as the saved ones
        GarbageCollector snapshotGarbageCollector3 = new GarbageCollector(tangle, snapshotManager, new TipsViewModel());
        Assert.assertTrue(snapshotGarbageCollector3.getJobQueue(MilestonePrunerJob.class).size() == 1);
        Assert.assertTrue(((MilestonePrunerJob) snapshotGarbageCollector3.getJobQueue(MilestonePrunerJob.class).getLast()).getStartingIndex() == 17);
        Assert.assertTrue(((MilestonePrunerJob) snapshotGarbageCollector3.getJobQueue(MilestonePrunerJob.class).getLast()).getTargetIndex() == mainnetConfig.getMilestoneStartIndex());
        Assert.assertTrue(((MilestonePrunerJob) snapshotGarbageCollector3.getJobQueue(MilestonePrunerJob.class).getLast()).getCurrentIndex() == 17);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        new GarbageCollector(tangle, snapshotManager, new TipsViewModel()).reset();
        tangle.shutdown();
    }
}
