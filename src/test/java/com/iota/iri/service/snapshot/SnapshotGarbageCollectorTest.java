package com.iota.iri.service.snapshot;

import com.iota.iri.conf.MainnetConfig;
import com.iota.iri.model.Hash;
import com.iota.iri.storage.Tangle;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class SnapshotGarbageCollectorTest {
    private static Tangle tangle;

    private static SnapshotManager snapshotManager;

    @BeforeClass
    public static void setUp() throws Exception {
        tangle = new Tangle();
        snapshotManager = new SnapshotManager(tangle, new MainnetConfig());
    }

    @Test
    public void testProcessCleanupJobs() throws SnapshotException {
        MainnetConfig mainnetConfig = new MainnetConfig();

        // add some jobs to our queue
        SnapshotGarbageCollector snapshotGarbageCollector1 = new SnapshotGarbageCollector(tangle, snapshotManager).reset();
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

        // add some jobs to our queue
        SnapshotGarbageCollector snapshotGarbageCollector1 = new SnapshotGarbageCollector(tangle, snapshotManager).reset();
        snapshotGarbageCollector1.addCleanupJob(12);
        snapshotGarbageCollector1.addCleanupJob(17);

        // check if the restored cleanupJobs are the same as the saved ones
        SnapshotGarbageCollector snapshotGarbageCollector2 = new SnapshotGarbageCollector(tangle, snapshotManager);
        Assert.assertTrue(snapshotGarbageCollector2.cleanupJobs.size() == 2);
        Assert.assertTrue(snapshotGarbageCollector2.cleanupJobs.getFirst().getStartingIndex() == 12);
        Assert.assertTrue(snapshotGarbageCollector2.cleanupJobs.getFirst().getCurrentIndex() == 12);
        Assert.assertTrue(snapshotGarbageCollector2.cleanupJobs.getLast().getStartingIndex() == 17);
        Assert.assertTrue(snapshotGarbageCollector2.cleanupJobs.getLast().getCurrentIndex() == 17);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        new SnapshotGarbageCollector(tangle, snapshotManager).reset();
        tangle.shutdown();
    }
}
