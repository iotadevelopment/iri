package com.iota.iri.service.garbageCollector;

import com.iota.iri.controllers.TipsViewModel;
import com.iota.iri.model.Hash;
import com.iota.iri.service.snapshot.SnapshotManager;
import com.iota.iri.storage.Tangle;
import com.iota.iri.utils.dag.DAGHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

/**
 * This class represents the manager for the cleanup jobs that are issued by the {@link SnapshotManager} in connection
 * with local snapshots and eventually other parts of the code.
 *
 * It plans, manages and executes the cleanup jobs asynchronously in separate thread so cleaning up does not affect the
 * performance of the node itself.
 */
public class GarbageCollector {
    /**
     * The interval in milliseconds that the garbage collector will check if new cleanup tasks are available.
     */
    protected static int GARBAGE_COLLECTOR_RESCAN_INTERVAL = 10000;

    /**
     * Logger for this class allowing us to dump debug and status messages.
     */
    protected static final Logger log = LoggerFactory.getLogger(GarbageCollector.class);

    /**
     * Boolean flag that indicates if the node is being shutdown.
     */
    protected boolean shuttingDown = false;

    /**
     * Holds a reference to the tangle instance which acts as an interface to the used database.
     */
    protected Tangle tangle;

    /**
     * Holds a reference to the {@link SnapshotManager} that this garbage collector belongs to.
     */
    protected SnapshotManager snapshotManager;

    /**
     * Holds a reference to the {@link TipsViewModel} which is necessary for removing tips that were pruned.
     */
    protected TipsViewModel tipsViewModel;

    /**
     * List of cleanup jobs that shall get processed by the garbage collector.
     */
    protected LinkedList<GarbageCollectorJob> milestoneCleanupJobs;

    /**
     * List of cleanup jobs that shall get processed by the garbage collector.
     */
    protected LinkedList<GarbageCollectorJob> solidEntryPointCleanupJobs;

    /**
     * DAGHelper instance that is used to traverse the graph.
     */
    protected DAGHelper dagHelper;

    /**
     * The constructor of this class stores the passed in parameters for future use and restores the previous state of
     * the garbage collector if there is a valid one (to continue with cleaning up after IRI restarts).
     */
    public GarbageCollector(Tangle tangle, SnapshotManager snapshotManager, TipsViewModel tipsViewModel) {
        this.tangle = tangle;
        this.snapshotManager = snapshotManager;
        this.tipsViewModel = tipsViewModel;
        this.dagHelper = DAGHelper.get(tangle);

        restoreCleanupJobs();
    }

    /**
     * This method allows us to add a new milestone cleanup job.
     *
     * The job that is created through this method will take care of removing all the unnecessary database entries
     * before (and including) the given milestoneIndex.
     *
     * It first adds the job to the queue, then persists it and checks if jobs can be consolidated.
     *
     * @param milestoneIndex starting point of the cleanup operation
     * @throws GarbageCollectorException if something goes wrong while persisting the job queue
     */
    public void addMilestoneCleanupJob(int milestoneIndex) throws GarbageCollectorException {
        milestoneCleanupJobs.addLast(new GarbageCollectorJob(this, milestoneIndex, milestoneIndex));

        persistChanges();
        consolidateCleanupJobs();
    }

    public void addSolidEntryPointCleanupJob(Hash solidEntryPoint) {
        ;
    }

    /**
     * This method spawns the thread that is taking care of processing the cleanup jobs in the background.
     *
     * It repeatedly calls {@link #processCleanupJobs()} while the GarbageCollector was not shutdown.
     *
     * @return
     */
    public void start() {
        (new Thread(() -> {
            log.info("Snapshot Garbage Collector started ...");

            while(!shuttingDown) {
                try {
                    processCleanupJobs();

                    try {
                        Thread.sleep(GARBAGE_COLLECTOR_RESCAN_INTERVAL);
                    } catch(InterruptedException e) { /* do nothing */ }
                } catch(GarbageCollectorException e) {
                    log.error("error while processing the garbage collector jobs", e);
                }
            }
        }, "Snapshot Garbage Collector")).start();
    }

    /**
     * Shuts down the background job by setting the corresponding shutdown flag.
     */
    public void shutdown() {
        shuttingDown = true;
    }

    /**
     * This method resets the state of the GarbageCollector.
     *
     * It prunes the job queue and deletes the state file afterwards. It can for example be used to cleanup the
     * remaining files after processing the unit tests.
     */
    public void reset() {
        milestoneCleanupJobs = new LinkedList<>();

        getStateFile().delete();
    }

    /**
     * This method contains the logic for scheduling the jobs and executing them.
     *
     * While the GarbageCollector is not shutting down and there are jobs that need to be processed, it retrieves the
     * first job and processes it. Afterwards it checks if there is a second job which also has to be processed. If both
     * jobs are "done", it consolidates their progress into a single job, that is consecutively used by the following
     * jobs for determining their "target milestone".
     *
     * @throws GarbageCollectorException
     */
    protected void processCleanupJobs() throws GarbageCollectorException {
        // repeat until all jobs are processed
        while(!shuttingDown && milestoneCleanupJobs.size() >= 1) {
            GarbageCollectorJob firstJob = milestoneCleanupJobs.getFirst();
            firstJob.process(snapshotManager.getConfiguration().getMilestoneStartIndex());

            if(milestoneCleanupJobs.size() >= 2) {
                milestoneCleanupJobs.removeFirst();
                GarbageCollectorJob secondJob = milestoneCleanupJobs.getFirst();
                milestoneCleanupJobs.addFirst(firstJob);

                secondJob.process(firstJob.getStartingIndex());

                // if both jobs are done we can consolidate them to one
                consolidateCleanupJobs();
            } else {
                break;
            }
        }
    }

    /**
     * This method consolidates the cleanup jobs by merging two or more jobs together if they are either both done or
     * pending.
     *
     * It is used to clean up the milestoneCleanupJobs queue and the corresponding garbage collector file, so it always has a
     * size of less than 4 jobs.
     *
     * Since the jobs are getting processed from the beginning of the list, we first check if the first two jobs are
     * "done" and merge them into a single one that reflects the "done" status of both jobs. Consecutively we check if
     * there are two or more pending jobs at the end that can also be consolidated.
     *
     * It is important to note that the jobs always clean from their startingPosition to the startingPosition of the
     * previous job (or the {@code milestoneStartIndex} of the last global snapshot if there is no previous one) without
     * any gaps in between which is required to be able to merge them.
     *
     * @throws GarbageCollectorException if an error occurs while persisting the state
     */
    protected void consolidateCleanupJobs() throws GarbageCollectorException {
        // if we have at least 2 jobs -> check if we can consolidate them at the beginning
        if(milestoneCleanupJobs.size() >= 2) {
            // retrieve the first two jobs
            GarbageCollectorJob job1 = milestoneCleanupJobs.removeFirst();
            GarbageCollectorJob job2 = milestoneCleanupJobs.removeFirst();

            // if both first job are done -> consolidate them and persists the changes
            if(job1.getCurrentIndex() == snapshotManager.getConfiguration().getMilestoneStartIndex() && job2.getCurrentIndex() == job1.getStartingIndex()) {
                milestoneCleanupJobs.addFirst(new GarbageCollectorJob(this, job2.getStartingIndex(), job1.getCurrentIndex()));

                persistChanges();
            }

            // otherwise just add them back to the queue
            else {
                milestoneCleanupJobs.addFirst(job2);
                milestoneCleanupJobs.addFirst(job1);
            }
        }

        // if we have at least 2 jobs -> check if we can consolidate them at the end
        boolean cleanupSuccessfull = true;
        while(milestoneCleanupJobs.size() >= 2 && cleanupSuccessfull) {
            // retrieve the last two jobs
            GarbageCollectorJob job1 = milestoneCleanupJobs.removeLast();
            GarbageCollectorJob job2 = milestoneCleanupJobs.removeLast();

            // if both jobs are pending -> consolidate them and persists the changes
            if(job1.getCurrentIndex() == job1.getStartingIndex() && job2.getCurrentIndex() == job2.getStartingIndex()) {
                milestoneCleanupJobs.addLast(new GarbageCollectorJob(this, job1.getStartingIndex(), job1.getCurrentIndex()));

                persistChanges();
            }

            // otherwise just add them back to the queue
            else {
                milestoneCleanupJobs.addLast(job2);
                milestoneCleanupJobs.addLast(job1);

                cleanupSuccessfull = false;
            }
        }
    }

    /**
     * This method persists the changes of the garbage collector so IRI can continue cleaning up upon restarts.
     *
     * Since cleaning up the old database entries can take a long time, we need to make sure that it is possible to
     * continue where we stopped. This method therefore writes the state of the queued jobs into a file that is read
     * upon re-initialization of the GarbageCollector.
     *
     * @throws GarbageCollectorException if something goes wrong while writing the state file
     */
    protected void persistChanges() throws GarbageCollectorException {
        try {
            Files.write(
                Paths.get(getStateFile().getAbsolutePath()),
                () -> Stream.concat(
                    Stream.of(
                        String.valueOf(milestoneCleanupJobs.size()),
                        String.valueOf(6)
                    ),
                    milestoneCleanupJobs.stream().<CharSequence>map(entry -> Integer.toString(entry.getStartingIndex()) + ";" + Integer.toString(entry.getCurrentIndex()))
                ).iterator()
            );
        } catch(IOException e) {
            throw new GarbageCollectorException("could not persists garbage collector state", e);
        }
    }

    /**
     * This method tries to restore the previous state of the Garbage Collector by reading the state file that get's
     * persisted whenever we modify the queue.
     *
     * It is used to restore the state of the garbage collector between IRI restarts and speed up the pruning
     * operations. If it fails to restore the state it just continues with an empty state which doesn't cause any
     * problems with future jobs other than requiring them to perform unnecessary steps and therefore slowing them down
     * a bit.
     */
    protected void restoreCleanupJobs() {
        milestoneCleanupJobs = new LinkedList<>();

        try {
            // create a reader for our file
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(
                    new BufferedInputStream(
                        new FileInputStream(getStateFile())
                    )
                )
            );

            // read the saved cleanup states back into our queue
            String line;
            while((line = reader.readLine()) != null) {
                String[] parts = line.split(";", 2);
                if(parts.length >= 2) {
                    milestoneCleanupJobs.addLast(new GarbageCollectorJob(this, Integer.valueOf(parts[0]), Integer.valueOf(parts[1])));
                }
            }

            reader.close();
        } catch(Exception e) { /* do nothing */ }
    }

    /**
     * This method returns a file handle to the local snapshots garbage collector file.
     *
     * It constructs the path of the file by appending the corresponding file extension to the
     * {@link com.iota.iri.conf.BaseIotaConfig#localSnapshotsBasePath} config variable. If the path is relative, it
     * places the file relative to the current working directory, which is usually the location of the iri.jar.
     *
     * @return File handle to the local snapshots garbage collector file.
     */
    protected File getStateFile() {
        return new File(snapshotManager.getConfiguration().getLocalSnapshotsBasePath() + ".snapshot.gc");
    }
}