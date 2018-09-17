package com.iota.iri.service.garbageCollector;

import com.iota.iri.controllers.TipsViewModel;
import com.iota.iri.service.snapshot.SnapshotManager;
import com.iota.iri.storage.Tangle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * This class represents the manager for the cleanup jobs that are issued by the {@link SnapshotManager} in connection
 * with local snapshots and eventually other parts of the code.
 *
 * It plans, manages and executes the cleanup jobs asynchronously in a separate thread so cleaning up does not affect
 * the performance of the other tasks of the node.
 */
public class GarbageCollector {
    /**
     * Boolean flag that indicates if the node is being shutdown.
     *
     * Note: It is not just used by the GarbageCollector itself but also by the jobs and therefore protected.
     */
    protected boolean shuttingDown = false;

    /**
     * Holds a reference to the tangle instance which acts as an interface to the used database.
     *
     * Note: It is not just used by the GarbageCollector itself but also by the jobs and therefore protected.
     */
    protected Tangle tangle;

    /**
     * Holds a reference to the {@link SnapshotManager} that this garbage collector belongs to.
     *
     * Note: It is not just used by the GarbageCollector itself but also by the jobs and therefore protected.
     */
    protected SnapshotManager snapshotManager;

    /**
     * Holds a reference to the {@link TipsViewModel} which is necessary for removing tips that were pruned.
     *
     * Note: It is not just used by the GarbageCollector itself but also by the jobs and therefore protected.
     */
    protected TipsViewModel tipsViewModel;

    /**
     * The interval in milliseconds that the garbage collector will check if new cleanup tasks are available.
     */
    private static int GARBAGE_COLLECTOR_RESCAN_INTERVAL = 10000;

    /**
     * Logger for this class allowing us to dump debug and status messages.
     */
    private static final Logger log = LoggerFactory.getLogger(GarbageCollector.class);

    /**
     * A map of {@link JobParser}s allowing us to determine how to parse the jobs from the garbage collector state file,
     * based on their type.
     */
    private HashMap<String, JobParser> jobParsers = new HashMap<>();

    /**
     * A map of {@link QueueProcessor}s allowing us to process queues based on the type of the job.
     */
    private HashMap<Class<? extends GarbageCollectorJob>, QueueProcessor> queueProcessors = new HashMap<>();

    /**
     * A map of {@link QueueConsolidator}s allowing us to consolidate a queue to consume less "space".
     */
    private HashMap<Class<? extends GarbageCollectorJob>, QueueConsolidator> queueConsolidators = new HashMap<>();

    /**
     * List of cleanup jobs that shall get processed by the garbage collector (grouped by their class).
     */
    private HashMap<Class<? extends GarbageCollectorJob>, ArrayDeque<GarbageCollectorJob>> garbageCollectorJobs = new HashMap<>();

    /**
     * The constructor of this class stores the passed in parameters for future use and restores the previous state of
     * the garbage collector if there is a valid one (to continue with cleaning up after IRI restarts).
     *
     * Before restoring the garbage collector state it registers the available job types, so we know how to parse and
     * process the found jobs.
     */
    public GarbageCollector(Tangle tangle, SnapshotManager snapshotManager, TipsViewModel tipsViewModel) {
        this.tangle = tangle;
        this.snapshotManager = snapshotManager;
        this.tipsViewModel = tipsViewModel;

        MilestonePrunerJob.registerInGarbageCollector(this);
        OrphanedSubtanglePrunerJob.registerInGarbageCollector(this);

        try {
            restoreCleanupJobs();
        } catch(GarbageCollectorException e) {
            log.error("could not restore garbage collector jobs", e);
        }
    }

    /**
     * This method allows to register a parser for a given job type.
     *
     * When we serialize the pending jobs to save the current state of the garbage collector, we also dump their class
     * name, which allows us to generically parse their serialized representation using the registered parser function
     * back into the corresponding job.
     *
     * This method automatically gets called for the builtin jobs in the constructor of the GarbageCollector.
     *
     * @param jobClass class of the job that the GarbageCollector shall be able to handle
     * @param jobParser parser function for the serialized version of jobs of the given type
     */
    protected void registerParser(Class<?> jobClass, JobParser jobParser) {
        this.jobParsers.put(jobClass.getCanonicalName(), jobParser);
    }

    /**
     * This method allows us to register a {@link QueueProcessor} for the given job type.
     *
     * Since different kinds of jobs get processed in a different way, we are able to generically process them based on
     * their type after having registered a processor for them.
     *
     * This method automatically gets called for the builtin jobs in the constructor of the GarbageCollector.
     *
     * @param jobClass class of the job that the GarbageCollector shall be able to handle
     * @param queueProcessor function that takes care of processing the queue for this particular type
     */
    protected void registerQueueProcessor(Class<? extends GarbageCollectorJob> jobClass, QueueProcessor queueProcessor) {
        this.queueProcessors.put(jobClass, queueProcessor);
    }

    /**
     * This method allows us to register a {@link QueueConsolidator} for the given job type.
     *
     * Some jobs can be consolidated to consume less space in the queue by grouping them together or skipping them
     * completely. While the consolidation of multiple jobs into fewer ones is optional and only required for certain
     * types of jobs, this method allows us to generically handle this use case by registering a handler for the job
     * class that supports this feature.
     *
     * This method automatically gets called for the builtin jobs in the constructor of the GarbageCollector.
     *
     * @param jobClass class of the job that the GarbageCollector shall be able to handle
     * @param queueConsolidator
     */
    protected void registerQueueConsolidator(Class<? extends GarbageCollectorJob> jobClass, QueueConsolidator queueConsolidator) {
        this.queueConsolidators.put(jobClass, queueConsolidator);
    }

    /**
     * This method adds a job to the GarbageCollector, that will get processed on the next run of the job processing.
     *
     * @param job
     * @throws GarbageCollectorException
     */
    public void addJob(GarbageCollectorJob job) throws GarbageCollectorException {
        job.registerGarbageCollector(this);

        ArrayDeque<GarbageCollectorJob> jobQueue = getJobQueue(job.getClass());
        jobQueue.addLast(job);

        // consolidating the queue is optional
        QueueConsolidator queueConsolidator = queueConsolidators.get(job.getClass());
        if(queueConsolidator != null) {
            queueConsolidator.consolidateQueue(this, jobQueue);
        }

        persistChanges();
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
        garbageCollectorJobs = new HashMap<>();

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
        for(Map.Entry<Class<? extends GarbageCollectorJob>, ArrayDeque<GarbageCollectorJob>> entry : garbageCollectorJobs.entrySet()) {
            if(shuttingDown) {
                return;
            }

            QueueProcessor queueProcessor = queueProcessors.get(entry.getKey());
            if(queueProcessor == null) {
                throw new GarbageCollectorException("could not determine a queue processor for cleanup job of type " + entry.getKey().getCanonicalName());
            }

            queueProcessor.processQueue(this, entry.getValue());
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
                () -> garbageCollectorJobs.entrySet().stream().flatMap(
                    entry -> entry.getValue().stream()
                ).<CharSequence>map(
                    entry -> entry.getClass().getCanonicalName() + ";" + entry.toString()
                ).iterator()
            );
        } catch(IOException e) {
            throw new GarbageCollectorException("could not persists garbage collector state", e);
        }
    }

    protected ArrayDeque<GarbageCollectorJob> getJobQueue(Class<? extends GarbageCollectorJob> jobClass) {
        if (garbageCollectorJobs.get(jobClass) == null) {
            synchronized(this) {
                if (garbageCollectorJobs.get(jobClass) == null) {
                    garbageCollectorJobs.put(jobClass, new ArrayDeque<>());
                }
            }
        }

        return garbageCollectorJobs.get(jobClass);
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
    private void restoreCleanupJobs() throws GarbageCollectorException {
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
                    JobParser jobParser = this.jobParsers.get(parts[0]);
                    if(jobParser == null) {
                        throw new GarbageCollectorException("could not determine a parser for cleanup job of type " + parts[0]);
                    }

                    addJob(jobParser.parse(parts[1]));
                }
            }

            reader.close();
        }
        catch(FileNotFoundException e) { /* do nothing */ }
        catch(IOException e) { /* do nothing */ }
        catch(Exception e) {
            log.error("could not load local snapshot file", e);
        }
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
    private File getStateFile() {
        return new File(snapshotManager.getConfiguration().getLocalSnapshotsBasePath() + ".snapshot.gc");
    }
}
