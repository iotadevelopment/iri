package com.iota.iri.service.garbageCollector;

import com.iota.iri.controllers.TipsViewModel;
import com.iota.iri.service.snapshot.SnapshotManager;
import com.iota.iri.storage.Tangle;
import com.iota.iri.utils.dag.DAGHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

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
    private static int GARBAGE_COLLECTOR_RESCAN_INTERVAL = 10000;

    /**
     * Logger for this class allowing us to dump debug and status messages.
     */
    private static final Logger log = LoggerFactory.getLogger(GarbageCollector.class);

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
     * List of cleanup jobs that shall get processed by the garbage collector (grouped by their class).
     */
    private HashMap<Class<? extends GarbageCollectorJob>, ArrayDeque<GarbageCollectorJob>> garbageCollectorJobs = new HashMap<>();

    /**
     * The constructor of this class stores the passed in parameters for future use and restores the previous state of
     * the garbage collector if there is a valid one (to continue with cleaning up after IRI restarts).
     */
    public GarbageCollector(Tangle tangle, SnapshotManager snapshotManager, TipsViewModel tipsViewModel) {
        this.tangle = tangle;
        this.snapshotManager = snapshotManager;
        this.tipsViewModel = tipsViewModel;

        try {
            restoreCleanupJobs();
        } catch(GarbageCollectorException e) {
            log.error("could not restore garbage collector jobs", e);
        }
    }

    public void addJob(GarbageCollectorJob job) throws GarbageCollectorException {
        job.registerGarbageCollector(this);

        ArrayDeque<GarbageCollectorJob> jobQueue = getJobQueue(job.getClass());
        jobQueue.addLast(job);

        try {
            Method consolidateQueueMethod = job.getClass().getMethod("consolidateQueue", GarbageCollector.class, ArrayDeque.class);
            consolidateQueueMethod.invoke(null, this, jobQueue);
        }
        catch(IllegalAccessException e)    { /* will never happen (enforced through jobClass) */ }
        catch(NoSuchMethodException e)     { /* will never happen (enforced through jobClass) */ }
        catch(InvocationTargetException e) {
            throw new GarbageCollectorException("could not consolidate the queue", e.getCause());
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

            Method processQueueMethod = null;
            try {
                processQueueMethod = entry.getKey().getMethod("processQueue", GarbageCollector.class, ArrayDeque.class);
                GarbageCollectorJob job = (GarbageCollectorJob) processQueueMethod.invoke(null, this, entry.getValue());
            }
            catch(IllegalAccessException e)    { /* will never happen (enforced through jobClass) */ }
            catch(NoSuchMethodException e)     { /* will never happen (enforced through jobClass) */ }
            catch(InvocationTargetException e) {
                throw new GarbageCollectorException("could not process the queue", e.getCause());
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
                    Class<GarbageCollectorJob> jobClass = (Class<GarbageCollectorJob>) Class.forName(parts[0]);
                    Method parseMethod = jobClass.getMethod("parse", String.class);
                    GarbageCollectorJob job = (GarbageCollectorJob) parseMethod.invoke(null, parts[1]);

                    addJob(job);
                }
            }

            reader.close();
        }
        catch(FileNotFoundException e) { /* do nothing */ }
        catch(IOException e) { /* do nothing */ }
        catch(IllegalAccessException e)    { /* will never happen (enforced through jobClass) */ }
        catch(NoSuchMethodException e)     { /* will never happen (enforced through jobClass) */ }
        catch(InvocationTargetException e) {
            throw new GarbageCollectorException("could not parse the garbage collector state file", e.getCause());
        }
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
