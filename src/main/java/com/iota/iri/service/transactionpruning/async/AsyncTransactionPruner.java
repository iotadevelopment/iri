package com.iota.iri.service.transactionpruning.async;

import com.iota.iri.conf.SnapshotConfig;
import com.iota.iri.controllers.TipsViewModel;
import com.iota.iri.service.snapshot.Snapshot;
import com.iota.iri.service.transactionpruning.TransactionPruner;
import com.iota.iri.service.transactionpruning.TransactionPrunerJob;
import com.iota.iri.service.transactionpruning.TransactionPruningException;
import com.iota.iri.service.transactionpruning.jobs.MilestonePrunerJob;
import com.iota.iri.service.transactionpruning.jobs.UnconfirmedSubtanglePrunerJob;
import com.iota.iri.storage.Tangle;
import com.iota.iri.utils.thread.ThreadIdentifier;
import com.iota.iri.utils.thread.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class implements the contract of the {@link TransactionPruner} while executing the jobs asynchronously in the
 * background.
 *
 * To start the background processing of pruning tasks one has to make use of the additional {@link #start()} and
 * {@link #shutdown()} methods.
 */
public class AsyncTransactionPruner implements com.iota.iri.service.transactionpruning.TransactionPruner {
    /**
     * The interval in milliseconds that the {@link AsyncTransactionPruner} will check if new cleanup tasks are
     * available and need to be processed.
     */
    private static final int GARBAGE_COLLECTOR_RESCAN_INTERVAL = 10000;

    /**
     * The interval (in milliseconds) in which the {@link AsyncTransactionPruner} will persist its state.
     *
     * Note: Since the worst thing that could happen when not having a 100% synced state file is to have a few floating
     *       "zombie" transactions in the database, we do not persist the state immediately but in intervals in a
     *       separate {@link Thread} (to save performance - until a db-based version gets introduced).
     */
    private static final int GARBAGE_COLLECTOR_PERSIST_INTERVAL = 1000;

    /**
     * Logger for this class allowing us to dump debug and status messages.
     */
    private static final Logger log = LoggerFactory.getLogger(AsyncTransactionPruner.class);

    /**
     * Tangle object which acts as a database interface
     */
    private final Tangle tangle;

    /**
     * Manager for the tips (required for removing pruned transactions from this manager)
     */
    private final TipsViewModel tipsViewModel;

    /**
     * Last local or global snapshot that acts as a starting point for the state of ledger
     */
    private final Snapshot snapshot;

    /**
     * Configuration with important snapshot related configuration parameters.
     */
    private final SnapshotConfig config;

    /**
     * Holds a reference to the {@link ThreadIdentifier} for the cleanup thread.
     *
     * Using a {@link ThreadIdentifier} for spawning the thread allows the {@link ThreadUtils} to spawn exactly one
     * thread for this instance even when we call the {@link #start()} method multiple times.
     */
    private final ThreadIdentifier cleanupThreadIdentifier = new ThreadIdentifier("Transaction Pruner");

    /**
     * Holds a reference to the {@link ThreadIdentifier} for the state persistence thread.
     *
     * Using a {@link ThreadIdentifier} for spawning the thread allows the {@link ThreadUtils} to spawn exactly one
     * thread for this instance even when we call the {@link #start()} method multiple times.
     */
    private final ThreadIdentifier persisterThreadIdentifier = new ThreadIdentifier("Transaction Pruner Persister");

    /**
     * A map of {@link JobParser}s allowing us to determine how to parse the jobs from the
     * {@link com.iota.iri.service.transactionpruning.TransactionPruner} state file, based on their type.
     */
    private final Map<String, JobParser> jobParsers = new HashMap<>();

    /**
     * List of cleanup jobs that shall get processed by the {@link AsyncTransactionPruner} (grouped by their class).
     */
    private final Map<Class<? extends TransactionPrunerJob>, JobQueue> jobQueues = new HashMap<>();

    /**
     * Holds a flag that indicates if the state shall be persisted.
     */
    private boolean persistRequested = false;

    /**
     * Creates a {@link TransactionPruner} that is able to process it's jobs asynchronously in the background and
     * persists its state in a file on the hard disk of the node.
     *
     * The asynchronous processing of the jobs is done through {@link Thread}s that are started and stopped by invoking
     * the corresponding {@link #start()} and {@link #shutdown()} methods. Since some of the builtin jobs require a
     * special logic for the way they are executed, we register the builtin job types here.
     *
     * @param tangle Tangle object which acts as a database interface
     * @param tipsViewModel manager for the tips (required for removing pruned transactions from this manager)
     * @param snapshot last local or global snapshot that acts as a starting point for the state of ledger
     * @param config Configuration with important snapshot related configuration parameters
     */
    public AsyncTransactionPruner(Tangle tangle, TipsViewModel tipsViewModel, Snapshot snapshot, SnapshotConfig config) {
        this.tangle = tangle;
        this.tipsViewModel = tipsViewModel;
        this.snapshot = snapshot;
        this.config = config;

        addJobQueue(UnconfirmedSubtanglePrunerJob.class, new SimpleJobQueue(this));
        addJobQueue(MilestonePrunerJob.class, new MilestonePrunerJobQueue(this, config));

        registerParser(MilestonePrunerJob.class, MilestonePrunerJob::parse);
        registerParser(UnconfirmedSubtanglePrunerJob.class, UnconfirmedSubtanglePrunerJob::parse);
    }

    /**
     * This method fulfills the contract of {@link TransactionPruner#addJob(TransactionPrunerJob)}.
     *
     * It first registers the required dependencies in the job that are relevant for its execution and that do not get
     * passed in via its constructor and then it adds the job to its corresponding queue.
     *
     * @param job the job that shall be executed
     * @throws TransactionPruningException if anything goes wrong while adding the job
     */
    public void addJob(TransactionPrunerJob job) throws TransactionPruningException {
        job.setTransactionPruner(this);
        job.setTangle(tangle);
        job.setTipsViewModel(tipsViewModel);
        job.setSnapshot(snapshot);

        getJobQueue(job.getClass()).addJob(job);

        saveState();
    }

    /**
     * This method fulfills the contract of {@link TransactionPruner#saveState()}.
     *
     * It iterates over all
     *
     * It does so by setting the {@link #persistRequested} flag to true, which will make the "Persister Thread" save the
     * state on its next iteration.
     *
     * Note: We incorporate a background job that periodically saves the state rather than doing it "live", to reduce
     *       the cost of this operation. While this can theoretically lead to a situation where the saved state is not
     *       100% correct and the latest changes get lost (if IRI crashes or gets restarted before the new changes could
     *       be persisted), the impact is marginal because it only leads to some floating "zombie" transactions that
     *       will stay in the database. This will be "solved" once we persist the changes in the database instead of a
     *       file on the hard disk. For now the trade off between faster processing times and leaving some garbage is
     *       reasonable.
     */
    public void saveState() {
        persistRequested = true;
    }

    public void saveStateNow() throws TransactionPruningException {
        try {
            AtomicInteger jobsPersisted = new AtomicInteger(0);

            Files.write(
                Paths.get(getStateFile().getAbsolutePath()),
                () -> jobQueues.values().stream()
                      .flatMap(JobQueue::stream)
                      .<CharSequence>map(jobEntry -> {
                          jobsPersisted.incrementAndGet();

                          return this.serializeJobEntry(jobEntry);
                      })
                      .iterator()
            );

            if (jobsPersisted.get() == 0) {
                try {
                    Files.deleteIfExists(Paths.get(getStateFile().getAbsolutePath()));
                } catch (IOException e) {
                    throw new TransactionPruningException("failed to remove the state file", e);
                }
            }
        } catch(Exception e) {
            throw new TransactionPruningException("failed to write the state file", e);
        }
    }




    /**
     * This method fulfills the contract of {@link TransactionPruner#restoreState()} by reading the serialized job
     * queues from the state file returned by {@link #getStateFile()} and parsing them back into their original
     * representation.
     */
    public void restoreState() throws TransactionPruningException {
        try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(new BufferedInputStream(new FileInputStream(getStateFile())))
        )) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(";", 2);
                if (parts.length >= 2) {
                    JobParser jobParser = this.jobParsers.get(parts[0]);
                    if (jobParser == null) {
                        throw new TransactionPruningException("could not determine a parser for cleanup job of type " + parts[0]);
                    }

                    addJob(jobParser.parse(parts[1]));
                }
            }
        } catch(IOException e) {
            if(getStateFile().exists()) {
                throw new TransactionPruningException("could not read the state file", e);
            }
        }
    }

    /**
     * This method fulfills the contract of {@link TransactionPruner#clear()}.
     *
     * It cycles through all registered {@link JobQueue}s and clears them.
     */
    public void clear() {
        for (JobQueue jobQueue : jobQueues.values()) {
            jobQueue.clear();
        }
    }

    /**
     * This method starts the cleanup and persistence {@link Thread}s that asynchronously process the queued jobs in the
     * background.
     *
     * Note: This method is thread safe since we use a {@link ThreadIdentifier} to address the {@link Thread}. The
     *       {@link ThreadUtils} take care of only launching exactly one {@link Thread} that is not terminated.
     */
    public void start() {
        ThreadUtils.spawnThread(this::processJobsThread, cleanupThreadIdentifier);
        ThreadUtils.spawnThread(this::persistThread, persisterThreadIdentifier);
    }

    /**
     * Shuts down the background job by setting the corresponding shutdown flag.
     */
    public void shutdown() {
        ThreadUtils.stopThread(cleanupThreadIdentifier);
        ThreadUtils.stopThread(persisterThreadIdentifier);
    }

    private void addJobQueue(Class<? extends TransactionPrunerJob> jobClass, JobQueue jobQueue) {
        jobQueues.put(jobClass, jobQueue);
    }

    /**
     * This method allows to register a {@link JobParser} for a given job type.
     *
     * When we serialize the pending jobs to save the current state, we also dump their class names, which allows us to
     * generically parse their serialized representation using the registered parser function back into the
     * corresponding job.
     *
     * @param jobClass class of the job that the TransactionPruner shall be able to handle
     * @param jobParser parser function for the serialized version of jobs of the given type
     */
    private void registerParser(Class<?> jobClass, JobParser jobParser) {
        this.jobParsers.put(jobClass.getCanonicalName(), jobParser);
    }

    /**
     * This method contains the logic for persisting the pruner state, that gets executed in a separate {@link Thread}.
     *
     * It periodically checks the {@link #persistRequested} flag and triggers the writing of the state file until the
     * {@link AsyncTransactionPruner} is shutting down.
     */
    private void persistThread() {
        while(!Thread.interrupted()) {
            try {
                if (persistRequested) {
                    saveStateNow();

                    persistRequested = false;
                }
            } catch(TransactionPruningException e) {
                log.error("could not persist transaction pruner state", e);
            }

            ThreadUtils.sleep(GARBAGE_COLLECTOR_PERSIST_INTERVAL);
        }
    }

    /**
     * This method creates a serialized version of the given job.
     *
     * @param job job that shall get serialized
     * @return serialized representation of the job
     */
    private String serializeJobEntry(TransactionPrunerJob job) {
        return job.getClass().getCanonicalName() + ";" + job.serialize();
    }

    /**
     * This method contains the logic for the processing of the cleanup jobs, that gets executed in a separate
     * {@link Thread}.
     *
     * It repeatedly calls {@link #processJobs()} until the TransactionPruner is shutting down.
     */
    private void processJobsThread() {
        while(!Thread.interrupted()) {
            try {
                processJobs();
            } catch(TransactionPruningException e) {
                log.error("error while processing the transaction pruner jobs", e);
            }

            ThreadUtils.sleep(GARBAGE_COLLECTOR_RESCAN_INTERVAL);
        }
    }

    /**
     * This method contains the logic for scheduling the jobs and executing them.
     *
     * It iterates through all available queues and triggers the processing of their jobs.
     *
     * @throws TransactionPruningException if anything goes wrong while processing the cleanup jobs
     */
    public void processJobs() throws TransactionPruningException {
        for(JobQueue jobQueue : jobQueues.values()) {
            if(Thread.interrupted()) {
                return;
            }

            jobQueue.processJobs();
        }
    }

    /**
     * This method retrieves the job queue belonging to a given job type.
     *
     * It first checks if a corresponding queue exists already and creates a new one if no queue was created yet for the
     * given job type.
     *
     * @param jobClass type of the job that we want to retrieve the queue for
     * @return the list of jobs for the provided job type
     */
    private JobQueue getJobQueue(Class<? extends TransactionPrunerJob> jobClass) throws TransactionPruningException {
        JobQueue jobQueue = jobQueues.get(jobClass);

        if(jobQueue == null) {
            throw new TransactionPruningException("jobs of type \"" + jobClass.getCanonicalName() + "\" are not supported");
        }

        return jobQueue;
        /*
        if (transactionPrunerJobs.get(jobClass) == null) {
            synchronized(this) {
                if (transactionPrunerJobs.get(jobClass) == null) {
                    transactionPrunerJobs.put(jobClass, new ArrayDeque<>());
                }
            }
        }

        return transactionPrunerJobs.get(jobClass);
        */
    }

    /**
     * This method returns a file handle to state file.
     *
     * It constructs the path of the file by appending the corresponding file extension to the
     * {@link com.iota.iri.conf.BaseIotaConfig#localSnapshotsBasePath} config variable. If the path is relative, it
     * places the file relative to the current working directory, which is usually the location of the iri.jar.
     *
     * @return File handle to the state file.
     */
    private File getStateFile() {
        return new File(config.getLocalSnapshotsBasePath() + ".snapshot.gc");
    }

    /**
     * Functional interface for the lambda function that takes care of parsing a specific job from its serialized String
     * representation into the corresponding object in memory.
     *
     * @see AsyncTransactionPruner#registerParser(Class, JobParser) to register the parser
     */
    @FunctionalInterface
    private interface JobParser {
        TransactionPrunerJob parse(String input) throws TransactionPruningException;
    }
}