package com.iota.iri.service.transactionpruning.async;

import com.iota.iri.conf.SnapshotConfig;
import com.iota.iri.service.snapshot.SnapshotManager;
import com.iota.iri.service.transactionpruning.TransactionPruner;
import com.iota.iri.service.transactionpruning.TransactionPrunerJob;
import com.iota.iri.service.transactionpruning.TransactionPruningException;
import com.iota.iri.utils.thread.ThreadIdentifier;
import com.iota.iri.utils.thread.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


/**
 * This class implements the contract of the {@link TransactionPruner} while executing the jobs asynchronously in the
 * background.
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
     * Holds a reference to the {@link ThreadIdentifier} for the cleanup thread.
     *
     * Using a {@link ThreadIdentifier} for spawning the thread allows the {@link ThreadUtils} to spawn exactly one
     * thread for this instance even when we call the {@link #start()} method multiple times.
     */
    private final ThreadIdentifier cleanupThreadIdentifier = new ThreadIdentifier("Transaction Pruner");

    /**
     * Holds a reference to the {@link ThreadIdentifier} for the cleanup thread.
     *
     * Using a {@link ThreadIdentifier} for spawning the thread allows the {@link ThreadUtils} to spawn exactly one
     * thread for this instance even when we call the {@link #start()} method multiple times.
     */
    private final ThreadIdentifier persisterThreadIdentifier = new ThreadIdentifier("Transaction Pruner Persister");

    /**
     * Holds a reference to the {@link SnapshotManager} that this
     * {@link com.iota.iri.service.transactionpruning.TransactionPruner} belongs to.
     */
    private final SnapshotManager snapshotManager;

    /**
     * A map of {@link JobParser}s allowing us to determine how to parse the jobs from the
     * {@link com.iota.iri.service.transactionpruning.TransactionPruner} state file, based on their type.
     */
    private final Map<String, JobParser> jobParsers = new HashMap<>();

    /**
     * A map of {@link QueueProcessor}s allowing us to process queues based on the type of the job.
     */
    //private final Map<Class<? extends TransactionPrunerJob>, QueueProcessor> queueProcessors = new HashMap<>();

    /**
     * A map of {@link QueueConsolidator}s allowing us to consolidate the queues to consume less "space".
     */
    //private final Map<Class<? extends TransactionPrunerJob>, QueueConsolidator> queueConsolidators = new HashMap<>();

    private final Map<Class<? extends TransactionPrunerJob>, JobQueue> jobQueues = new HashMap<>();

    /**
     * List of cleanup jobs that shall get processed by the
     * {@link com.iota.iri.service.transactionpruning.TransactionPruner} (grouped by their class).
     */
    private final Map<Class<? extends TransactionPrunerJob>, Deque<TransactionPrunerJob>> transactionPrunerJobs
        = new ConcurrentHashMap<>();

    /**
     * Holds a flag that indicates if the state shall be persisted.
     */
    private boolean persistRequested = true;

    /**
     * TODO
     */
    public AsyncTransactionPruner(SnapshotManager snapshotManager, SnapshotConfig config) {
        this.snapshotManager = snapshotManager;

        registerSupportedJobTypes(config);
    }

    /**
     * This method fulfills the contract of {@link TransactionPruner#addJob(TransactionPrunerJob)}.
     *
     * It first registers the {@link TransactionPruner} in the job, so the job has access to all the properties that are
     * relevant for its execution, and that do not get passed in via its constructor. Then it retrieves the queue based
     * on its class and adds it there.
     *
     * After adding the job to its corresponding queue we try to consolidate the queue and persist the changes.
     *
     * @param job the job that shall be executed
     * @throws TransactionPruningException if anything goes wrong while adding the job
     */
    public void addJob(TransactionPrunerJob job) throws TransactionPruningException {
        job.registerGarbageCollector(this);

        getJobQueue(job.getClass()).addJob(job);

        /*
        Deque<TransactionPrunerJob> jobQueue = getJobQueue(job.getClass());
        jobQueue.addLast(job);

        // consolidating the queue is optional
        QueueConsolidator queueConsolidator = queueConsolidators.get(job.getClass());
        if(queueConsolidator != null) {
            queueConsolidator.consolidateQueue(this, snapshotManager, jobQueue);
        }
        */

        saveState();
    }

    /**
     * This method fulfills the contract of {@link TransactionPruner#saveState()}.
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

    /**
     * This method fulfills the contract of {@link TransactionPruner#restoreState()} by reading the serialized job
     * queues from the state file returned by {@link #getStateFile()} and parsing them back into their original
     * representation.
     */
    public void restoreState() throws TransactionPruningException {
        try {
            BufferedReader reader = new BufferedReader(
                new InputStreamReader(
                    new BufferedInputStream(
                        new FileInputStream(getStateFile())
                    )
                )
            );

            try {
                String line;
                while((line = reader.readLine()) != null) {
                    String[] parts = line.split(";", 2);
                    if(parts.length >= 2) {
                        JobParser jobParser = this.jobParsers.get(parts[0]);
                        if(jobParser == null) {
                            throw new TransactionPruningException("could not determine a parser for cleanup job of type " + parts[0]);
                        }

                        addJob(jobParser.parse(parts[1]));
                    }
                }
            } finally {
                reader.close();
            }
        } catch(IOException e) {
            throw new TransactionPruningException("could not read the state file", e);
        }
    }

    /**
     * This method fulfills the contract of {@link TransactionPruner#clear()}.
     */
    public void clear() {
        transactionPrunerJobs.clear();
    }

    /**
     * This method starts the cleanup and persistence {@link Thread}s that asynchronously process the queued jobs in the
     * background.
     *
     * Note: This method is thread safe since we use a {@link ThreadIdentifier} to address the {@link Thread}. The
     *       {@link ThreadUtils} take care of only launching exactly one {@link Thread} that is not terminated.
     */
    public void start() {
        ThreadUtils.spawnThread(this::cleanupThread, cleanupThreadIdentifier);
        ThreadUtils.spawnThread(this::persistThread, persisterThreadIdentifier);
    }

    /**
     * Shuts down the background job by setting the corresponding shutdown flag.
     */
    public void shutdown() {
        ThreadUtils.stopThread(cleanupThreadIdentifier);
        ThreadUtils.stopThread(persisterThreadIdentifier);
    }

    /**
     * This method registers the builtin job types, so the {@link AsyncTransactionPruner} knows how to process the
     * different kind of jobs it can hold.
     */
    private void registerSupportedJobTypes(SnapshotConfig config) {
        addJobQueue(MilestonePrunerJob.class, new MilestonePrunerJobQueue(this, config));
        addJobQueue(UnconfirmedSubtanglePrunerJob.class, new JobQueue(this));

        registerParser(MilestonePrunerJob.class, MilestonePrunerJob::parse);
        registerParser(UnconfirmedSubtanglePrunerJob.class, UnconfirmedSubtanglePrunerJob::parse);

        /*
        registerQueueProcessor(MilestonePrunerJob.class, MilestonePrunerJob::processQueue);
        registerQueueConsolidator(MilestonePrunerJob.class, MilestonePrunerJob::consolidateQueue);


        registerQueueProcessor(UnconfirmedSubtanglePrunerJob.class, UnconfirmedSubtanglePrunerJob::processQueue);
        */
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
     * This method allows us to register a {@link QueueProcessor} for the given job type.
     *
     * Since different kinds of jobs get processed in a different way, we are able to generically process them based on
     * their type after having registered a processor for them.
     *
     * @param jobClass class of the job that the TransactionPruner shall be able to handle
     * @param queueProcessor function that takes care of processing the queue for this particular type
     */
    /*
    private void registerQueueProcessor(Class<? extends TransactionPrunerJob> jobClass, QueueProcessor queueProcessor) {
        this.queueProcessors.put(jobClass, queueProcessor);
    }
    */

    /**
     * This method allows us to register a {@link QueueConsolidator} for the given job type.
     *
     * Some jobs can be consolidated to consume less space in the queue by grouping them together or skipping them
     * completely. While the consolidation of multiple jobs into fewer ones is optional and only required for certain
     * types of jobs, this method allows us to generically handle this use case by registering a handler for the job
     * class that supports this feature.
     *
     * @param jobClass class of the job that the TransactionPruner shall be able to handle
     * @param queueConsolidator lambda that takes care of consolidating the entries in a queue
     */
    /*
    private void registerQueueConsolidator(Class<? extends TransactionPrunerJob> jobClass,
                                           QueueConsolidator queueConsolidator) {
        this.queueConsolidators.put(jobClass, queueConsolidator);
    }
    */

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
                    Files.write(
                       Paths.get(getStateFile().getAbsolutePath()),
                        () -> transactionPrunerJobs.values().stream()
                              .flatMap(Collection::stream)
                              .<CharSequence>map(this::serializeJobEntry)
                              .iterator()
                    );

                    persistRequested = false;
                }
            } catch(Exception e) {
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
    private void cleanupThread() {
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
     * It iterates through all available queues and executes the corresponding {@link QueueProcessor} for each of them
     * until all queues have been processed.
     *
     * @throws TransactionPruningException if anything goes wrong while processing the cleanup jobs
     */
    public void processJobs() throws TransactionPruningException {
        for(Map.Entry<Class<? extends TransactionPrunerJob>, JobQueue> entry : jobQueues.entrySet()) {
            if(Thread.interrupted()) {
                return;
            }

            entry.getValue().processJobs();

            /*
            QueueProcessor queueProcessor = queueProcessors.get(entry.getKey());
            if(queueProcessor == null) {
                throw new TransactionPruningException("could not determine a queue processor for cleanup job of type " + entry.getKey().getCanonicalName());
            }

            queueProcessor.processQueue(this, snapshotManager, entry.getValue());
            */
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
        return new File(snapshotManager.getConfiguration().getLocalSnapshotsBasePath() + ".snapshot.gc");
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

    /**
     * Functional interface for the lambda function that takes care of processing a queue.
     *
     * It is used to offer an interface for generically processing the different jobs that are supported by the
     * {@link TransactionPruner}.
     *
     * @see AsyncTransactionPruner#registerQueueProcessor(Class, QueueProcessor) to register the processor
     */
    @FunctionalInterface
    private interface QueueProcessor {
        void processQueue(AsyncTransactionPruner transactionPruner, SnapshotManager snapshotManager, Deque<TransactionPrunerJob> jobQueue) throws TransactionPruningException;
    }

    /**
     * Functional interface for the lambda function that takes care of consolidating a queue.
     *
     * It is mainly used to merge multiple jobs that share the same status into a single one that covers all merged jobs and
     * therefore reduce the memory footprint and file size of the {@link TransactionPruner} state file.
     *
     * The consolidation of the queues is optional and does not have to be supported by every job type.
     *
     * @see AsyncTransactionPruner#registerQueueConsolidator(Class, QueueConsolidator) to register the consolidator
     */
    @FunctionalInterface
    private interface QueueConsolidator {
        void consolidateQueue(TransactionPruner transactionPruner, SnapshotManager snapshotManager, Deque<TransactionPrunerJob> jobQueue) throws TransactionPruningException;
    }
}
