package com.iota.iri.utils.thread;

import jdk.nashorn.internal.objects.annotations.Constructor;
import jdk.nashorn.internal.objects.annotations.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class DedicatedScheduledExecutorService implements ScheduledExecutorService {
    /**
     * Logger for this class allowing us to dump debug and status messages.
     */
    private static final Logger DEFAULT_LOGGER = LoggerFactory.getLogger(DedicatedScheduledExecutorService.class);

    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

    private final Logger logger;

    private final String threadName;

    private final boolean debug;

    private AtomicBoolean threadStarted = new AtomicBoolean(false);

    public DedicatedScheduledExecutorService(String threadName, Logger logger, boolean debug) {
        this.threadName = threadName;
        this.logger = logger;
        this.debug = debug;
    }

    public DedicatedScheduledExecutorService(String threadName, boolean debug) {
        this(threadName, DEFAULT_LOGGER, debug);
    }

    public DedicatedScheduledExecutorService(String threadName, Logger logger) {
        this(threadName, logger, false);
    }

    public DedicatedScheduledExecutorService(boolean debug, Logger logger) {
        this(null, logger, true);
    }

    public DedicatedScheduledExecutorService(String threadName) {
        this(threadName, DEFAULT_LOGGER, false);
    }

    public DedicatedScheduledExecutorService(boolean debug) {
        this(null, DEFAULT_LOGGER, true);
    }

    public DedicatedScheduledExecutorService() {
        this(null, DEFAULT_LOGGER, false);
    }

    public String getThreadName() {
        return threadName;
    }

    public ScheduledFuture<?> silentScheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        try {
            return scheduleAtFixedRate(command, initialDelay, period, unit);
        } catch (RejectedExecutionException e) {
            // omit error message (we are silent)
            return null;
        }
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        if (threadStarted.compareAndSet(false, true)) {
            printStartupMessage(delay, unit);

            return executorService.schedule(buildLoggingRunnable(command), delay, unit);
        }

        throw new RejectedExecutionException("thread pool capacity exhausted");
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        if (threadStarted.compareAndSet(false, true)) {
            printStartupMessage(delay, unit);

            return executorService.schedule(buildLoggingCallable(callable), delay, unit);
        }

        throw new RejectedExecutionException("thread pool capacity exhausted");
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        if (threadStarted.compareAndSet(false, true)) {
            printStartupMessage(initialDelay, period, unit);

            return executorService.scheduleAtFixedRate(buildLoggingRunnable(command), initialDelay, period, unit);
        }

        throw new RejectedExecutionException("thread pool capacity exhausted");
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        if (threadStarted.compareAndSet(false, true)) {
            printStartupMessage(initialDelay, delay, unit);

            return executorService.scheduleWithFixedDelay(buildLoggingRunnable(command), initialDelay, delay, unit);
        }

        throw new RejectedExecutionException("thread pool capacity exhausted");
    }

    @Override
    public void shutdown() {
        printStopMessage();

        executorService.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        printStopMessage();

        return executorService.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return executorService.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return executorService.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return executorService.awaitTermination(timeout, unit);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        if (threadStarted.compareAndSet(false, true)) {
            printStartupMessage();

            return executorService.submit(buildLoggingCallable(task));
        }

        throw new RejectedExecutionException("thread pool capacity exhausted");
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        if (threadStarted.compareAndSet(false, true)) {
            printStartupMessage();

            return executorService.submit(buildLoggingRunnable(task), result);
        }

        throw new RejectedExecutionException("thread pool capacity exhausted");
    }

    @Override
    public Future<?> submit(Runnable task) {
        if (threadStarted.compareAndSet(false, true)) {
            printStartupMessage();

            return executorService.submit(buildLoggingRunnable(task));
        }

        throw new RejectedExecutionException("thread pool capacity exhausted");
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return executorService.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        return null;
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return null;
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return null;
    }

    @Override
    public void execute(Runnable command) {

    }

    private <V> Callable<V> buildLoggingCallable(Callable<V> callable) {
        String callerThreadName = Thread.currentThread().getName();

        return () -> {
            if (threadName != null) {
                Thread.currentThread().setName(threadName);
            } else {
                Thread.currentThread().setName(callerThreadName);
            }

            String printableThreadName = threadName != null
                    ? threadName
                    : "UNNAMED THREAD (started by \"" + callerThreadName + "\")";

            if (debug) {
                logger.info(printableThreadName + " [STARTED]");
            }

            try {
                V result = callable.call();

                if (debug) {
                    logger.info(printableThreadName + " [FINISHED]");
                }

                return result;
            } catch (Exception e) {
                logger.error(printableThreadName + " [CRASHED]", e);

                throw e;
            } finally {
                threadStarted.set(false);
            }
        };
    }

    private <V> Runnable buildLoggingRunnable(Runnable runnable) {
        String callerThreadName = Thread.currentThread().getName();

        return () -> {
            if (threadName != null) {
                Thread.currentThread().setName(threadName);
            } else {
                Thread.currentThread().setName(callerThreadName);
            }

            String printableThreadName = threadName != null
                    ? threadName
                    : "UNNAMED THREAD (started by \"" + callerThreadName + "\")";

            if (debug) {
                logger.info(printableThreadName + " [STARTED]");
            }

            try {
                runnable.run();

                if (debug) {
                    logger.info(printableThreadName + " [STOPPED]");
                }
            } catch (Exception e) {
                logger.error(printableThreadName + " [CRASHED]", e);

                throw e;
            } finally {
                threadStarted.set(false);
            }
        };
    }

    private void printStartupMessage(long delay, long interval, TimeUnit unit) {
        if (threadName != null) {
            logger.info(buildStartupMessage(delay, interval, unit));
        }
    }

    private void printStartupMessage(long delay, TimeUnit unit) {
        if (threadName != null) {
            logger.info(buildStartupMessage(delay, unit));
        }
    }

    private void printStartupMessage() {
        if (threadName != null) {
            logger.info(buildStartupMessage());
        }
    }

    private void printStopMessage() {
        if (threadName != null) {
            logger.info(buildStopMessage());
        }
    }

    private String buildStartupMessage(long delay, long interval, TimeUnit unit) {
        String timeoutMessageFragment = buildTimeoutMessageFragment(delay, unit);
        String intervalMessageFragment = buildIntervalMessageFragment(interval, unit);

        String timeMessageFragment = "";
        if (timeoutMessageFragment != null) {
            timeMessageFragment += " (" + timeoutMessageFragment + (intervalMessageFragment != null ? " / " : ")");
        }
        if (intervalMessageFragment != null) {
            timeMessageFragment += (timeoutMessageFragment == null ? " (" : "") + intervalMessageFragment + ")";
        }

        if (threadName != null) {
            return "Starting [" + threadName + "]" + timeMessageFragment + " ...";
        } else {
            return "Starting [UNNAMED]" + timeMessageFragment + " ...";
        }
    }

    private String buildStartupMessage(long delay, TimeUnit unit) {
        return buildStartupMessage(delay, 0, unit);
    }

    private String buildStartupMessage() {
        return buildStartupMessage(0, 0, null);
    }

    private String buildTimeoutMessageFragment(long timeout, TimeUnit unit) {
        if (timeout == 0) {
            return null;
        } else {
            return "in " + timeout + buildUnitAbbreviation(unit);
        }
    }

    private String buildIntervalMessageFragment(long interval, TimeUnit unit) {
        if (interval == 0) {
            return null;
        } else {
            return "every " + interval + buildUnitAbbreviation(unit);
        }
    }

    private String buildUnitAbbreviation(TimeUnit unit) {
        switch (unit) {
            case NANOSECONDS:  return "ns";
            case MICROSECONDS: return "µs";
            case MILLISECONDS: return "ms";
            case SECONDS:      return "s";
            case MINUTES:      return "min";
            case HOURS:        return "hours";
            case DAYS:         return "days";
            default:           return "";
        }
    }

    private String buildStopMessage() {
        return "Stopping " + threadName + " ...";
    }
}
