package com.iota.iri.utils.thread;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

/**
 * This interface extends the {@link ScheduledExecutorService} by providing additional methods to enqueue tasks without
 * throwing a {@link RejectedExecutionException} exception.
 *
 * This can be useful when preventing additional tasks to run is no error but an intended design decision in the
 * implementing class. In these cases raising an exception and catching it would cause too much unnecessary overhead
 * (using {@link Exception}s for control flow is an anti pattern).
 */
public interface SilentScheduledExecutorService extends ScheduledExecutorService {
    /**
     * Does the same as {@link ScheduledExecutorService#schedule(Runnable, long, TimeUnit)} but returns {@code null}
     * instead of throwing a {@link RejectedExecutionException} if the task cannot be scheduled for execution.
     */
    ScheduledFuture<?> silentSchedule(Runnable command, long delay, TimeUnit unit);

    /**
     * Does the same as {@link ScheduledExecutorService#schedule(Callable, long, TimeUnit)} but returns {@code null}
     * instead of throwing a {@link java.util.concurrent.RejectedExecutionException} if the task cannot be scheduled for
     * execution.
     */
    <V> ScheduledFuture<V> silentSchedule(Callable<V> callable, long delay, TimeUnit unit);

    /**
     * Does the same as {@link ScheduledExecutorService#scheduleAtFixedRate(Runnable, long, long, TimeUnit)} but returns
     * {@code null} instead of throwing a {@link RejectedExecutionException} if the task cannot be scheduled for
     * execution.
     */
    ScheduledFuture<?> silentScheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit);

    /**
     * Does the same as
     * {@link ScheduledExecutorService#scheduleWithFixedDelay(Runnable, long, long, TimeUnit)} but returns {@code null}
     * instead of throwing a {@link RejectedExecutionException} if the task cannot be scheduled for execution.
     */
    ScheduledFuture<?> silentScheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit);

    /**
     * Does the same as {@link ScheduledExecutorService#submit(Callable)} but returns {@code null} instead of throwing a
     * {@link RejectedExecutionException} if the task cannot be scheduled for execution.
     */
    <T> Future<T> silentSubmit(Callable<T> task);

    /**
     * Does the same as {@link ScheduledExecutorService#submit(Runnable)} but returns {@code null} instead of throwing a
     * {@link RejectedExecutionException} if the task cannot be scheduled for execution.
     */
    Future<?> silentSubmit(Runnable task);

    /**
     * Does the same as {@link ScheduledExecutorService#submit(Runnable, Object)} but returns {@code null} instead of
     * throwing a {@link RejectedExecutionException} if the task cannot be scheduled for execution.
     */
    <T> Future<T> silentSubmit(Runnable task, T result);

    /**
     * Does the same as {@link ScheduledExecutorService#invokeAll(Collection)} but returns {@code null} instead of
     * throwing a {@link RejectedExecutionException} if any task cannot be scheduled for execution.
     */
    <T> List<Future<T>> silentInvokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException;

    /**
     * Does the same as {@link ScheduledExecutorService#invokeAll(Collection, long, TimeUnit)} but returns {@code null}
     * instead of throwing a {@link RejectedExecutionException} if any task cannot be scheduled for execution.
     */
    <T> List<Future<T>> silentInvokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
            throws InterruptedException;

    /**
     * Does the same as {@link ScheduledExecutorService#invokeAny(Collection)} but returns {@code null} instead of
     * throwing a {@link RejectedExecutionException} if tasks cannot be scheduled for execution.
     */
    <T> T silentInvokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException;

    /**
     * Does the same as {@link ScheduledExecutorService#invokeAny(Collection, long, TimeUnit)} but returns {@code null}
     * instead of throwing a {@link RejectedExecutionException} if tasks cannot be scheduled for execution.
     */
    <T> T silentInvokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException;

    /**
     * Does the same as {@link ScheduledExecutorService#execute(Runnable)} but returns {@code null} instead of throwing
     * a {@link RejectedExecutionException} if this task cannot be accepted for execution.
     */
    void silentExecute(Runnable command);
}
