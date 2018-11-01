package com.iota.iri.utils.thread;

import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public interface SilentScheduledExecutorService {
    /**
     * Creates and executes a one-shot action that becomes enabled
     * after the given delay.
     *
     * @param command the task to execute
     * @param delay the time from now to delay execution
     * @param unit the time unit of the delay parameter
     * @return a ScheduledFuture representing pending completion of the task and whose {@code get()} method will return
     *         {@code null} upon completion or {@code null} if the command could not be scheduled because we have
     *         scheduled another command already
     * @throws NullPointerException if command is null
     */
    ScheduledFuture<?> silentSchedule(Runnable command, long delay, TimeUnit unit);

    /**
     * Creates and executes a ScheduledFuture that becomes enabled after the
     * given delay.
     *
     * @param callable the function to execute
     * @param delay the time from now to delay execution
     * @param unit the time unit of the delay parameter
     * @param <V> the type of the callable's result
     * @return a ScheduledFuture that can be used to extract result or cancel or {@code null} if the callable could not
     *         be scheduled because we have scheduled another callable already
     * @throws NullPointerException if callable is null
     */
    <V> ScheduledFuture<V> silentSchedule(Callable<V> callable, long delay, TimeUnit unit);

    /**
     * Creates and executes a periodic action that becomes enabled first
     * after the given initial delay, and subsequently with the given
     * period; that is executions will commence after
     * {@code initialDelay} then {@code initialDelay+period}, then
     * {@code initialDelay + 2 * period}, and so on.
     * If any execution of the task
     * encounters an exception, subsequent executions are suppressed.
     * Otherwise, the task will only terminate via cancellation or
     * termination of the executor.  If any execution of this task
     * takes longer than its period, then subsequent executions
     * may start late, but will not concurrently execute.
     *
     * @param command the task to execute
     * @param initialDelay the time to delay first execution
     * @param period the period between successive executions
     * @param unit the time unit of the initialDelay and period parameters
     * @return a ScheduledFuture representing pending completion of the task, and whose {@code get()} method will throw
     *         an exception upon cancellation or {@code null} if the command could not be scheduled because we have
     *         scheduled another command already
     * @throws NullPointerException if command is null
     * @throws IllegalArgumentException if period less than or equal to zero
     */
    ScheduledFuture<?> silentScheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit);
}
