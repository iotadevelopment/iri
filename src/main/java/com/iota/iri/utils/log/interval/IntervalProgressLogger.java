package com.iota.iri.utils.log.interval;

import com.iota.iri.utils.log.ProgressLogger;
import com.iota.iri.utils.log.StatusLogger;
import org.slf4j.Logger;

import java.text.DecimalFormat;

/**
 * Implements the basic contract of the {@link ProgressLogger} while showing the progress as a status message in the
 * console.
 *
 * Instead of printing all messages immediately and unnecessarily spamming the console output, it rate limits the amount
 * of messages shown and only prints updates in a pre-defined interval.
 */
public class IntervalProgressLogger implements ProgressLogger {
    /**
     * Holds the default interval in which the logger shows messages.
     */
    private static final int DEFAULT_LOG_INTERVAL = 3000;

    /**
     * Holds the name of the task that this logger represents.
     */
    private final String taskName;

    /**
     * Holds the status logger, that takes care of showing our messages.
     */
    private final StatusLogger statusLogger;

    /**
     * The current step of the task.
     */
    private int currentStep = 0;

    /**
     * The total amount of steps of this task.
     */
    private int stepCount = 1;

    /**
     * Flag that indicates if the output is enabled.
     */
    private boolean enabled = true;

    /**
     * Creates a {@link ProgressLogger} that shows its progress in a pre-defined interval.
     *
     * @param taskName name of the task that shall be shown on the screen
     * @param logger {@link Logger} object that is used in combination with logback to issue messages in the console
     * @param logInterval the time in milliseconds between consecutive log messages
     */
    public IntervalProgressLogger(String taskName, Logger logger, int logInterval) {
        this.taskName = taskName;
        this.statusLogger = new StatusLogger(logger, logInterval);
    }

    /**
     * Does the same as {@link #IntervalProgressLogger(String, Logger, int)} but defaults to
     *
     * @param taskName name of the task that shall be shown on the screen
     * @param logger {@link Logger} object that is used in combination with logback to issue messages in the console
     */
    public IntervalProgressLogger(String taskName, Logger logger) {
        this(taskName, logger, DEFAULT_LOG_INTERVAL);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IntervalProgressLogger start() {
        return setCurrentStep(0).triggerOutput();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IntervalProgressLogger start(int stepCount) {
        return setStepCount(stepCount).setCurrentStep(0).triggerOutput();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IntervalProgressLogger progress() {
        return setCurrentStep(++currentStep).triggerOutput();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IntervalProgressLogger progress(int currentStep) {
        return setCurrentStep(currentStep).triggerOutput();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IntervalProgressLogger finish() {
        return setCurrentStep(stepCount).triggerOutput();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IntervalProgressLogger abort() {
        return abort(null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IntervalProgressLogger abort(Exception reason) {
        double progress = Math.min(100d, ((double) currentStep / (double) stepCount) * 100d);
        String logMessage = taskName + ": " + new DecimalFormat("#.00").format(progress) + "% ... [FAILED]";

        if (reason != null) {
            statusLogger.error(logMessage, reason);
        } else {
            statusLogger.error(logMessage);
        }

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getCurrentStep() {
        return currentStep;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IntervalProgressLogger setCurrentStep(int currentStep) {
        this.currentStep = currentStep;

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getStepCount() {
        return stepCount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IntervalProgressLogger setStepCount(int stepCount) {
        this.stepCount = stepCount;

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IntervalProgressLogger setEnabled(boolean enabled) {
        this.enabled = enabled;

        return this;
    }

    /**
     * This method triggers the output of the logger.
     *
     * It calculates the current progress, generates the log message and passes it on to the underlying status logger.
     *
     * @return the logger itself to allow the chaining of calls
     */
    private IntervalProgressLogger triggerOutput() {
        if (enabled && stepCount != 0) {
            double progress = Math.min(100d, ((double) currentStep / (double) stepCount) * 100d);
            String logMessage = taskName + ": " + new DecimalFormat("0.00").format(progress) + "% ..." +
                    (currentStep >= stepCount ? " [DONE]" : "");

            statusLogger.status(logMessage);
        }

        return this;
    }
}
