package com.iota.iri.utils.thread;

import com.iota.iri.service.milestone.MilestoneSolidifier;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class DedicatedScheduledExecutorServiceTest {
    private static final Logger logger = LoggerFactory.getLogger(MilestoneSolidifier.class);

    private static final BoundedScheduledExecutorService executorService =
            new DedicatedScheduledExecutorService("Milestone Solidifier", logger);

    @Test
    public void testSubmit() {
        /*
        ScheduledFuture future = executorService.silentScheduleAtFixedRate(this::solidificationThread, 0, 2000, TimeUnit.MILLISECONDS);

        ThreadUtils.sleep(100);

        future.cancel(true);
        future.cancel(true);
        */

        // recurring tasks get cancelled by shutdown -> non recurring ones not


        executorService.silentScheduleWithFixedDelay(() -> {
            logger.info("I get executed every 500 milliseconds");

            throw new RuntimeException("huch");
        }, 100, 500, TimeUnit.MILLISECONDS);

        ThreadUtils.sleep(2000);

        executorService.shutdownNow();
    }

    private void solidificationThread() {
        logger.info("THREAD DOES SOMETHING");
    }
}
