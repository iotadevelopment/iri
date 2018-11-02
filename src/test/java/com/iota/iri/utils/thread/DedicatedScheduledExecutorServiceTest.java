package com.iota.iri.utils.thread;

import com.iota.iri.service.milestone.MilestoneSolidifier;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class DedicatedScheduledExecutorServiceTest {
    private static final Logger logger = LoggerFactory.getLogger(MilestoneSolidifier.class);

    private static final DedicatedScheduledExecutorService executorService =
            new DedicatedScheduledExecutorService("Solidification Thread", logger);

    @Test
    public void testSubmit() {
        executorService.silentScheduleAtFixedRate(this::solidificationThread, 100, 500, TimeUnit.MILLISECONDS);

        ThreadUtils.sleep(5000);

        executorService.shutdownNow();
    }

    private void solidificationThread() {
        logger.info("THREAD DOES SOMETHING");
    }
}
