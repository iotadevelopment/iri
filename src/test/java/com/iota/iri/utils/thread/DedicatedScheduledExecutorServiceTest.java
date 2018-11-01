package com.iota.iri.utils.thread;

import com.iota.iri.service.milestone.MilestoneSolidifier;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class DedicatedScheduledExecutorServiceTest {
    private static final DedicatedScheduledExecutorService executorService =
            new DedicatedScheduledExecutorService("Solidification Thread", LoggerFactory.getLogger(MilestoneSolidifier.class));

    @Test
    public void testSubmit() {
        executorService.silentScheduleAtFixedRate(this::solidificationThread, 500, 2000, TimeUnit.MILLISECONDS);

        ThreadUtils.sleep(5000);

        executorService.shutdown();
    }

    private void solidificationThread() {
        System.out.println("THREAD DOES SOMETHING");
    }
}
