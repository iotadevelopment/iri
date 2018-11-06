package com.iota.iri.utils.thread;

import com.iota.iri.service.milestone.MilestoneSolidifier;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class DedicatedScheduledExecutorServiceTest {
    private static final Logger logger = LoggerFactory.getLogger(MilestoneSolidifier.class);

    private static final BoundedScheduledExecutorService executorService =
            new BoundedScheduledExecutorService(1);

    @Test
    public void testSubmit() {
        executorService.silentScheduleAtFixedRate(this::solidificationThread, 0, 2000, TimeUnit.MILLISECONDS).cancel(true);
        System.out.println(executorService.silentScheduleAtFixedRate(() -> {
            System.out.println("TUST");
        }, 0, 2000, TimeUnit.MILLISECONDS));

        ThreadUtils.sleep(5000);

        executorService.shutdownNow();
    }

    private void solidificationThread() {
        logger.info("THREAD DOES SOMETHING");
    }
}
