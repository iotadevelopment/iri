package com.iota.iri.utils.thread;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class DedicatedScheduledExecutorServiceTest {
    private static final DedicatedScheduledExecutorService executorService =
            new DedicatedScheduledExecutorService("Milestone Solidifier");

    @Test
    public void testSubmit() {
        executorService.silentScheduleAtFixedRate(this::threadException, 0, 1000, TimeUnit.MILLISECONDS);
        executorService.silentScheduleAtFixedRate(this::threadException, 0, 1000, TimeUnit.MILLISECONDS);

        ThreadUtils.sleep(5000);
    }

    private void threadException() {
        System.out.println("THREAD DOES SOMETHING");

        throw new RuntimeException("Test");
    }
}
