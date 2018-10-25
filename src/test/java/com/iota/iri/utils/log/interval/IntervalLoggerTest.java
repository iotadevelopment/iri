package com.iota.iri.utils.log.interval;

import com.iota.iri.utils.thread.ThreadUtils;
import org.junit.Test;

public class IntervalLoggerTest {
    @Test
    public void testInfo() {
        IntervalLogger logger = new IntervalLogger(IntervalLogger.class);

        logger.info("Test");
        logger.info("Test1");
        logger.info("Test2");

        logger.triggerOutput(true);

        ThreadUtils.sleep(5000);

        logger.info("Test2");
    }
}
