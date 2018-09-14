package com.iota.iri.utils.log;

import org.slf4j.Logger;

public class StatusLogger {
    private static final int LOG_INTERVAL = 3000;

    private Logger logger;

    private String statusMessage = "";

    private String lastPrintedStatusMessage = "";

    private Thread outputThread = null;

    private long lastLogTime = 0;

    public StatusLogger(Logger logger) {
        this.logger = logger;
    }

    public void updateStatus(String message) {
        if (!message.equals(statusMessage)) {
            statusMessage = message;

            if (System.currentTimeMillis() - lastLogTime >= LOG_INTERVAL) {
                printStatusMessage();
            } else if (outputThread == null) {
                synchronized(this) {
                    if(outputThread == null) {
                        outputThread = new Thread(() -> {
                            try {
                                Thread.sleep(Math.max(LOG_INTERVAL - (System.currentTimeMillis() - lastLogTime), 1));
                            } catch(InterruptedException e) { /* do nothing */ }

                            printStatusMessage();

                            outputThread = null;
                        }, Thread.currentThread().getName());

                        outputThread.start();
                    }
                }
            }
        }
    }

    private void printStatusMessage() {
        if(!statusMessage.equals(lastPrintedStatusMessage)) {
            logger.info(statusMessage);

            lastLogTime = System.currentTimeMillis();
            lastPrintedStatusMessage = statusMessage;
        }
    }
}
