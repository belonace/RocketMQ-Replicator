package com.belo.rocketmq.replicator.worker.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by liqiang on 2017/10/17.
 */
public abstract class ShutdownableThread extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShutdownableThread.class);

    private AtomicBoolean isRunning = new AtomicBoolean(true);
    private CountDownLatch shutdownLatch = new CountDownLatch(1);
    private Boolean isInterruptible;

    public ShutdownableThread(String name, boolean isInterruptible) {
        super(name);
        this.setDaemon(false);
        this.isInterruptible = isInterruptible;
    }

    @Override
    public void run() {
        LOGGER.info("Starting ");
        try {
            while (isRunning.get()) {
                doWork();
            }
        } catch (Exception e) {
            if (isRunning.get()) {
                LOGGER.error("Error due to ", e);
            }
        }
        shutdownLatch.countDown();
        LOGGER.info("Stopped ");
    }

    public abstract void doWork();

    public void shutdown() {
        initiateShutdown();
        awaitShutdown();
    }

    public void awaitShutdown() {
        try {
            shutdownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LOGGER.info("Shutdown completed");
    }

    public Boolean initiateShutdown() {
        if (isRunning.compareAndSet(true, false)) {
            LOGGER.info("Shutting down");
            isRunning.set(false);
            if (isInterruptible) {
                interrupt();
            }
            return true;
        } else {
            return false;
        }
    }
}
