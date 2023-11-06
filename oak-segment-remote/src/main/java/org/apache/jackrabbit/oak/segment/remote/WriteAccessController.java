package org.apache.jackrabbit.oak.segment.remote;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WriteAccessController {
    private boolean isWritingAllowed = false;
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public void disableWriting() {
        try {


        lock.writeLock().lock();
        try {
            this.isWritingAllowed = false;
        } finally {
            lock.writeLock().unlock();
        }

        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public void enableWriting() {
        try {
            lock.writeLock().lock();
            try {
                this.isWritingAllowed = true;
                synchronized (this) {
                    this.notifyAll();
                }
            } finally {
                lock.writeLock().unlock();
            }

        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public void checkWritingAllowed() {
        lock.readLock().lock();
        try {
            while (!isWritingAllowed) {
                synchronized (this) {
                    try {
                        this.wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while waiting for writing to be allowed", e);
                    }
                }
            }
        } finally {
            lock.readLock().unlock();
        }
    }
}
