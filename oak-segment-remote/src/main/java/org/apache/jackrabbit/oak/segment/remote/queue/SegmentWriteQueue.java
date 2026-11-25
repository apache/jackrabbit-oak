/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.segment.remote.queue;

import org.apache.jackrabbit.oak.segment.remote.RemoteSegmentArchiveEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SegmentWriteQueue implements Closeable {

    public static final int THREADS = Integer.getInteger("oak.segment.remote.threads", 5);

    private static final int QUEUE_SIZE = Integer.getInteger("oak.segment.remote.queue.size", 20);

    private static final long FLUSH_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(10);

    private static final Logger log = LoggerFactory.getLogger(SegmentWriteQueue.class);

    private final BlockingDeque<SegmentWriteAction> queue;

    private final BlockingDeque<SegmentWriteAction> retryQueue;

    private final Map<UUID, SegmentWriteAction> segmentsByUUID;

    private final ExecutorService executor;

    private final ReadWriteLock flushLock;

    private final SegmentConsumer writer;

    private final long flushTimeoutNanos;

    private volatile boolean shutdown;

    private final Object retryMonitor = new Object();

    private volatile boolean retryPending;

    public SegmentWriteQueue(SegmentConsumer writer) {
        this(writer, QUEUE_SIZE, THREADS);
    }

    SegmentWriteQueue(SegmentConsumer writer, int queueSize, int threadNo) {
        this(writer, queueSize, threadNo, FLUSH_TIMEOUT_MILLIS);
    }

    SegmentWriteQueue(SegmentConsumer writer, int queueSize, int threadNo, long flushTimeoutMillis) {
        if (flushTimeoutMillis <= 0) {
            throw new IllegalArgumentException("Flush timeout must be positive");
        }
        this.writer = writer;
        flushTimeoutNanos = TimeUnit.MILLISECONDS.toNanos(flushTimeoutMillis);
        segmentsByUUID = new ConcurrentHashMap<>();
        flushLock = new ReentrantReadWriteLock();

        queue = new LinkedBlockingDeque<>(queueSize);
        retryQueue = new LinkedBlockingDeque<>(queueSize);
        executor = Executors.newFixedThreadPool(threadNo + 1);
        for (int i = 0; i < threadNo; i++) {
            executor.submit(this::mainLoop);
        }
        executor.submit(this::emergencyLoop);
    }

    private void mainLoop() {
        while (!shutdown) {
            try {
                waitWhileBroken();
                if (shutdown) {
                    break;
                }
                consume(queue);
            } catch (SegmentConsumeException e) {
                SegmentWriteAction segment = e.segment;
                log.error("Can't persist the segment {}", segment.getUuid(), e.getCause());
                enqueueForRetry(segment);
            }
        }
    }

    private void enqueueForRetry(SegmentWriteAction segment) {
        retryPending = true;
        boolean interrupted = false;
        while (true) {
            try {
                retryQueue.putLast(segment);
                synchronized (retryMonitor) {
                    retryPending = !retryQueue.isEmpty();
                    retryMonitor.notifyAll();
                }
                break;
            } catch (InterruptedException e) {
                interrupted = true;
                log.warn("Interrupted while re-adding the segment {} to the retry queue", segment.getUuid(), e);
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private void consume(BlockingDeque<SegmentWriteAction> source) throws SegmentConsumeException {
        SegmentWriteAction segment = null;
        try {
            segment = source.poll(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            log.error("Poll from queue interrupted", e);
        }
        if (segment != null) {
            persist(segment);
            complete(segment);
        }
    }

    private void persist(SegmentWriteAction segment) throws SegmentConsumeException {
        try {
            segment.passTo(writer);
        } catch (IOException | RuntimeException e) {
            throw new SegmentConsumeException(segment, e);
        }
    }

    private void complete(SegmentWriteAction segment) {
        synchronized (segmentsByUUID) {
            segmentsByUUID.remove(segment.getUuid());
            segmentsByUUID.notifyAll();
        }
    }

    private void emergencyLoop() {
        while (!shutdown) {
            waitUntilBroken();
            if (shutdown) {
                break;
            }

            SegmentWriteAction segment = retryQueue.peekFirst();
            if (segment == null) {
                continue;
            }
            try {
                persist(segment);
                retryQueue.removeFirst();
                synchronized (retryMonitor) {
                    retryPending = !retryQueue.isEmpty();
                    retryMonitor.notifyAll();
                }
                complete(segment);
            } catch (SegmentConsumeException e) {
                log.error("Can't persist the segment {}", segment.getUuid(), e.getCause());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e1) {
                    log.warn("Interrupted", e);
                }
                if (shutdown) {
                    log.error("Shutdown initiated. The segment {} will be dropped.", segment.getUuid());
                }
            }
        }
    }

    public void addToQueue(RemoteSegmentArchiveEntry indexEntry, byte[] data, int offset, int size) throws IOException {
        waitWhileBroken();
        if (shutdown) {
            throw new IllegalStateException("Can't accept the new segment - shutdown in progress");
        }

        SegmentWriteAction action = new SegmentWriteAction(indexEntry, data, offset, size);
        flushLock.readLock().lock();
        try {
            segmentsByUUID.put(action.getUuid(), action);
            if (!queue.offer(action, 1, TimeUnit.MINUTES)) {
                segmentsByUUID.remove(action.getUuid());
                throw new IOException("Can't add segment to the queue");
            }
        } catch (InterruptedException e) {
            segmentsByUUID.remove(action.getUuid());
            throw new IOException(e);
        } finally {
            flushLock.readLock().unlock();
        }
    }

    public void flush() throws IOException {
        flushLock.writeLock().lock();
        try {
            synchronized (segmentsByUUID) {
                int lastPendingCount = segmentsByUUID.size();
                long lastProgress = System.nanoTime();
                while (!segmentsByUUID.isEmpty()) {
                    segmentsByUUID.wait(100);
                    int pendingCount = segmentsByUUID.size();
                    if (pendingCount < lastPendingCount) {
                        lastPendingCount = pendingCount;
                        lastProgress = System.nanoTime();
                    } else if (System.nanoTime() - lastProgress >= flushTimeoutNanos) {
                        throw new IOException("No segment write completed for "
                                + TimeUnit.NANOSECONDS.toMillis(flushTimeoutNanos)
                                + " ms. Queue: " + queue + ". Retry queue: " + retryQueue
                                + ". Segment map: " + segmentsByUUID);
                    }
                }
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        } finally {
            flushLock.writeLock().unlock();
        }
    }

    public SegmentWriteAction read(UUID id) {
        return segmentsByUUID.get(id);
    }

    @Override
    public void close() throws IOException {
        shutdown = true;
        try {
            executor.shutdown();
            if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
                throw new IOException("The write wasn't able to shut down clearly");
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    public boolean isEmpty() {
        return segmentsByUUID.isEmpty();
    }

    boolean isBroken() {
        return retryPending;
    }

    int getSize() {
        return queue.size() + retryQueue.size();
    }

    private void waitWhileBroken() {
        if (!retryPending) {
            return;
        }
        synchronized (retryMonitor) {
            while (isBroken() && !shutdown) {
                try {
                    retryMonitor.wait(100);
                } catch (InterruptedException e) {
                    log.warn("Interrupted", e);
                }
            }
        }
    }

    private void waitUntilBroken() {
        if (retryPending) {
            return;
        }
        synchronized (retryMonitor) {
            while (!isBroken() && !shutdown) {
                try {
                    retryMonitor.wait(100);
                } catch (InterruptedException e) {
                    log.warn("Interrupted", e);
                }
            }
        }
    }

    public interface SegmentConsumer {

        void consume(RemoteSegmentArchiveEntry indexEntry, byte[] data, int offset, int size) throws IOException;

    }

    public static class SegmentConsumeException extends Exception {

        private final SegmentWriteAction segment;

        public SegmentConsumeException(SegmentWriteAction segment, Exception cause) {
            super(cause);
            this.segment = segment;
        }
    }
}
