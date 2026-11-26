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
package org.apache.jackrabbit.oak.segment.remote.persistentcache;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.jackrabbit.oak.segment.remote.persistentcache.PersistentDiskCacheV2Async.PendingSegment;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

/** Bounded FIFO whose head becomes eligible by age, queue pressure, or shutdown. */
final class PendingWriteQueue {
    enum ReleaseReason { SHUTDOWN, AGE, PRESSURE }

    private static final class Entry {
        final PendingSegment value;
        final long enqueuedNanos = System.nanoTime();
        final int bytes;
        final boolean control;

        Entry(PendingSegment value, int bytes, boolean control) {
            this.value = value;
            this.bytes = bytes;
            this.control = control;
        }
    }

    private final ArrayDeque<Entry> entries = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final Condition notFull = lock.newCondition();
    private final int maxSize;
    private final int preemptiveFlushSize;
    private final long delayNanos;
    private final AsyncDiskCacheStats monitor;
    private boolean draining;

    PendingWriteQueue(int maxSize, int preemptiveFlushSize, long delayMillis) {
        this(maxSize, preemptiveFlushSize, delayMillis,
                new AsyncDiskCacheStats(StatisticsProvider.NOOP));
    }

    PendingWriteQueue(int maxSize, int preemptiveFlushSize, long delayMillis,
                      AsyncDiskCacheStats monitor) {
        if (maxSize <= 0 || preemptiveFlushSize <= 0 || preemptiveFlushSize > maxSize
                || (delayMillis > 0 && preemptiveFlushSize == maxSize) || delayMillis < 0) {
            throw new IllegalArgumentException("Require positive queue sizes, a nonnegative delay, "
                    + "and preemptiveFlushSize < maxSize when delayed writes are enabled");
        }
        this.maxSize = maxSize;
        this.preemptiveFlushSize = preemptiveFlushSize;
        this.delayNanos = TimeUnit.MILLISECONDS.toNanos(delayMillis);
        this.monitor = monitor;
    }

    void put(PendingSegment value) throws InterruptedException {
        Objects.requireNonNull(value);
        long start = System.nanoTime();
        try {
            putInternal(value);
        } finally {
            // Include lock acquisition and interrupted attempts, not just capacity waits.
            monitor.recordEnqueue(System.nanoTime() - start);
        }
    }

    private void putInternal(PendingSegment value) throws InterruptedException {
        lock.lockInterruptibly();
        try {
            checkAcceptingWrites();
            if (entries.size() == maxSize) {
                monitor.recordQueueFull();
            }
            while (entries.size() == maxSize) {
                notFull.await();
                checkAcceptingWrites();
            }
            enqueue(value, false);
        } finally {
            lock.unlock();
        }
    }

    /** Timed insertion of shutdown control entries, excluded from segment metrics. */
    boolean offer(PendingSegment value, long timeout, TimeUnit unit) throws InterruptedException {
        Objects.requireNonNull(value);
        long remaining = unit.toNanos(timeout);
        lock.lockInterruptibly();
        try {
            while (entries.size() == maxSize) {
                if (remaining <= 0) {
                    return false;
                }
                remaining = notFull.awaitNanos(remaining);
            }
            enqueue(value, true);
            return true;
        } finally {
            lock.unlock();
        }
    }

    private void enqueue(PendingSegment value, boolean control) {
        int bytes = control ? 0 : value.buffer.remaining();
        entries.addLast(new Entry(value, bytes, control));
        if (!control) {
            monitor.recordQueued(bytes);
        }
        // Wake all writers: several entries may be eligible, and shutdown uses one sentinel per writer.
        changed.signalAll();
    }

    PendingSegment take() throws InterruptedException {
        lock.lockInterruptibly();
        try {
            while (true) {
                Entry head = entries.peekFirst();
                if (head == null) {
                    changed.await();
                    continue;
                }
                long residence = System.nanoTime() - head.enqueuedNanos;
                long remaining = delayNanos - residence;
                if (draining || remaining <= 0 || entries.size() >= preemptiveFlushSize) {
                    entries.removeFirst();
                    if (!head.control) {
                        monitor.recordDequeued(head.bytes, residence,
                                draining ? ReleaseReason.SHUTDOWN : remaining <= 0 ? ReleaseReason.AGE : ReleaseReason.PRESSURE);
                    }
                    notFull.signal();
                    return head.value;
                }
                changed.awaitNanos(remaining);
            }
        } finally {
            lock.unlock();
        }
    }

    void startDraining() {
        lock.lock();
        try {
            draining = true;
            changed.signalAll();
            notFull.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /** Discard entries left after stopping the writers, without counting them as processed releases. */
    List<PendingSegment> discardPending() {
        lock.lock();
        try {
            draining = true;
            List<PendingSegment> discarded = new ArrayList<>();
            Entry entry;
            while ((entry = entries.pollFirst()) != null) {
                if (!entry.control) {
                    monitor.recordRemovedFromQueue(entry.bytes);
                    discarded.add(entry.value);
                }
            }
            notFull.signalAll();
            return discarded;
        } finally {
            lock.unlock();
        }
    }

    private void checkAcceptingWrites() {
        if (draining) {
            throw new IllegalStateException("Pending write queue is shutting down");
        }
    }

    int size() {
        lock.lock();
        try {
            return entries.size();
        } finally {
            lock.unlock();
        }
    }
}
