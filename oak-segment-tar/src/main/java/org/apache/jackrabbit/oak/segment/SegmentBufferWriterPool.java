/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.segment;

import static java.util.Objects.requireNonNull;

import static java.lang.Thread.currentThread;

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCGeneration;
import org.jetbrains.annotations.NotNull;

/**
 * This {@link WriteOperationHandler} uses a pool of {@link SegmentBufferWriter}s,
 * which it passes to its {@link #execute(GCGeneration, WriteOperation) execute} method.
 * <p>
 * Instances of this class are thread safe.
 */
public abstract class SegmentBufferWriterPool implements WriteOperationHandler {
    @NotNull
    private final SegmentIdProvider idProvider;

    @NotNull
    private final Supplier<GCGeneration> gcGeneration;

    @NotNull
    private final String wid;

    private short writerId = -1;

    private SegmentBufferWriterPool(
            @NotNull SegmentIdProvider idProvider,
            @NotNull String wid,
            @NotNull Supplier<GCGeneration> gcGeneration) {
        this.idProvider = requireNonNull(idProvider);
        this.wid = requireNonNull(wid);
        this.gcGeneration = requireNonNull(gcGeneration);
    }

    public enum PoolType {
        GLOBAL,
        THREAD_SPECIFIC;
    }

    public static class SegmentBufferWriterPoolFactory {
        @NotNull
        private final SegmentIdProvider idProvider;
        @NotNull
        private final String wid;
        @NotNull
        private final Supplier<GCGeneration> gcGeneration;

        private SegmentBufferWriterPoolFactory(
                @NotNull SegmentIdProvider idProvider,
                @NotNull String wid,
                @NotNull Supplier<GCGeneration> gcGeneration) {
            this.idProvider = requireNonNull(idProvider);
            this.wid = requireNonNull(wid);
            this.gcGeneration = requireNonNull(gcGeneration);
        }

        @NotNull
        public SegmentBufferWriterPool newPool(@NotNull SegmentBufferWriterPool.PoolType poolType) {
            switch (poolType) {
                case GLOBAL:
                    return new GlobalSegmentBufferWriterPool(idProvider, wid, gcGeneration);
                case THREAD_SPECIFIC:
                    return new ThreadSpecificSegmentBufferWriterPool(idProvider, wid, gcGeneration);
                default:
                    throw new IllegalArgumentException("Unknown writer pool type.");
            }
        }
    }

    public static SegmentBufferWriterPoolFactory factory(
            @NotNull SegmentIdProvider idProvider,
            @NotNull String wid,
            @NotNull Supplier<GCGeneration> gcGeneration) {
        return new SegmentBufferWriterPoolFactory(idProvider, wid, gcGeneration);
    }

    private static class ThreadSpecificSegmentBufferWriterPool extends SegmentBufferWriterPool {
        /**
         * Read write lock protecting the state of this pool. Multiple threads can access their writers in parallel,
         * acquiring the read lock. The writer lock is needed for the flush operation since it requires none
         * of the writers to be in use.
         */
        private final ReadWriteLock lock = new ReentrantReadWriteLock(true);

        /**
         * Pool of writers. Every thread is assigned a unique writer per GC generation, therefore only requiring
         * a concurrent map to synchronize access to them.
         */
        private final ConcurrentMap<Object, SegmentBufferWriter> writers = new ConcurrentHashMap<>();

        public ThreadSpecificSegmentBufferWriterPool(
                @NotNull SegmentIdProvider idProvider,
                @NotNull String wid,
                @NotNull Supplier<GCGeneration> gcGeneration) {
            super(idProvider, wid, gcGeneration);
        }

        @NotNull
        @Override
        public RecordId execute(@NotNull GCGeneration gcGeneration,
                                @NotNull WriteOperation writeOperation)
                throws IOException {
            lock.readLock().lock();
            SegmentBufferWriter writer = getWriter(currentThread(), gcGeneration);
            try {
                return writeOperation.execute(writer);
            } finally {
                lock.readLock().unlock();
            }
        }

        @Override
        public void flush(@NotNull SegmentStore store) throws IOException {
            lock.writeLock().lock();
            try {
                for (SegmentBufferWriter writer : writers.values()) {
                    writer.flush(store);
                }
                writers.clear();
            } finally {
                lock.writeLock().unlock();
            }
        }

        @NotNull
        private SegmentBufferWriter getWriter(@NotNull Thread thread, @NotNull GCGeneration gcGeneration) {
            SimpleImmutableEntry<?,?> key = new SimpleImmutableEntry<>(thread, gcGeneration);
            return writers.computeIfAbsent(key, f -> newWriter(gcGeneration));
        }
    }

    private static class GlobalSegmentBufferWriterPool extends SegmentBufferWriterPool {
        /**
         * Lock protecting the state of this pool. Neither of {@link #writers},
         * {@link #borrowed} and {@link #disposed} must be modified without owning
         * this lock.
         */
        private final ReentrantLock lock = new ReentrantLock(true);

        /**
         * Condition signaled when a writer is returned to {@link #disposed},
         * allowing {@link #flush} to proceed once all borrowed writers are returned.
         */
        private final Condition writersReturned = lock.newCondition();

        /**
         * Pool of current writers that are not in use
         */
        private final Map<Object, SegmentBufferWriter> writers = new HashMap<>();

        /**
         * Writers that are currently in use
         */
        private final Set<SegmentBufferWriter> borrowed = new HashSet<>();

        /**
         * Retired writers that have not yet been flushed
         */
        private final Set<SegmentBufferWriter> disposed = new HashSet<>();

        public GlobalSegmentBufferWriterPool(
                @NotNull SegmentIdProvider idProvider,
                @NotNull String wid,
                @NotNull Supplier<GCGeneration> gcGeneration) {
            super(idProvider, wid, gcGeneration);
        }

        @NotNull
        @Override
        public RecordId execute(@NotNull GCGeneration gcGeneration, @NotNull WriteOperation writeOperation)
                throws IOException {
            SimpleImmutableEntry<?,?> key = new SimpleImmutableEntry<>(currentThread(), gcGeneration);
            SegmentBufferWriter writer = borrowWriter(key, gcGeneration);
            try {
                return writeOperation.execute(writer);
            } finally {
                returnWriter(key, writer);
            }
        }

        @Override
        public void flush(@NotNull SegmentStore store) throws IOException {
            List<SegmentBufferWriter> toFlush = new ArrayList<>();
            List<SegmentBufferWriter> toReturn = new ArrayList<>();

            lock.lock();
            try {
                // Collect all writers that are not currently in use and clear
                // the list so they won't get re-used anymore.
                toFlush.addAll(writers.values());
                writers.clear();

                // Collect all borrowed writers, which we need to wait for.
                // Clear the list so they will get disposed once returned.
                toReturn.addAll(borrowed);
                borrowed.clear();
            } finally {
                lock.unlock();
            }

            // Wait for the return of the borrowed writers. This is the
            // case once all of them appear in the disposed set.
            lock.lock();
            try {
                while (!disposed.containsAll(toReturn)) {
                    try {
                        writersReturned.await();
                    } catch (InterruptedException e) {
                        currentThread().interrupt();
                        break;
                    }
                }
                if (disposed.containsAll(toReturn)) {
                    // Collect all disposed writers and clear the list to mark them
                    // as flushed.
                    toFlush.addAll(toReturn);
                    disposed.removeAll(toReturn);
                }
            } finally {
                lock.unlock();
            }

            // Call flush from outside the pool monitor to avoid potential
            // deadlocks of that method calling SegmentStore.writeSegment
            for (SegmentBufferWriter writer : toFlush) {
                writer.flush(store);
            }
        }

        /**
         * Return a writer from the pool by its {@code key}. This method may return
         * a fresh writer at any time. Callers need to return a writer before
         * borrowing it again. Failing to do so leads to undefined behaviour.
         */
        @NotNull
        private SegmentBufferWriter borrowWriter(@NotNull Object key, @NotNull GCGeneration gcGeneration) {
            lock.lock();
            try {
                SegmentBufferWriter writer = writers.remove(key);
                if (writer == null) {
                    writer = newWriter(gcGeneration);
                }
                borrowed.add(writer);
                return writer;
            } finally {
                lock.unlock();
            }
        }

        /**
         * Return a writer to the pool using the {@code key} that was used to borrow
         * it.
         */
        private void returnWriter(Object key, SegmentBufferWriter writer) {
            lock.lock();
            try {
                if (borrowed.remove(writer)) {
                    Validate.checkState(writers.put(key, writer) == null);
                } else {
                    // Defer flush this writer as it was borrowed while flush() was called.
                    disposed.add(writer);
                    writersReturned.signalAll();
                }
            } finally {
                lock.unlock();
            }
        }
    }

    @NotNull
    @Override
    public GCGeneration getGCGeneration() {
        return gcGeneration.get();
    }

    @NotNull
    protected SegmentBufferWriter newWriter(@NotNull GCGeneration gcGeneration) {
        return new SegmentBufferWriter(idProvider, getWriterId(), gcGeneration);
    }

    @NotNull
    protected String getWriterId() {
        if (++writerId > 9999) {
            writerId = 0;
        }
        // Manual padding seems to be fastest here
        if (writerId < 10) {
            return wid + ".000" + writerId;
        } else if (writerId < 100) {
            return wid + ".00" + writerId;
        } else if (writerId < 1000) {
            return wid + ".0" + writerId;
        } else {
            return wid + "." + writerId;
        }
    }
}
