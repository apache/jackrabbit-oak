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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.Thread.currentThread;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Monitor;
import com.google.common.util.concurrent.Monitor.Guard;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;

/**
 * This {@link WriteOperationHandler} uses a pool of {@link SegmentBufferWriter}s,
 * which it passes to its {@link #execute(WriteOperation) execute} method.
 * <p>
 * Instances of this class are thread safe.
 */
public class SegmentBufferWriterPool implements WriteOperationHandler {

    /**
     * Monitor protecting the state of this pool. Neither of {@link #writers},
     * {@link #borrowed} and {@link #disposed} must be modified without owning
     * this monitor.
     */
    private final Monitor poolMonitor = new Monitor(true);

    /**
     * Pool of current writers that are not in use
     */
    private final Map<Object, SegmentBufferWriter> writers = newHashMap();

    /**
     * Writers that are currently in use
     */
    private final Set<SegmentBufferWriter> borrowed = newHashSet();

    /**
     * Retired writers that have not yet been flushed
     */
    private final Set<SegmentBufferWriter> disposed = newHashSet();

    @Nonnull
    private final SegmentIdProvider idProvider;

    @Nonnull
    private final SegmentReader reader;

    @Nonnull
    private final Supplier<GCGeneration> gcGeneration;

    @Nonnull
    private final String wid;

    private short writerId = -1;

    public SegmentBufferWriterPool(
            @Nonnull SegmentIdProvider idProvider,
            @Nonnull SegmentReader reader,
            @Nonnull String wid,
            @Nonnull Supplier<GCGeneration> gcGeneration) {
        this.idProvider = checkNotNull(idProvider);
        this.reader = checkNotNull(reader);
        this.wid = checkNotNull(wid);
        this.gcGeneration = checkNotNull(gcGeneration);
    }

    @Nonnull
    @Override
    public RecordId execute(@Nonnull WriteOperation writeOperation) throws IOException {
        SegmentBufferWriter writer = borrowWriter(currentThread());
        try {
            return writeOperation.execute(writer);
        } finally {
            returnWriter(currentThread(), writer);
        }
    }

    @Override
    public void flush(@Nonnull SegmentStore store) throws IOException {
        List<SegmentBufferWriter> toFlush = newArrayList();
        List<SegmentBufferWriter> toReturn = newArrayList();

        poolMonitor.enter();
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
            poolMonitor.leave();
        }

        // Wait for the return of the borrowed writers. This is the
        // case once all of them appear in the disposed set.
        if (safeEnterWhen(poolMonitor, allReturned(toReturn))) {
            try {
                // Collect all disposed writers and clear the list to mark them
                // as flushed.
                toFlush.addAll(toReturn);
                disposed.removeAll(toReturn);
            } finally {
                poolMonitor.leave();
            }
        }

        // Call flush from outside the pool monitor to avoid potential
        // deadlocks of that method calling SegmentStore.writeSegment
        for (SegmentBufferWriter writer : toFlush) {
            writer.flush(store);
        }
    }

    /**
     * Create a {@code Guard} that is satisfied if and only if {@link #disposed}
     * contains all items in {@code toReturn}
     */
    @Nonnull
    private Guard allReturned(final List<SegmentBufferWriter> toReturn) {
        return new Guard(poolMonitor) {

            @Override
            public boolean isSatisfied() {
                return disposed.containsAll(toReturn);
            }

        };
    }

    /**
     * Same as {@code monitor.enterWhen(guard)} but copes with that pesky {@code
     * InterruptedException} by catching it and setting this thread's
     * interrupted flag.
     */
    private static boolean safeEnterWhen(Monitor monitor, Guard guard) {
        try {
            monitor.enterWhen(guard);
            return true;
        } catch (InterruptedException ignore) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Return a writer from the pool by its {@code key}. This method may return
     * a fresh writer at any time. Callers need to return a writer before
     * borrowing it again. Failing to do so leads to undefined behaviour.
     */
    private SegmentBufferWriter borrowWriter(Object key) {
        poolMonitor.enter();
        try {
            SegmentBufferWriter writer = writers.remove(key);
            if (writer == null) {
                writer = new SegmentBufferWriter(
                        idProvider,
                        reader,
                        getWriterId(wid),
                        gcGeneration.get()
                );
            } else if (!writer.getGCGeneration().equals(gcGeneration.get())) {
                disposed.add(writer);
                writer = new SegmentBufferWriter(
                        idProvider,
                        reader,
                        getWriterId(wid),
                        gcGeneration.get()
                );
            }
            borrowed.add(writer);
            return writer;
        } finally {
            poolMonitor.leave();
        }
    }

    /**
     * Return a writer to the pool using the {@code key} that was used to borrow
     * it.
     */
    private void returnWriter(Object key, SegmentBufferWriter writer) {
        poolMonitor.enter();
        try {
            if (borrowed.remove(writer)) {
                checkState(writers.put(key, writer) == null);
            } else {
                // Defer flush this writer as it was borrowed while flush() was called.
                disposed.add(writer);
            }
        } finally {
            poolMonitor.leave();
        }
    }

    private String getWriterId(String wid) {
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
