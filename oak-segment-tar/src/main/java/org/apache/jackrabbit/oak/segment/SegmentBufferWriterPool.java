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

import com.google.common.base.Supplier;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Maps.newConcurrentMap;
import static java.lang.Thread.currentThread;

/**
 * This {@link WriteOperationHandler} uses a pool of {@link SegmentBufferWriter}s,
 * which it passes to its {@link #execute(GCGeneration, WriteOperation) execute} method.
 * <p>
 * Instances of this class are thread safe.
 */
public class SegmentBufferWriterPool implements WriteOperationHandler {
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
    private final ConcurrentMap<Object, SegmentBufferWriter> writers = newConcurrentMap();

    @NotNull
    private final SegmentIdProvider idProvider;

    @NotNull
    private final SegmentReader reader;

    @NotNull
    private final Supplier<GCGeneration> gcGeneration;

    @NotNull
    private final String wid;

    private short writerId = -1;

    public SegmentBufferWriterPool(
            @NotNull SegmentIdProvider idProvider,
            @NotNull SegmentReader reader,
            @NotNull String wid,
            @NotNull Supplier<GCGeneration> gcGeneration) {
        this.idProvider = checkNotNull(idProvider);
        this.reader = checkNotNull(reader);
        this.wid = checkNotNull(wid);
        this.gcGeneration = checkNotNull(gcGeneration);
    }

    @NotNull
    @Override
    public GCGeneration getGCGeneration() {
        return gcGeneration.get();
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

    private SegmentBufferWriter getWriter(@NotNull Thread thread, @NotNull GCGeneration gcGeneration) {
        SimpleImmutableEntry<?,?> key = new SimpleImmutableEntry<>(thread, gcGeneration);
        SegmentBufferWriter writer = writers.get(key);
        if (writer == null) {
             writer = new SegmentBufferWriter(
                    idProvider,
                    reader,
                    getWriterId(wid),
                    gcGeneration
            );
            writers.put(key, writer);
        }
        return writer;
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
