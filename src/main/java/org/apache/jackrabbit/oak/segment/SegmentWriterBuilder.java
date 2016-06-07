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
import static java.lang.Integer.getInteger;
import static org.apache.jackrabbit.oak.segment.SegmentVersion.LATEST_VERSION;

import javax.annotation.Nonnull;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.http.HttpStore;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;

/**
 * Builder for building {@link SegmentWriter} instances.
 * The returned instances are thread safe if {@link #withWriterPool()}
 * was specified and <em>not</em> thread sage if {@link #withoutWriterPool()}
 * was specified (default).
 */
public final class SegmentWriterBuilder {
    private static final int STRING_RECORDS_CACHE_SIZE = getInteger(
            "oak.segment.writer.stringsCacheSize", 15000);

    private static final int TPL_RECORDS_CACHE_SIZE = getInteger(
            "oak.segment.writer.templatesCacheSize", 3000);

    @Nonnull
    private final String name;

    @Nonnull
    private SegmentVersion version = LATEST_VERSION;

    @Nonnull
    private Supplier<Integer> generation = Suppliers.ofInstance(0);

    private boolean pooled = false;

    @Nonnull
    private WriterCacheManager cacheManager = WriterCacheManager.Default.create(
        STRING_RECORDS_CACHE_SIZE <= 0
            ? RecordCache.<String>empty()
            : RecordCache.<String>factory(STRING_RECORDS_CACHE_SIZE),
        TPL_RECORDS_CACHE_SIZE <= 0
            ? RecordCache.<Template>empty()
            : RecordCache.<Template>factory(TPL_RECORDS_CACHE_SIZE),
        // FIXME OAK-4277: Finalise de-duplication caches: make sizes and depth configurable
        NodeCache.factory(1000000, 20));


    private SegmentWriterBuilder(@Nonnull String name) { this.name = checkNotNull(name); }

    @Nonnull
    public static SegmentWriterBuilder segmentWriterBuilder(@Nonnull String name) {
        return new SegmentWriterBuilder(name);
    }

    @Nonnull
    public SegmentWriterBuilder with(@Nonnull SegmentVersion version) {
        this.version = checkNotNull(version);
        return this;
    }

    /**
     * Specify the {@code generation} for the segment written by the returned
     * segment writer.
     * <p>
     * If {@link #withoutWriterPool()} was specified all segments will be written
     * at the generation that {@code generation.get()} returned at the time
     * any of the {@code build()} methods is called.
     * If {@link #withWriterPool()} was specified a segments will be written
     * at the generation that {@code generation.get()} returns when a new segment
     * is created by the returned writer.
     */
    @Nonnull
    public SegmentWriterBuilder withGeneration(@Nonnull Supplier<Integer> generation) {
        this.generation = checkNotNull(generation);
        return this;
    }

    @Nonnull
    public SegmentWriterBuilder withGeneration(int generation) {
        this.generation = Suppliers.ofInstance(generation);
        return this;
    }

    /**
     * Create a {@code SegmentWriter} backed by a {@link SegmentBufferWriterPool}.
     * The returned instance is thread safe.
     */
    @Nonnull
    public SegmentWriterBuilder withWriterPool() {
        this.pooled = true;
        return this;
    }

    /**
     * Create a {@code SegmentWriter} backed by a {@link SegmentBufferWriter}.
     * The returned instance is <em>not</em> thread safe.
     */
    @Nonnull
    public SegmentWriterBuilder withoutWriterPool() {
        this.pooled = false;
        return this;
    }

    @Nonnull
    public SegmentWriterBuilder with(WriterCacheManager cacheManager) {
        this.cacheManager = checkNotNull(cacheManager);
        return this;
    }

    @Nonnull
    public SegmentWriterBuilder withoutCache() {
        this.cacheManager = WriterCacheManager.Empty.create();
        return this;
    }

    @Nonnull
    public SegmentWriter build(@Nonnull FileStore store) {
        return new SegmentWriter(checkNotNull(store), store.getReader(),
                store.getBlobStore(), store.getTracker(), cacheManager, createWriter(store, pooled));
    }

    @Nonnull
    public SegmentWriter build(@Nonnull MemoryStore store) {
        return new SegmentWriter(checkNotNull(store), store.getReader(),
                store.getBlobStore(), store.getTracker(), cacheManager, createWriter(store, pooled));
    }

    @Nonnull
    public SegmentWriter build(@Nonnull HttpStore store) {
        return new SegmentWriter(checkNotNull(store), store.getReader(),
                store.getBlobStore(), store.getTracker(), cacheManager, createWriter(store, pooled));
    }

    @Nonnull
    private WriteOperationHandler createWriter(@Nonnull FileStore store, boolean pooled) {
        if (pooled) {
            return new SegmentBufferWriterPool(store,
                    store.getTracker(), store.getReader(), version, name, generation);
        } else {
            return new SegmentBufferWriter(store,
                    store.getTracker(), store.getReader(), version, name, generation.get());
        }
    }

    @Nonnull
    private WriteOperationHandler createWriter(@Nonnull MemoryStore store, boolean pooled) {
        if (pooled) {
            return new SegmentBufferWriterPool(store,
                    store.getTracker(), store.getReader(), version, name, generation);
        } else {
            return new SegmentBufferWriter(store,
                    store.getTracker(), store.getReader(), version, name, generation.get());
        }
    }

    @Nonnull
    private WriteOperationHandler createWriter(@Nonnull HttpStore store, boolean pooled) {
        if (pooled) {
            return new SegmentBufferWriterPool(store,
                    store.getTracker(), store.getReader(), version, name, generation);
        } else {
            return new SegmentBufferWriter(store,
                    store.getTracker(), store.getReader(), version, name, generation.get());
        }
    }

}
