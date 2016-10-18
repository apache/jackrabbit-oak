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

import javax.annotation.Nonnull;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.jackrabbit.oak.segment.WriterCacheManager.Empty;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.http.HttpStore;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;

/**
 * Builder for building {@link SegmentWriter} instances.
 * The returned instances are thread safe if {@link #withWriterPool()}
 * was specified and <em>not</em> thread sage if {@link #withoutWriterPool()}
 * was specified (default).
 * <p>
 * <em>Default:</em> calling one of the {@code build()} methods without previously
 * calling one of the {@code with...()} methods returns a {@code SegmentWriter}
 * as would the following chain of calls:
 * <pre>
     segmentWriterBuilder("name")
        .with(LATEST_VERSION)
        .withGeneration(0)
        .withoutWriterPool()
        .with(new WriterCacheManager.Default())
        .build(store);
 * </pre>
 */
public final class SegmentWriterBuilder {

    @Nonnull
    private final String name;

    @Nonnull
    private Supplier<Integer> generation = Suppliers.ofInstance(0);

    private boolean pooled = false;

    @Nonnull
    private WriterCacheManager cacheManager = new WriterCacheManager.Default();

    private SegmentWriterBuilder(@Nonnull String name) { this.name = checkNotNull(name); }

    /**
     * Set the {@code name} of this builder. This name will appear in the segment's
     * meta data.
     */
    @Nonnull
    public static SegmentWriterBuilder segmentWriterBuilder(@Nonnull String name) {
        return new SegmentWriterBuilder(name);
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

    /**
     * Specify the {@code generation} for the segment written by the returned
     * segment writer.
     */
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

    /**
     * Specify the {@code cacheManager} used by the returned writer.
     */
    @Nonnull
    public SegmentWriterBuilder with(WriterCacheManager cacheManager) {
        this.cacheManager = checkNotNull(cacheManager);
        return this;
    }

    /**
     * Specify that the returned writer should not use a cache.
     * @see #with(WriterCacheManager)
     */
    @Nonnull
    public SegmentWriterBuilder withoutCache() {
        this.cacheManager = Empty.INSTANCE;
        return this;
    }

    /**
     * Build a {@code SegmentWriter} for a {@code FileStore}.
     */
    @Nonnull
    public SegmentWriter build(@Nonnull FileStore store) {
        return new SegmentWriter(
                checkNotNull(store),
                store.getReader(),
                store.getBlobStore(),
                cacheManager,
                createWriter(store, pooled)
        );
    }

    /**
     * Build a {@code SegmentWriter} for a {@code MemoryStore}.
     */
    @Nonnull
    public SegmentWriter build(@Nonnull MemoryStore store) {
        return new SegmentWriter(
                checkNotNull(store),
                store.getReader(),
                store.getBlobStore(),
                cacheManager,
                createWriter(store, pooled)
        );
    }

    /**
     * Build a {@code SegmentWriter} for a {@code HttpStore}.
     */
    @Nonnull
    public SegmentWriter build(@Nonnull HttpStore store) {
        return new SegmentWriter(
                checkNotNull(store),
                store.getReader(),
                store.getBlobStore(),
                cacheManager,
                createWriter(store, pooled)
        );
    }

    @Nonnull
    private WriteOperationHandler createWriter(@Nonnull FileStore store, boolean pooled) {
        if (pooled) {
            return new SegmentBufferWriterPool(
                    store,
                    store.getTracker(),
                    store.getReader(),
                    name,
                    generation
            );
        } else {
            return new SegmentBufferWriter(
                    store,
                    store.getTracker(),
                    store.getReader(),
                    name,
                    generation.get()
            );
        }
    }

    @Nonnull
    private WriteOperationHandler createWriter(@Nonnull MemoryStore store, boolean pooled) {
        if (pooled) {
            return new SegmentBufferWriterPool(
                    store,
                    store.getTracker(),
                    store.getReader(),
                    name,
                    generation
            );
        } else {
            return new SegmentBufferWriter(
                    store,
                    store.getTracker(),
                    store.getReader(),
                    name,
                    generation.get()
            );
        }
    }

    @Nonnull
    private WriteOperationHandler createWriter(@Nonnull HttpStore store, boolean pooled) {
        if (pooled) {
            return new SegmentBufferWriterPool(
                    store,
                    store.getTracker(),
                    store.getReader(),
                    name,
                    generation
            );
        } else {
            return new SegmentBufferWriter(
                    store,
                    store.getTracker(),
                    store.getReader(),
                    name,
                    generation.get()
            );
        }
    }

}
