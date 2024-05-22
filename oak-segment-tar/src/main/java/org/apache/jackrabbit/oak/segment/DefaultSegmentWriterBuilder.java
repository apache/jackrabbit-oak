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

import static org.apache.jackrabbit.guava.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.segment.SegmentBufferWriterPool.PoolType;

import org.apache.jackrabbit.guava.common.base.Supplier;
import org.apache.jackrabbit.guava.common.base.Suppliers;
import org.apache.jackrabbit.oak.segment.WriterCacheManager.Empty;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.file.tar.GCGeneration;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Builder for building {@link DefaultSegmentWriter} instances.
 * The returned instances are thread-safe if {@link #withWriterPool(PoolType)}
 * was specified and <em>not</em> thread-safe if {@link #withoutWriterPool()}
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
public final class DefaultSegmentWriterBuilder {

    @NotNull
    private final String name;

    @NotNull
    private Supplier<GCGeneration> generation = () -> GCGeneration.NULL;

    private PoolType poolType = null;

    @NotNull
    private WriterCacheManager cacheManager = new WriterCacheManager.Default();

    private DefaultSegmentWriterBuilder(@NotNull String name) {
        this.name = checkNotNull(name);
    }

    /**
     * Set the {@code name} of this builder. This name will appear in the segment's
     * meta data.
     */
    @NotNull
    public static DefaultSegmentWriterBuilder defaultSegmentWriterBuilder(@NotNull String name) {
        return new DefaultSegmentWriterBuilder(name);
    }

    /**
     * Specify the {@code generation} for the segment written by the returned
     * segment writer.
     * <p>
     * If {@link #withoutWriterPool()} was specified all segments will be written
     * at the generation that {@code generation.get()} returned at the time
     * any of the {@code build()} methods is called.
     * If {@link #withWriterPool(PoolType)} ()} was specified, segments will be written
     * at the generation that {@code generation.get()} returns when a new segment
     * is created by the returned writer.
     */
    @NotNull
    public DefaultSegmentWriterBuilder withGeneration(@NotNull Supplier<GCGeneration> generation) {
        this.generation = checkNotNull(generation);
        return this;
    }

    /**
     * Specify the {@code generation} for the segment written by the returned
     * segment writer.
     */
    @NotNull
    public DefaultSegmentWriterBuilder withGeneration(@NotNull GCGeneration generation) {
        this.generation = () -> checkNotNull(generation);
        return this;
    }

    @NotNull
    public DefaultSegmentWriterBuilder withWriterPool() {
        return withWriterPool(PoolType.GLOBAL);
    }

    /**
     * Create a {@code SegmentWriter} backed by a {@link SegmentBufferWriterPool}.
     * The returned instance is thread safe.
     */
    @NotNull
    public DefaultSegmentWriterBuilder withWriterPool(PoolType writerType) {
        this.poolType = writerType;
        return this;
    }

    /**
     * Create a {@code SegmentWriter} backed by a {@link SegmentBufferWriter}.
     * The returned instance is <em>not</em> thread safe.
     */
    @NotNull
    public DefaultSegmentWriterBuilder withoutWriterPool() {
        this.poolType = null;
        return this;
    }

    /**
     * Specify the {@code cacheManager} used by the returned writer.
     */
    @NotNull
    public DefaultSegmentWriterBuilder with(WriterCacheManager cacheManager) {
        this.cacheManager = checkNotNull(cacheManager);
        return this;
    }

    /**
     * Specify that the returned writer should not use a cache.
     * @see #with(WriterCacheManager)
     */
    @NotNull
    public DefaultSegmentWriterBuilder withoutCache() {
        this.cacheManager = Empty.INSTANCE;
        return this;
    }

    /**
     * Build a {@code SegmentWriter} for a {@code FileStore}.
     */
    @NotNull
    public DefaultSegmentWriter build(@NotNull FileStore store) {
        return new DefaultSegmentWriter(
                checkNotNull(store),
                store.getReader(),
                store.getSegmentIdProvider(),
                store.getBlobStore(),
                cacheManager,
                createWriter(store, poolType),
                store.getBinariesInlineThreshold()
        );
    }

    /**
     * Build a {@code SegmentWriter} for a {@code ReadOnlyFileStore}.
     * Attempting to write to the returned writer will cause a
     * {@code UnsupportedOperationException} to be thrown.
     */
    @NotNull
    public DefaultSegmentWriter build(@NotNull ReadOnlyFileStore store) {
        return new DefaultSegmentWriter(
                checkNotNull(store),
                store.getReader(),
                store.getSegmentIdProvider(),
                store.getBlobStore(),
                cacheManager,
                new WriteOperationHandler() {

                    @Override
                    @NotNull
                    public GCGeneration getGCGeneration() {
                        throw new UnsupportedOperationException("Cannot write to read-only store");
                    }

                    @NotNull
                    @Override
                    public RecordId execute(@NotNull GCGeneration gcGeneration,
                                            @NotNull WriteOperation writeOperation) {
                        throw new UnsupportedOperationException("Cannot write to read-only store");
                    }

                    @Override
                    public void flush(@NotNull SegmentStore store) {
                        throw new UnsupportedOperationException("Cannot write to read-only store");
                    }
                },
                store.getBinariesInlineThreshold()
        );
    }

    /**
     * Build a {@code SegmentWriter} for a {@code MemoryStore}.
     */
    @NotNull
    public DefaultSegmentWriter build(@NotNull MemoryStore store) {
        return new DefaultSegmentWriter(
                checkNotNull(store),
                store.getReader(),
                store.getSegmentIdProvider(),
                store.getBlobStore(),
                cacheManager,
                createWriter(store, poolType),
                Segment.MEDIUM_LIMIT
        );
    }

    @NotNull
    private WriteOperationHandler createWriter(@NotNull FileStore store, @Nullable PoolType poolType) {
        return createWriter(store.getSegmentIdProvider(), store.getReader(), poolType);
    }

    @NotNull
    private WriteOperationHandler createWriter(@NotNull MemoryStore store, @Nullable PoolType poolType) {
        return createWriter(store.getSegmentIdProvider(), store.getReader(), poolType);
    }

    @NotNull
    private WriteOperationHandler createWriter(@NotNull SegmentIdProvider idProvider, @NotNull SegmentReader reader, @Nullable PoolType poolType) {
        if (poolType == null) {
            return new SegmentBufferWriter(idProvider, reader, name, generation.get());
        } else {
            return SegmentBufferWriterPool.factory(idProvider, reader, name, generation).newPool(poolType);
        }
    }
}
