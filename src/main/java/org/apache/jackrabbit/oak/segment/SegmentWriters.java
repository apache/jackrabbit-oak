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

import javax.annotation.Nonnull;

import com.google.common.base.Supplier;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.http.HttpStore;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;

public final class SegmentWriters {
    private SegmentWriters() {}

    @Nonnull
    public static SegmentWriter pooledSegmentWriter(@Nonnull FileStore store,
                                                    @Nonnull SegmentVersion version,
                                                    @Nonnull String name,
                                                    @Nonnull Supplier<Integer> generation) {
        return new SegmentWriter(store, store.getReader(), store.getBlobStore(), store.getTracker(),
                new SegmentBufferWriterPool(store, store.getTracker(), store.getReader(), version, name, generation));
    }

    @Nonnull
    public static SegmentWriter pooledSegmentWriter(@Nonnull MemoryStore store,
                                                    @Nonnull SegmentVersion version,
                                                    @Nonnull String name,
                                                    @Nonnull Supplier<Integer> generation) {
        return new SegmentWriter(store, store.getReader(), store.getBlobStore(), store.getTracker(),
                new SegmentBufferWriterPool(store, store.getTracker(), store.getReader(), version, name, generation));
    }

    @Nonnull
    public static SegmentWriter pooledSegmentWriter(@Nonnull HttpStore store,
                                                    @Nonnull SegmentVersion version,
                                                    @Nonnull String name,
                                                    @Nonnull Supplier<Integer> generation) {
        return new SegmentWriter(store, store.getReader(), store.getBlobStore(), store.getTracker(),
                new SegmentBufferWriterPool(store, store.getTracker(), store.getReader(), version, name, generation));
    }

    @Nonnull
    public static SegmentWriter segmentWriter(@Nonnull FileStore store,
                                              @Nonnull SegmentVersion version,
                                              @Nonnull String name,
                                              int generation) {
        return new SegmentWriter(store, store.getReader(), store.getBlobStore(), store.getTracker(),
                new SegmentBufferWriter(store, store.getTracker(), store.getReader(), version, name, generation));
    }

    @Nonnull
    public static SegmentWriter segmentWriter(@Nonnull MemoryStore store,
                                              @Nonnull SegmentVersion version,
                                              @Nonnull String name,
                                              int generation) {
        return new SegmentWriter(store, store.getReader(), store.getBlobStore(), store.getTracker(),
                new SegmentBufferWriter(store, store.getTracker(), store.getReader(), version, name, generation));
    }

    @Nonnull
    public static SegmentWriter segmentWriter(@Nonnull HttpStore store,
                                              @Nonnull SegmentVersion version,
                                              @Nonnull String name,
                                              int generation) {
        return new SegmentWriter(store, store.getReader(), store.getBlobStore(), store.getTracker(),
                new SegmentBufferWriter(store, store.getTracker(), store.getReader(), version, name, generation));
    }

}
