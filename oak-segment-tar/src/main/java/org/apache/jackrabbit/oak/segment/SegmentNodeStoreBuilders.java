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

import org.apache.jackrabbit.oak.segment.SegmentNodeStore.SegmentNodeStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.jetbrains.annotations.NotNull;

/**
 * Static factories for creating {@link SegmentNodeBuilder} instances
 * pertaining to specific {@link SegmentStore} instances.
 */
public final class SegmentNodeStoreBuilders {
    private SegmentNodeStoreBuilders() {}

    /**
     * Create a {@code SegmentNodeStoreBuilder} based on a {@code FileStore}.
     */
    @NotNull
    public static SegmentNodeStoreBuilder builder(@NotNull FileStore store) {
        return SegmentNodeStore.builder(store.getRevisions(),
                store.getReader(), store.getWriter(), store.getBlobStore());
    }

    /**
     * Create a {@code SegmentNodeStoreBuilder} based on a {@code MemoryStore}.
     */
    @NotNull
    public static SegmentNodeStoreBuilder builder(@NotNull MemoryStore store) {
        return SegmentNodeStore.builder(store.getRevisions(),
                store.getReader(), store.getWriter(), store.getBlobStore());
    }

    /**
     * Create a {@code SegmentNodeStoreBuilder} based on a {@code ReadOnlyFileStore@}.
     */
    @NotNull
    public static SegmentNodeStoreBuilder builder(@NotNull ReadOnlyFileStore store) {
        return SegmentNodeStore.builder(store.getRevisions(),
                store.getReader(), store.getWriter(), store.getBlobStore());
    }
}
