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
package org.apache.jackrabbit.oak.plugins.document;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Iterables;

import org.apache.jackrabbit.oak.cache.CacheStats;

/**
 * Implements a tiered diff cache which consists of a {@link LocalDiffCache} and
 * a {@link MemoryDiffCache}.
 */
class TieredDiffCache extends DiffCache {

    private final DiffCache localCache;
    private final DiffCache memoryCache;

    TieredDiffCache(DocumentNodeStoreBuilder<?> builder) {
        this.localCache = new LocalDiffCache(builder);
        this.memoryCache = new MemoryDiffCache(builder);
    }

    @Override
    public String getChanges(@Nonnull RevisionVector from,
                             @Nonnull RevisionVector to,
                             @Nonnull String path,
                             @Nullable Loader loader) {
        // check local first without loader
        String changes = localCache.getChanges(from, to, path, null);
        if (changes != null) {
            return changes;
        }
        return memoryCache.getChanges(from, to, path, loader);
    }

    /**
     * Creates a new entry in the {@link LocalDiffCache} for local changes
     * and {@link MemoryDiffCache} for external changes
     *
     * @param from the from revision.
     * @param to the to revision.
     * @return the new entry.
     */
    @Nonnull
    @Override
    public Entry newEntry(@Nonnull RevisionVector from, @Nonnull RevisionVector to, boolean local) {
        if (local) {
            return localCache.newEntry(from, to, true);
        } else {
            return memoryCache.newEntry(from, to, false);
        }
    }

    @Nonnull
    @Override
    public Iterable<CacheStats> getStats() {
        return Iterables.concat(localCache.getStats(), memoryCache.getStats());
    }
}
