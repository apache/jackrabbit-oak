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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;

import com.google.common.cache.Cache;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An in-memory diff cache implementation.
 */
class MemoryDiffCache implements DiffCache {

    /**
     * Diff cache.
     *
     * Key: PathRev, value: StringValue
     */
    protected final Cache<CacheValue, StringValue> diffCache;
    protected final CacheStats diffCacheStats;


    MemoryDiffCache(DocumentMK.Builder builder) {
        diffCache = builder.buildCache(builder.getDiffCacheSize());
        diffCacheStats = new CacheStats(diffCache, "Document-Diff",
                builder.getWeigher(), builder.getDiffCacheSize());
    }

    @CheckForNull
    @Override
    public String getChanges(@Nonnull Revision from,
                             @Nonnull Revision to,
                             @Nonnull String path) {
        PathRev key = diffCacheKey(path, from, to);
        StringValue diff = diffCache.getIfPresent(key);
        return diff != null ? diff.toString() : null;
    }

    @Nonnull
    @Override
    public Entry newEntry(@Nonnull Revision from,
                          @Nonnull Revision to) {
        return new MemoryEntry(from, to);
    }

    public CacheStats getDiffCacheStats() {
        return diffCacheStats;
    }

    protected class MemoryEntry implements Entry {

        private final Revision from;
        private final Revision to;

        protected MemoryEntry(Revision from, Revision to) {
            this.from = checkNotNull(from);
            this.to = checkNotNull(to);
        }

        @Override
        public void append(@Nonnull String path, @Nonnull String changes) {
            PathRev key = diffCacheKey(path, from, to);
            diffCache.put(key, new StringValue(changes));
        }

        @Override
        public void done() {
        }
    }

    private static PathRev diffCacheKey(@Nonnull String path,
                                        @Nonnull Revision from,
                                        @Nonnull Revision to) {
        return new PathRev(from + path, to);
    }

}
