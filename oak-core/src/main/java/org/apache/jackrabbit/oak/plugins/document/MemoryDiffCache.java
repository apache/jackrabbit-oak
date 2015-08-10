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

import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;

import com.google.common.cache.Cache;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An in-memory diff cache implementation.
 */
public class MemoryDiffCache implements DiffCache {

    /**
     * Diff cache.
     *
     * Key: PathRev, value: StringValue
     */
    protected final Cache<PathRev, StringValue> diffCache;
    protected final CacheStats diffCacheStats;


    protected MemoryDiffCache(DocumentMK.Builder builder) {
        diffCache = builder.buildMemoryDiffCache();
        diffCacheStats = new CacheStats(diffCache, "Document-MemoryDiff",
                builder.getWeigher(), builder.getMemoryDiffCacheSize());
    }

    @CheckForNull
    @Override
    public String getChanges(@Nonnull Revision from,
                             @Nonnull Revision to,
                             @Nonnull String path,
                             final @Nullable Loader loader) {
        PathRev key = diffCacheKey(path, from, to);
        StringValue diff;
        if (loader == null) {
            diff = diffCache.getIfPresent(key);
        } else {
            try {
                diff = diffCache.get(key, new Callable<StringValue>() {
                    @Override
                    public StringValue call() throws Exception {
                        return new StringValue(loader.call());
                    }
                });
            } catch (ExecutionException e) {
                // try again with loader directly
                diff = new StringValue(loader.call());
            }
        }
        return diff != null ? diff.toString() : null;
    }

    @Nonnull
    @Override
    public Entry newEntry(@Nonnull Revision from,
                          @Nonnull Revision to) {
        return new MemoryEntry(from, to);
    }

    @Nonnull
    @Override
    public Iterable<CacheStats> getStats() {
        return Collections.singleton(diffCacheStats);
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
        public boolean done() {
            return true;
        }
    }

    private static PathRev diffCacheKey(@Nonnull String path,
                                        @Nonnull Revision from,
                                        @Nonnull Revision to) {
        return new PathRev(from + path, to);
    }

}
