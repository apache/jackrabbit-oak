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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.commons.PathUtils.denotesRoot;
import static org.apache.jackrabbit.oak.commons.PathUtils.getName;
import static org.apache.jackrabbit.oak.commons.PathUtils.getParentPath;

/**
 * An in-memory diff cache implementation.
 */
public class MemoryDiffCache extends DiffCache {

    /**
     * Cache entry Strings with a length more than this limit are not put into
     * the cache.
     */
    static final int CACHE_VALUE_LIMIT = Integer.getInteger(
            "oak.memoryDiffCache.limit", 8 * 1024 * 1024);

    private static final Logger LOG = LoggerFactory.getLogger(MemoryDiffCache.class);

    /**
     * Diff cache.
     *
     * Key: PathRev, value: StringValue
     */
    protected final Cache<PathRev, StringValue> diffCache;
    protected final CacheStats diffCacheStats;


    protected MemoryDiffCache(DocumentNodeStoreBuilder<?> builder) {
        diffCache = builder.buildMemoryDiffCache();
        diffCacheStats = new CacheStats(diffCache, "Document-MemoryDiff",
                builder.getWeigher(), builder.getMemoryDiffCacheSize());
    }

    @CheckForNull
    @Override
    public String getChanges(@Nonnull final RevisionVector from,
                             @Nonnull final RevisionVector to,
                             @Nonnull final String path,
                             @Nullable final Loader loader) {
        PathRev key = diffCacheKey(path, from, to);
        StringValue diff;
        if (loader == null) {
            diff = diffCache.getIfPresent(key);
            if (diff == null && isUnchanged(from, to, path)) {
                diff = new StringValue("");
            }
        } else {
            try {
                diff = diffCache.get(key, new Callable<StringValue>() {
                    @Override
                    public StringValue call() throws Exception {
                        if (isUnchanged(from, to, path)) {
                            return new StringValue("");
                        } else {
                            return new StringValue(loader.call());
                        }
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
    public Entry newEntry(@Nonnull RevisionVector from,
                          @Nonnull RevisionVector to,
                          boolean local /*ignored*/) {
        return new MemoryEntry(from, to);
    }

    @Nonnull
    @Override
    public Iterable<CacheStats> getStats() {
        return Collections.singleton(diffCacheStats);
    }

    protected class MemoryEntry implements Entry {

        private final RevisionVector from;
        private final RevisionVector to;

        protected MemoryEntry(RevisionVector from, RevisionVector to) {
            this.from = checkNotNull(from);
            this.to = checkNotNull(to);
        }

        @Override
        public void append(@Nonnull String path, @Nonnull String changes) {
            PathRev key = diffCacheKey(path, from, to);
            if (changes.length() > CACHE_VALUE_LIMIT) {
                LOG.warn("Not caching entry for {} from {} to {}. Length of changes is {}.",
                        path, from, to, changes.length());
            } else {
                LOG.debug("Adding cache entry for {} from {} to {}", path, from, to);
                diffCache.put(key, new StringValue(changes));
            }
        }

        @Override
        public boolean done() {
            return true;
        }
    }

    private static PathRev diffCacheKey(@Nonnull String path,
                                        @Nonnull RevisionVector from,
                                        @Nonnull RevisionVector to) {
        return new PathRev(from + path, to);
    }

    /**
     * Returns {@code true} if it can be inferred from cache entries on
     * ancestors of the given {@code path} that the node was not changed between
     * the two revisions. This method returns {@code false} if there are no
     * matching cache entries for the given revision range or one of them
     * indicates that the node at the given path may have been modified.
     *
     * @param from the from revision.
     * @param to the to revision.
     * @param path the path of the node to check.
     * @return {@code true} if there are cache entries that indicate the node
     *      at the given path was modified between the two revisions.
     */
    private boolean isUnchanged(@Nonnull final RevisionVector from,
                                @Nonnull final RevisionVector to,
                                @Nonnull final String path) {
        return !denotesRoot(path)
                && isChildUnchanged(from, to, getParentPath(path), getName(path));
    }

    private boolean isChildUnchanged(@Nonnull final RevisionVector from,
                                     @Nonnull final RevisionVector to,
                                     @Nonnull final String parent,
                                     @Nonnull final String name) {
        PathRev parentKey = diffCacheKey(parent, from, to);
        StringValue parentCachedEntry = diffCache.getIfPresent(parentKey);
        boolean unchanged;
        if (parentCachedEntry == null) {
            if (denotesRoot(parent)) {
                // reached root and we don't know whether name
                // changed between from and to
                unchanged = false;
            } else {
                // recurse down
                unchanged = isChildUnchanged(from, to,
                        getParentPath(parent), getName(parent));
            }
        } else {
            unchanged = parseJsopDiff(parentCachedEntry.asString(), new Diff() {
                @Override
                public boolean childNodeAdded(String n) {
                    return !name.equals(n);
                }

                @Override
                public boolean childNodeChanged(String n) {
                    return !name.equals(n);
                }

                @Override
                public boolean childNodeDeleted(String n) {
                    return !name.equals(n);
                }
            });
        }
        return unchanged;
    }
}
