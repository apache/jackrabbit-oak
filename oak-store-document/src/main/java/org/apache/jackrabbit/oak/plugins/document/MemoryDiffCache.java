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

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.cache.CacheValue;
import org.apache.jackrabbit.oak.plugins.document.util.StringValue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import com.google.common.cache.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

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
    protected final Cache<CacheValue, StringValue> diffCache;
    protected final CacheStats diffCacheStats;


    protected MemoryDiffCache(DocumentNodeStoreBuilder<?> builder) {
        diffCache = builder.buildMemoryDiffCache();
        diffCacheStats = new CacheStats(diffCache, "Document-MemoryDiff",
                builder.getWeigher(), builder.getMemoryDiffCacheSize());
    }

    @Nullable
    @Override
    public String getChanges(@NotNull final RevisionVector from,
                             @NotNull final RevisionVector to,
                             @NotNull final Path path,
                             @Nullable final Loader loader) {
        Key key = new Key(path, from, to);
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

    @NotNull
    @Override
    public Entry newEntry(@NotNull RevisionVector from,
                          @NotNull RevisionVector to,
                          boolean local /*ignored*/) {
        return new MemoryEntry(from, to);
    }

    @NotNull
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
        public void append(@NotNull Path path, @NotNull String changes) {
            Key key = new Key(path, from, to);
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
    private boolean isUnchanged(@NotNull final RevisionVector from,
                                @NotNull final RevisionVector to,
                                @NotNull final Path path) {
        Path parent = path.getParent();
        return parent != null
                && isChildUnchanged(from, to, parent, path.getName());
    }

    private boolean isChildUnchanged(@NotNull final RevisionVector from,
                                     @NotNull final RevisionVector to,
                                     @NotNull final Path parent,
                                     @NotNull final String name) {
        Key parentKey = new Key(parent, from, to);
        StringValue parentCachedEntry = diffCache.getIfPresent(parentKey);
        boolean unchanged;
        if (parentCachedEntry == null) {
            if (parent.getParent() == null) {
                // reached root and we don't know whether name
                // changed between from and to
                unchanged = false;
            } else {
                // recurse down
                unchanged = isChildUnchanged(from, to,
                        parent.getParent(), parent.getName());
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

    public static final class Key implements CacheValue, Comparable<Key> {

        private final Path path;

        private final RevisionVector from;

        private final RevisionVector to;

        public Key(@NotNull Path path,
                   @NotNull RevisionVector from,
                   @NotNull RevisionVector to) {
            this.path = checkNotNull(path);
            this.from = checkNotNull(from);
            this.to = checkNotNull(to);
        }

        @NotNull
        public Path getPath() {
            return path;
        }

        @NotNull
        public RevisionVector getFromRevision() {
            return from;
        }

        @NotNull
        public RevisionVector getToRevision() {
            return to;
        }

        public String asString() {
            return toString();
        }

        public static Key fromString(@NotNull String s) {
            int idx1 = s.indexOf('/');
            int idx2 = s.lastIndexOf('@');
            if (idx1 == -1 || idx2 == -1) {
                throw new IllegalArgumentException("Malformed "
                        + MemoryDiffCache.Key.class.getSimpleName() + ": " + s);
            }
            return new Key(
                    Path.fromString(s.substring(idx1, idx2)),
                    RevisionVector.fromString(s.substring(0, idx1)),
                    RevisionVector.fromString(s.substring(idx2 + 1))
            );
        }

        @Override
        public int getMemory() {
            return 32 + path.getMemory() + from.getMemory() + to.getMemory();
        }

        @Override
        public int compareTo(@NotNull MemoryDiffCache.Key other) {
            if (this == other) {
                return 0;
            }
            int compare = this.from.compareTo(other.from);
            if (compare != 0) {
                return compare;
            }
            compare = this.path.compareTo(other.path);
            if (compare != 0) {
                return compare;
            }
            return this.to.compareTo(other.to);
        }

        @Override
        public String toString() {
            int dim = from.getDimensions() + to.getDimensions();
            StringBuilder sb = new StringBuilder(path.length() + (Revision.REV_STRING_APPROX_SIZE + 1) * dim);
            from.toStringBuilder(sb);
            path.toStringBuilder(sb).append('@');
            to.toStringBuilder(sb);
            return sb.toString();
        }

        @Override
        public int hashCode() {
            int h = 17;
            h = 37 * h + path.hashCode();
            h = 37 * h + from.hashCode();
            h = 37 * h + to.hashCode();
            return h;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            } else if (obj instanceof Key) {
                Key other = (Key) obj;
                return from.equals(other.from)
                        && to.equals(other.to)
                        && path.equals(other.path);
            }
            return false;
        }
    }
}
