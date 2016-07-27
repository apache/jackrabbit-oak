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
import static com.google.common.base.Suppliers.memoize;
import static com.google.common.collect.Iterators.transform;
import static com.google.common.collect.Maps.newConcurrentMap;
import static java.lang.Integer.getInteger;
import static org.apache.jackrabbit.oak.segment.RecordCache.newRecordCache;

import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.cache.CacheStats;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;

// FIXME OAK-4277: Finalise de-duplication caches
// implement configuration
/**
 * Instances of this class manage the deduplication caches used
 * by the {@link SegmentWriter} to avoid writing multiple copies
 * of the same record. The caches are striped into generations
 * with one generation per gc cycle. This avoids records old
 * generations being reused.
 */
public abstract class WriterCacheManager {

    /**
     * Default size of the string cache.
     * @see #getStringCache(int)
     */
    public static final int DEFAULT_STRING_CACHE_SIZE = getInteger(
            "oak.tar.stringsCacheSize", 15000);

    /**
     * Default size of the template cache.
     * @see #getTemplateCache(int)
     */
    public static final int DEFAULT_TEMPLATE_CACHE_SIZE = getInteger(
            "oak.tar.templatesCacheSize", 3000);

    /**
     * @param generation
     * @return  cache for string records of the given {@code generation}.
     */
    @Nonnull
    public abstract RecordCache<String> getStringCache(int generation);

    /**
     * @param generation
     * @return  cache for template records of the given {@code generation}.
     */
    @Nonnull
    public abstract RecordCache<Template> getTemplateCache(int generation);

    /**
     * @param generation
     * @return  cache for node records of the given {@code generation}.
     */
    @Nonnull
    public abstract NodeCache getNodeCache(int generation);

    /**
     * @return  statistics for the string cache or {@code null} if not available.
     */
    @CheckForNull
    public CacheStatsMBean getStringCacheStats() {
        return null;
    }

    /**
     * @return  statistics for the template cache or {@code null} if not available.
     */
    @CheckForNull
    public CacheStatsMBean getTemplateCacheStats() {
        return null;
    }

    /**
     * @return  statistics for the node cache or {@code null} if not available.
     */
    @CheckForNull
    public CacheStatsMBean getNodeCacheStats() {
        return null;
    }

    /**
     * This implementation of {@link WriterCacheManager} returns empty caches
     * of size 0.
     * @see #INSTANCE
     */
    public static class Empty extends WriterCacheManager {

        /**
         * Singleton instance of {@link Empty}
         */
        public static final WriterCacheManager INSTANCE = new Empty();

        private final RecordCache<String> stringCache = newRecordCache(0);
        private final RecordCache<Template> templateCache = newRecordCache(0);
        private final NodeCache nodeCache = NodeCache.newNodeCache(0, 0);

        private Empty() {}

        /**
         * @return  empty cache of size 0
         */
        @Override
        public RecordCache<String> getStringCache(int generation) {
            return stringCache;
        }

        /**
         * @return  empty cache of size 0
         */
        @Override
        public RecordCache<Template> getTemplateCache(int generation) {
            return templateCache;
        }

        /**
         * @return  empty cache of size 0
         */
        @Override
        public NodeCache getNodeCache(int generation) {
            return nodeCache;
        }
    }

    /**
     * This implementation of {@link WriterCacheManager} returns
     * {@link RecordCache} instances for the string and template cache
     * and {@link NodeCache} instances for the node cache.
     */
    public static class Default extends WriterCacheManager {
        /**
         * Cache of recently stored string records, used to avoid storing duplicates
         * of frequently occurring data.
         */
        private final Generations<RecordCache<String>> stringCaches;

        /**
         * Cache of recently stored template records, used to avoid storing
         * duplicates of frequently occurring data.
         */
        private final Generations<RecordCache<Template>> templateCaches;

        /**
         * Cache of recently stored nodes to avoid duplicating linked nodes (i.e. checkpoints)
         * during compaction.
         */
        private final Generations<NodeCache> nodeCaches;

        /**
         * New instance using the passed factories for creating cache instances.
         * The factories will be invoked exactly once when a generation of a
         * cache is requested that has not been requested before.
         *
         * @param stringCacheFactory       factory for the string cache
         * @param templateCacheFactory     factory for the template cache
         * @param nodeCacheFactory         factory for the node cache
         */
        public Default(
                @Nonnull Supplier<RecordCache<String>> stringCacheFactory,
                @Nonnull Supplier<RecordCache<Template>> templateCacheFactory,
                @Nonnull Supplier<NodeCache> nodeCacheFactory) {
            this.stringCaches = new Generations<>(stringCacheFactory);
            this.templateCaches = new Generations<>(templateCacheFactory);
            this.nodeCaches = new Generations<>(nodeCacheFactory);
        }

        /**
         * New instance using the default factories {@link RecordCache#factory(int)}
         * and {@link NodeCache#factory(int, int)} with the sizes
         * {@link #DEFAULT_STRING_CACHE_SIZE} and {@link #DEFAULT_TEMPLATE_CACHE_SIZE}.
         */
        public Default() {
            this(RecordCache.<String>factory(DEFAULT_STRING_CACHE_SIZE),
                 RecordCache.<Template>factory(DEFAULT_TEMPLATE_CACHE_SIZE),
                 NodeCache.factory(1000000, 20));
        }

        private static class Generations<T> implements Iterable<T> {
            private final ConcurrentMap<Integer, Supplier<T>> generations = newConcurrentMap();
            private final Supplier<T> cacheFactory;

            Generations(@Nonnull Supplier<T> cacheFactory) {
                this.cacheFactory = checkNotNull(cacheFactory);
            }

            T getGeneration(final int generation) {
                // Preemptive check to limit the number of wasted (Memoizing)Supplier instances
                if (!generations.containsKey(generation)) {
                    generations.putIfAbsent(generation, memoize(cacheFactory));
                }
                return generations.get(generation).get();
            }

            @Override
            public Iterator<T> iterator() {
                return transform(generations.values().iterator(), new Function<Supplier<T>, T>() {
                    @Nullable @Override
                    public T apply(Supplier<T> cacheFactory) {
                        return cacheFactory.get();
                    }
                });
            }

            void evictGenerations(@Nonnull Predicate<Integer> evict) {
                Iterator<Integer> it = generations.keySet().iterator();
                while (it.hasNext()) {
                    if (evict.apply(it.next())) {
                        it.remove();
                    }
                }
            }
        }

        @Nonnull
        @Override
        public RecordCache<String> getStringCache(int generation) {
            return stringCaches.getGeneration(generation);
        }

        @Nonnull
        @Override
        public RecordCache<Template> getTemplateCache(int generation) {
            return templateCaches.getGeneration(generation);
        }

        @Nonnull
        @Override
        public NodeCache getNodeCache(int generation) {
            return nodeCaches.getGeneration(generation);
        }

        @CheckForNull
        @Override
        public CacheStatsMBean getStringCacheStats() {
            return new RecordCacheStats("String deduplication cache stats",
                    accumulateRecordCacheStats(stringCaches), accumulateRecordCacheSizes(stringCaches));
        }

        @CheckForNull
        @Override
        public CacheStatsMBean getTemplateCacheStats() {
            return new RecordCacheStats("Template deduplication cache stats",
                    accumulateRecordCacheStats(templateCaches), accumulateRecordCacheSizes(templateCaches));
        }

        @Nonnull
        private static <T> Supplier<CacheStats> accumulateRecordCacheStats(
                final Iterable<RecordCache<T>> caches) {
            return new Supplier<CacheStats>() {
                @Override
                public CacheStats get() {
                    CacheStats stats = new CacheStats(0, 0, 0, 0, 0, 0);
                    for (RecordCache<?> cache : caches) {
                        stats = stats.plus(cache.getStats());
                    }
                    return stats;
                }
            };
        }

        @Nonnull
        public static <T> Supplier<Long> accumulateRecordCacheSizes(
                final Iterable<RecordCache<T>> caches) {
            return new Supplier<Long>() {
                @Override
                public Long get() {
                    long size = 0;
                    for (RecordCache<?> cache : caches) {
                        size += cache.size();
                    }
                    return size;
                }
            };
        }

        @CheckForNull
        @Override
        public CacheStatsMBean getNodeCacheStats() {
            return new RecordCacheStats("Node deduplication cache stats",
                    accumulateNodeCacheStats(nodeCaches), accumulateNodeCacheSizes(nodeCaches));
        }

        @Nonnull
        private static <T> Supplier<CacheStats> accumulateNodeCacheStats(
                final Iterable<NodeCache> caches) {
            return new Supplier<CacheStats>() {
                @Override
                public CacheStats get() {
                    CacheStats stats = new CacheStats(0, 0, 0, 0, 0, 0);
                    for (NodeCache cache : caches) {
                        stats = stats.plus(cache.getStats());
                    }
                    return stats;
                }
            };
        }

        @Nonnull
        public static <T> Supplier<Long> accumulateNodeCacheSizes(
                final Iterable<NodeCache> caches) {
            return new Supplier<Long>() {
                @Override
                public Long get() {
                    long size = 0;
                    for (NodeCache cache : caches) {
                        size += cache.size();
                    }
                    return size;
                }
            };
        }

        /**
         * Remove all cache generations matching the passed {@code generations} predicate.
         * @param generations
         */
        protected final void evictCaches(Predicate<Integer> generations) {
            stringCaches.evictGenerations(generations);
            templateCaches.evictGenerations(generations);
            nodeCaches.evictGenerations(generations);
        }
    }
}
