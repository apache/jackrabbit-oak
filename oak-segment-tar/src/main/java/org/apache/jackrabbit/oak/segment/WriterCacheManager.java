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
import static com.google.common.collect.Maps.newConcurrentMap;
import static java.lang.Integer.getInteger;
import static org.apache.jackrabbit.oak.segment.RecordCache.newRecordCache;

import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nonnull;

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;

// FIXME OAK-4277: Finalise de-duplication caches
// implement configuration, monitoring and management
// add unit tests
// document, nullability
public abstract class WriterCacheManager {
    private static final int DEFAULT_STRING_CACHE_SIZE = getInteger(
            "oak.tar.stringsCacheSize", 15000);

    private static final int DEFAULT_TEMPLATE_CACHE_SIZE = getInteger(
            "oak.tar.templatesCacheSize", 3000);

    @Nonnull
    public abstract RecordCache<String> getStringCache(int generation);

    @Nonnull
    public abstract RecordCache<Template> getTemplateCache(int generation);

    @Nonnull
    public abstract NodeCache getNodeCache(int generation);

    public static class Empty extends WriterCacheManager {
        public static final WriterCacheManager INSTANCE = new Empty();

        private final RecordCache<String> stringCache = newRecordCache(0);
        private final RecordCache<Template> templateCache = newRecordCache(0);
        private final NodeCache nodeCache = NodeCache.newNodeCache(0, 0);

        private Empty() {}

        @Override
        public RecordCache<String> getStringCache(int generation) {
            return stringCache;
        }

        @Override
        public RecordCache<Template> getTemplateCache(int generation) {
            return templateCache;
        }

        @Override
        public NodeCache getNodeCache(int generation) {
            return nodeCache;
        }
    }

    public static class Default extends WriterCacheManager {
        /**
         * Cache of recently stored string records, used to avoid storing duplicates
         * of frequently occurring data.
         */
        private final Generation<RecordCache<String>> stringCaches;

        /**
         * Cache of recently stored template records, used to avoid storing
         * duplicates of frequently occurring data.
         */
        private final Generation<RecordCache<Template>> templateCaches;

        /**
         * Cache of recently stored nodes to avoid duplicating linked nodes (i.e. checkpoints)
         * during compaction.
         */
        private final Generation<NodeCache> nodeCaches;

        public Default(
                @Nonnull Supplier<RecordCache<String>> stringCacheFactory,
                @Nonnull Supplier<RecordCache<Template>> templateCacheFactory,
                @Nonnull Supplier<NodeCache> nodeCacheFactory) {
            this.stringCaches = new Generation<>(stringCacheFactory);
            this.templateCaches = new Generation<>(templateCacheFactory);
            this.nodeCaches = new Generation<>(nodeCacheFactory);
        }

        public Default() {
            this(RecordCache.<String>factory(DEFAULT_STRING_CACHE_SIZE),
                 RecordCache.<Template>factory(DEFAULT_TEMPLATE_CACHE_SIZE),
                 NodeCache.factory(1000000, 20));
        }

        private static class Generation<T> {
            private final ConcurrentMap<Integer, Supplier<T>> generations = newConcurrentMap();
            private final Supplier<T> cacheFactory;

            Generation(@Nonnull Supplier<T> cacheFactory) {
                this.cacheFactory = checkNotNull(cacheFactory);
            }

            T getGeneration(final int generation) {
                // Preemptive check to limit the number of wasted (Memoizing)Supplier instances
                if (!generations.containsKey(generation)) {
                    generations.putIfAbsent(generation, memoize(cacheFactory));
                }
                return generations.get(generation).get();
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

        protected final void evictCaches(Predicate<Integer> generations) {
            stringCaches.evictGenerations(generations);
            templateCaches.evictGenerations(generations);
            nodeCaches.evictGenerations(generations);
        }
    }
}
