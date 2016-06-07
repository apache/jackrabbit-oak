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

import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nonnull;

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;

// FIXME OAK-4277: Finalise de-duplication caches
// implement configuration, monitoring and management
// add unit tests
// document, nullability
public interface WriterCacheManager {

    @Nonnull
    RecordCache<String> getStringCache(int generation);

    @Nonnull
    RecordCache<Template> getTemplateCache(int generation);

    @Nonnull
    NodeCache getNodeCache(int generation);

    void evictCaches(Predicate<Integer> generations);

    class Empty implements WriterCacheManager {
        private static final WriterCacheManager EMPTY = new Empty();
        private final RecordCache<String> stringCache = RecordCache.<String>empty().get();
        private final RecordCache<Template> templateCache = RecordCache.<Template>empty().get();
        private final NodeCache nodeCache = NodeCache.empty().get();

        public static WriterCacheManager create() { return EMPTY; }

        private Empty(){}

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

        @Override
        public void evictCaches(Predicate<Integer> generations) { }
    }

    class Default implements WriterCacheManager {
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

        public static WriterCacheManager create(
                @Nonnull Supplier<RecordCache<String>> stringCacheFactory,
                @Nonnull Supplier<RecordCache<Template>> templateCacheFactory,
                @Nonnull Supplier<NodeCache> nodeCacheFactory) {
            return new Default(stringCacheFactory, templateCacheFactory, nodeCacheFactory);
        }

        private Default(
                @Nonnull Supplier<RecordCache<String>> stringCacheFactory,
                @Nonnull Supplier<RecordCache<Template>> templateCacheFactory,
                @Nonnull Supplier<NodeCache> nodeCacheFactory) {
            this.stringCaches = new Generation<>(stringCacheFactory);
            this.templateCaches = new Generation<>(templateCacheFactory);
            this.nodeCaches = new Generation<>(nodeCacheFactory);
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

        @Override
        public void evictCaches(Predicate<Integer> generations) {
            stringCaches.evictGenerations(generations);
            templateCaches.evictGenerations(generations);
            nodeCaches.evictGenerations(generations);
        }
    }
}
