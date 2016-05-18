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

import static com.google.common.base.Suppliers.memoize;
import static com.google.common.collect.Maps.newConcurrentMap;

import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// FIXME OAK-4277: Finalise de-duplication caches
// implement configuration, monitoring and management
// add unit tests
// document, nullability
public class WriterCacheManager {
    private static final Logger LOG = LoggerFactory.getLogger(WriterCacheManager.class);

    private static final int STRING_RECORDS_CACHE_SIZE = Integer.getInteger(
            "oak.segment.writer.stringsCacheSize", 15000);

    /**
     * Cache of recently stored string records, used to avoid storing duplicates
     * of frequently occurring data.
     */
    private final Generation<RecordCache<String>> stringCaches =
            new Generation<>(STRING_RECORDS_CACHE_SIZE <= 0
                    ? RecordCache.<String>empty()
                    : RecordCache.<String>factory(STRING_RECORDS_CACHE_SIZE));

    private static final int TPL_RECORDS_CACHE_SIZE = Integer.getInteger(
            "oak.segment.writer.templatesCacheSize", 3000);

    /**
     * Cache of recently stored template records, used to avoid storing
     * duplicates of frequently occurring data.
     */
    private final Generation<RecordCache<Template>> templateCaches =
            new Generation<>(TPL_RECORDS_CACHE_SIZE <= 0
                    ? RecordCache.<Template>empty()
                    : RecordCache.<Template>factory(TPL_RECORDS_CACHE_SIZE));

    private final Generation<NodeCache> nodeCaches =
            new Generation<>(NodeCache.factory(1000000, 20));

    private static class Generation<T> {
        private final ConcurrentMap<Integer, Supplier<T>> generations = newConcurrentMap();
        private final Supplier<T> cacheFactory;

        Generation(Supplier<T> cacheFactory) {
            this.cacheFactory = cacheFactory;
        }

        T getGeneration(final int generation) {
            // Preemptive check to limit the number of wasted (Memoizing)Supplier instances
            if (!generations.containsKey(generation)) {
                generations.putIfAbsent(generation, memoize(cacheFactory));
            }
            return generations.get(generation).get();
        }

        void evictGenerations(Predicate<Integer> evict) {
            Iterator<Integer> it = generations.keySet().iterator();
            while (it.hasNext()) {
                if (evict.apply(it.next())) {
                    it.remove();
                }
            }
        }
    }

    // FIXME OAK-4277 replace with GC monitor (improved with notification of failed and interrupted compaction)
    public void evictCaches(Predicate<Integer> evict) {
        stringCaches.evictGenerations(evict);
        templateCaches.evictGenerations(evict);
        nodeCaches.evictGenerations(evict);
    }

    public RecordCache<String> getStringCache(int generation) {
        return stringCaches.getGeneration(generation);
    }

    public RecordCache<Template> getTemplateCache(int generation) {
        return templateCaches.getGeneration(generation);
    }

    public NodeCache getNodeCache(int generation) {
        return nodeCaches.getGeneration(generation);
    }

}
