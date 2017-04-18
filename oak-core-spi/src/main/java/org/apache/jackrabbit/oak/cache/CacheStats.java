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
package org.apache.jackrabbit.oak.cache;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.cache.Cache;
import com.google.common.cache.Weigher;

/**
 * Cache statistics.
 */
public class CacheStats extends AbstractCacheStats {
    private final Cache<Object, Object> cache;
    private final Weigher<Object, Object> weigher;
    private final long maxWeight;

    /**
     * Construct the cache stats object.
     * 
     * @param cache the cache
     * @param name the name of the cache
     * @param weigher the weigher used to estimate the current weight
     * @param maxWeight the maximum weight
     */
    @SuppressWarnings("unchecked")
    public CacheStats(
            @Nonnull Cache<?, ?> cache,
            @Nonnull String name,
            @Nullable Weigher<?, ?> weigher,
            long maxWeight) {
        super(name);
        this.cache = (Cache<Object, Object>) checkNotNull(cache);
        this.weigher = (Weigher<Object, Object>) weigher;
        this.maxWeight = maxWeight;
    }

    @Override
    protected com.google.common.cache.CacheStats getCurrentStats() {
        return cache.stats();
    }

    @Override
    public long getElementCount() {
        return cache.size();
    }

    @Override
    public long estimateCurrentWeight() {
        if (weigher == null) {
            return -1;
        }
        long size = 0;
        for (Map.Entry<?, ?> e : cache.asMap().entrySet()) {
            Object k = e.getKey();
            Object v = e.getValue();
            size += weigher.weigh(k, v);
        }
        return size;
    }

    @Override
    public long getMaxTotalWeight() {
        return maxWeight;
    }
}
