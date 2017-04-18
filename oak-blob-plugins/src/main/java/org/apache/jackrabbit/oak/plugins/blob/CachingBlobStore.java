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
package org.apache.jackrabbit.oak.plugins.blob;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.spi.blob.AbstractBlobStore;

import com.google.common.cache.Weigher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A blob store with a cache.
 */
public abstract class CachingBlobStore extends AbstractBlobStore {

    private static final Logger LOG = LoggerFactory.getLogger(CachingBlobStore.class);

    protected static final long DEFAULT_CACHE_SIZE = 16 * 1024 * 1024;

    protected final CacheLIRS<String, byte[]> cache;

    protected final long blobCacheSize;

    private final Weigher<String, byte[]> weigher = new Weigher<String, byte[]>() {
        @Override
        public int weigh(@Nonnull String key, @Nonnull byte[] value) {
            long weight = (long)StringUtils.estimateMemoryUsage(key) + value.length;
            if (weight > Integer.MAX_VALUE) {
                LOG.debug("Calculated weight larger than Integer.MAX_VALUE: {}.", weight);
                weight = Integer.MAX_VALUE;
            }
            return (int) weight;
        }
    };

    private final CacheStats cacheStats;

    public static final String MEM_CACHE_NAME = "BlobStore-MemCache";

    public CachingBlobStore(long cacheSize) {
        this.blobCacheSize = cacheSize;
        cache = CacheLIRS.<String, byte[]>newBuilder().
                recordStats().
                module(MEM_CACHE_NAME).
                maximumWeight(cacheSize).
                averageWeight(getBlockSize() / 2).
                weigher(weigher).
                build();

        cacheStats = new CacheStats(cache, MEM_CACHE_NAME, weigher, cacheSize);
    }

    public CachingBlobStore() {
        this(DEFAULT_CACHE_SIZE);
    }

    @Override
    public void clearCache() {
        cache.invalidateAll();
    }

    public long getBlobCacheSize() {
        //Required for testcase to validate the configured cache size
        return blobCacheSize;
    }

    public CacheStats getCacheStats() {
        return cacheStats;
    }
}
