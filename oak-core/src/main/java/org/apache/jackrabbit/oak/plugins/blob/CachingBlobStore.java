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

import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.spi.blob.AbstractBlobStore;

import com.google.common.cache.Weigher;

/**
 * A blob store with a cache.
 */
public abstract class CachingBlobStore extends AbstractBlobStore {

    protected static final long DEFAULT_CACHE_SIZE = 16*1024*1024;

    protected final CacheLIRS<String, byte[]> cache;

    protected final long blobCacheSize;

    public CachingBlobStore(long cacheSize) {
        this.blobCacheSize = cacheSize;
        cache = CacheLIRS.newBuilder().
                maximumWeight(cacheSize).
                averageWeight(getBlockSize() / 2).
                weigher(new Weigher<String, byte[]>() {
                    @Override
                    public int weigh(String key, byte[] value) {
                        return value.length;
                    }
                }).build();

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
}
