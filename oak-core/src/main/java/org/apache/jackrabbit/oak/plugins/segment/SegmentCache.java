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
package org.apache.jackrabbit.oak.plugins.segment;

import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * Combined memory and disk cache for segments.
 */
public class SegmentCache {

    private static final long DEFAULT_MEMORY_CACHE_SIZE = 1 << 28; // 256MB

    private final Cache<UUID, Segment> memoryCache;

    // private final Cache<UUID, File> diskCache;

    // private final File diskCacheDirectory;

    public SegmentCache(long memoryCacheSize) {
//        this.diskCacheDirectory = diskCacheDirectory;
//        this.diskCache = CacheBuilder.newBuilder()
//                .maximumWeight(diskCacheSize)
//                .weigher(new Weigher<UUID, File>() {
//                    @Override
//                    public int weigh(UUID key, File value) {
//                        return (int) value.length(); // <= max segment size
//                    }
//                }).build();
        this.memoryCache = CacheBuilder.newBuilder()
                .maximumWeight(memoryCacheSize)
                .weigher(Segment.WEIGHER)
//                .removalListener(new RemovalListener<UUID, Segment>() {
//                    @Override
//                    public void onRemoval(
//                            RemovalNotification<UUID, Segment> notification) {
//                        notification.getValue();
//                    }
//                })
                .build();
    }

    public SegmentCache() {
        this(DEFAULT_MEMORY_CACHE_SIZE);
    }

    public Segment getSegment(UUID segmentId, Callable<Segment> loader) {
        try {
            return memoryCache.get(segmentId, loader);
        } catch (ExecutionException e) {
            throw new IllegalStateException(
                    "Failed to load segment " + segmentId, e);
        }
    }

    public void addSegment(Segment segment) {
        memoryCache.put(segment.getSegmentId(), segment);
    }

    public void removeSegment(UUID segmentId) {
        memoryCache.invalidate(segmentId);
    }

}
