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
import java.util.concurrent.ExecutionException;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

public class CacheStore implements SegmentStore {

    private final SegmentStore store;

    private final LoadingCache<UUID, Segment> cache;

    public CacheStore(final SegmentStore store, long cacheSize) {
        this.store = store;
        this.cache = CacheBuilder.newBuilder()
                .maximumWeight(cacheSize)
                .weigher(Segment.weigher())
                .build(new CacheLoader<UUID, Segment>() {
                    @Override
                    public Segment load(UUID key) throws Exception {
                        return store.readSegment(key);
                    }
                });
    }

    @Override
    public int getMaxSegmentSize() {
        return store.getMaxSegmentSize();
    }

    @Override
    public Segment readSegment(UUID segmentId) {
        try {
            return cache.get(segmentId);
        } catch (ExecutionException e) {
            throw new IllegalStateException(
                    "Failed to read segment " + segmentId, e);
        }
    }

    @Override
    public void createSegment(Segment segment) {
        store.createSegment(segment);
        cache.put(segment.getSegmentId(), segment);
    }

    @Override
    public void createSegment(
            UUID segmentId, byte[] data, int offset, int length) {
        store.createSegment(segmentId, data, offset, length);
    }

    @Override
    public void deleteSegment(UUID segmentId) {
        store.deleteSegment(segmentId);
        cache.invalidate(segmentId);
    }
}
