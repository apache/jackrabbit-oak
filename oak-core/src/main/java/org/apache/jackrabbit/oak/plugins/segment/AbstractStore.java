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

import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentIdFactory.isBulkSegmentId;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.UUID;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.cache.CacheLIRS;

import com.google.common.cache.Cache;

public abstract class AbstractStore implements SegmentStore {

    protected static final int MB = 1024 * 1024;

    protected final Cache<UUID, Segment> segments;

    /**
     * Identifiers of the segments that are currently being loaded.
     */
    private final Set<UUID> currentlyLoading = newHashSet();

    /**
     * Number of threads that are currently waiting for segments to be loaded.
     * Used to avoid extra {@link #notifyAll()} calls when nobody is waiting.
     */
    private int currentlyWaiting = 0;

    private final SegmentIdFactory factory = new SegmentIdFactory();

    private final SegmentWriter writer;

    protected AbstractStore(int cacheSizeMB) {
        this.writer = new SegmentWriter(this, factory);
        if (cacheSizeMB > 0) {
            this.segments = CacheLIRS.newBuilder()
                    .weigher(Segment.WEIGHER)
                    .maximumWeight(cacheSizeMB * MB)
                    .build();
        } else {
            this.segments = null;
        }
    }

    protected Segment createSegment(UUID segmentId, ByteBuffer data) {
        return new Segment(this, factory, segmentId, data);
    }

    @Nonnull
    protected abstract Segment loadSegment(UUID id);

    @Override
    public SegmentWriter getWriter() {
        return writer;
    }

    @Override
    public Segment readSegment(UUID id) {
        if (isBulkSegmentId(id)) {
            return loadSegment(id);
        }

        Segment segment = getWriter().getCurrentSegment(id);
        if (segment != null) {
            return segment;
        }

        if (segments == null) {
            // no in-memory cache, load the segment directly
            return loadSegment(id);
        }

        synchronized (segments) {
            // check if the segment is already cached
            segment = segments.getIfPresent(id);
            // ... or currently being loaded
            while (segment == null && currentlyLoading.contains(id)) {
                currentlyWaiting++;
                try {
                    segments.wait(); // for another thread to load the segment
                } catch (InterruptedException e) {
                    throw new RuntimeException("Interrupted", e);
                } finally {
                    currentlyWaiting--;
                }
                segment = segments.getIfPresent(id);
            }
            if (segment != null) {
                // found the segment in the cache
                return segment;
            }
            // not yet cached, so start let others know that we're loading it
            currentlyLoading.add(id);
        }

        try {
            segment = loadSegment(id);
        } finally {
            synchronized (segments) {
                if (segment != null) {
                    segments.put(id, segment);
                }
                currentlyLoading.remove(id);
                if (currentlyWaiting > 0) {
                    segments.notifyAll();
                }
            }
        }

        return segment;
    }

    @Override
    public void deleteSegment(UUID segmentId) {
        if (segments != null) {
            synchronized (segments) {
                while (currentlyLoading.contains(segmentId)) {
                    try {
                        segments.wait(); // for concurrent load to finish
                    } catch (InterruptedException e) {
                        throw new RuntimeException("Interrupted", e);
                    }
                }
                segments.invalidate(segmentId);
            }
        }
    }

    @Override
    public void close() {
        if (segments != null) {
            synchronized (segments) {
                while (!currentlyLoading.isEmpty()) {
                    try {
                        segments.wait(); // for concurrent loads to finish
                    } catch (InterruptedException e) {
                        throw new RuntimeException("Interrupted", e);
                    }
                }
                segments.invalidateAll();
            }
        }
    }

    @Override
    public boolean isInstance(Object object, Class<? extends Record> type) {
        assert Record.class.isAssignableFrom(type);
        return type.isInstance(object) && ((Record) object).getStore() == this;
    }

}
