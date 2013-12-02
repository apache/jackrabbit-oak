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

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.cache.CacheLIRS;

import com.google.common.cache.Cache;

public abstract class AbstractStore implements SegmentStore {

    protected static final int MB = 1024 * 1024;

    private final Cache<UUID, Segment> segments;

    /**
     * Identifiers of the segments that are currently being loaded.
     */
    private final Set<UUID> currentlyLoading = newHashSet();

    /**
     * Number of threads that are currently waiting for segments to be loaded.
     * Used to avoid extra {@link #notifyAll()} calls when nobody is waiting.
     */
    private int currentlyWaiting = 0;

    private final Cache<RecordId, Object> records =
            CacheLIRS.newBuilder().maximumSize(1000).build();

    private final SegmentWriter writer = new SegmentWriter(this);

    protected AbstractStore(int cacheSizeMB) {
        if (cacheSizeMB > 0) {
            this.segments = CacheLIRS.newBuilder()
                    .weigher(Segment.WEIGHER)
                    .maximumWeight(cacheSizeMB * MB)
                    .build();
        } else {
            this.segments = null;
        }
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

        synchronized (this) {
            // check if the segment is already cached
            segment = segments.getIfPresent(id);
            // ... or currently being loaded
            while (segment == null && currentlyLoading.contains(id)) {
                currentlyWaiting++;
                try {
                    wait(); // for another thread to load the segment
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
            segments.put(id, segment);
            return segment;
        } finally {
            synchronized (this) {
                currentlyLoading.remove(id);
                if (currentlyWaiting > 0) {
                    notifyAll();
                }
            }
        }
    }

    @Override
    public synchronized void deleteSegment(UUID segmentId) {
        while (currentlyLoading.contains(segmentId)) {
            try {
                wait(); // for another thread to finish loading the segment
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted", e);
            }
        }
        if (segments != null) {
            segments.invalidate(segmentId);
        }
    }

    @Override
    public <T> T getRecord(RecordId id, Callable<T> loader) {
        @SuppressWarnings("unchecked")
        T record = (T) records.getIfPresent(id);
        if (record == null) {
            try {
                record = loader.call();
                records.put(id, record);
            } catch (Exception e) {
                throw new IllegalStateException(
                        "Failed to load record " + id, e);
            }
        }
        return record;
    }

    @Override
    public synchronized void close() {
        while (!currentlyLoading.isEmpty()) {
            try {
                wait(); // for other threads to finish loading segments
            } catch (InterruptedException e) {
                throw new RuntimeException("Interrupted", e);
            }
        }
        records.invalidateAll();
        if (segments != null) {
            segments.invalidateAll();
        }
    }

    @Override
    public boolean isInstance(Object object, Class<? extends Record> type) {
        assert Record.class.isAssignableFrom(type);
        return type.isInstance(object) && ((Record) object).getStore() == this;
    }

}
