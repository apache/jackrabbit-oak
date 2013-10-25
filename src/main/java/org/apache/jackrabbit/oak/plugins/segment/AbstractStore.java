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

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

import org.apache.jackrabbit.oak.cache.CacheLIRS;

import com.google.common.cache.Cache;

public abstract class AbstractStore implements SegmentStore {

    private final Cache<UUID, Segment> segments;

    /**
     * Identifiers of the segments that are currently being loaded.
     */
    private final Set<UUID> currentlyLoading = newHashSet();

    private final Cache<RecordId, Object> records =
            CacheLIRS.newBuilder().maximumSize(1000).build();

    private final SegmentWriter writer = new SegmentWriter(this);

    protected AbstractStore(int cacheSize) {
        this.segments = CacheLIRS.newBuilder()
                .weigher(Segment.WEIGHER)
                .maximumWeight(cacheSize)
                .build();
    }

    protected abstract Segment loadSegment(UUID id) throws Exception;

    @Override
    public SegmentWriter getWriter() {
        return writer;
    }

    @Override
    public Segment readSegment(UUID id) {
        Segment segment = segments.getIfPresent(id);
        if (segment != null) {
            return segment;
        }

        segment = getWriter().getCurrentSegment(id);
        if (segment != null) {
            return segment;
        }

        synchronized (this) {
            segment = segments.getIfPresent(id);
            while (segment == null && currentlyLoading.contains(id)) {
                try {
                    wait(); // for another thread to load the segment
                } catch (InterruptedException e) {
                    throw new RuntimeException("Interrupted", e);
                }
                segment = segments.getIfPresent(id);
            }
            if (segment != null) {
                return segment;
            }
            currentlyLoading.add(id);
        }

        try {
            segment = loadSegment(id);
            if (segment == null) {
                throw new IllegalStateException("Unable to find segment " + id);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load segment " + id, e);
        } finally {
            synchronized (this) {
                if (segment != null) {
                    segments.put(id, segment);
                }
                currentlyLoading.remove(id);
                notifyAll();
            }
        }

        return segment;
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
        segments.invalidate(segmentId);
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
        segments.invalidateAll();
    }

}
