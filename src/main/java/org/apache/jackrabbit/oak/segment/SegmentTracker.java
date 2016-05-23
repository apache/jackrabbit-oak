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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.Long.getLong;

import java.security.SecureRandom;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import com.google.common.base.Predicate;
import com.google.common.cache.RemovalCause;
import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.cache.CacheLIRS.EvictionCallback;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracker of references to segment identifiers and segment instances
 * that are currently kept in memory.
 * <p>
 * It is also responsible to cache segment objects in memory.
 */
public class SegmentTracker {

    /** Logger instance */
    private static final Logger log =
            LoggerFactory.getLogger(SegmentTracker.class);

    static final String STRING_CACHE_SIZE = "oak.segment.stringCache";

    private static final long MSB_MASK = ~(0xfL << 12);

    private static final long VERSION = (0x4L << 12);

    private static final long LSB_MASK = ~(0xfL << 60);

    private static final long DATA = 0xAL << 60;

    private static final long BULK = 0xBL << 60;

    private static final long MB = 1024 * 1024;

    private static final int DEFAULT_MEMORY_CACHE_SIZE = 256;

    /**
     * The random number source for generating new segment identifiers.
     */
    private final SecureRandom random = new SecureRandom();

    private final SegmentStore store;

    private final SegmentWriter writer;

    /**
     * Hash table of weak references to segment identifiers that are
     * currently being accessed. The size of the table is always a power
     * of two, which optimizes the {@code refresh()} operation. The table is
     * indexed by the random identifier bits, which guarantees uniform
     * distribution of entries. Each table entry is either {@code null}
     * (when there are no matching identifiers) or a list of weak references
     * to the matching identifiers.
     */
    private final SegmentIdTable[] tables = new SegmentIdTable[32];

    /**
     * Cache for string records
     */
    private final StringCache stringCache;

    /**
     * Cache of recently accessed segments
     */
    private final CacheLIRS<SegmentId, Segment> segmentCache;

    /**
     * Number of segment tracked since this tracker was instantiated
     */
    private final AtomicInteger segmentCounter = new AtomicInteger();

    public SegmentTracker(SegmentStore store, int cacheSizeMB, SegmentVersion version) {
        checkArgument(SegmentVersion.isValid(version));

        for (int i = 0; i < tables.length; i++) {
            tables[i] = new SegmentIdTable(this);
        }

        this.store = store;
        this.writer = new SegmentWriter(store, version,
                            new SegmentBufferWriterPool(store, version, "sys"));
        stringCache = new StringCache(getLong(STRING_CACHE_SIZE, (long) cacheSizeMB) * MB);
        segmentCache = CacheLIRS.<SegmentId, Segment>newBuilder()
            .module("SegmentTracker")
            .maximumWeight((long) cacheSizeMB * MB)
            .averageWeight(Segment.MAX_SEGMENT_SIZE/2)
            .evictionCallback(new EvictionCallback<SegmentId, Segment>() {
                @Override
                public void evicted(SegmentId segmentId, Segment segment, RemovalCause cause) {
                    if (segment != null) {
                        segmentId.setSegment(null);
                    }
                }
            })
            .build();
    }

    public SegmentTracker(SegmentStore store, SegmentVersion version) {
        this(store, DEFAULT_MEMORY_CACHE_SIZE, version);
    }

    public SegmentTracker(SegmentStore store) {
        this(store, DEFAULT_MEMORY_CACHE_SIZE, SegmentVersion.LATEST_VERSION);
    }

    /**
     * Number of segment tracked since this tracker was instantiated
     * @return count
     */
    int getSegmentCount() {
        return segmentCounter.get();
    }

    public boolean isTracking(SegmentId segmentId) {
        return this == segmentId.getTracker();
    }

    @Nonnull
    public CacheStats getSegmentCacheStats() {
        return new CacheStats(segmentCache, "Segment Cache", null, -1);
    }

    @CheckForNull
    public CacheStats getStringCacheStats() {
        return stringCache.getStats();
    }

    public SegmentWriter getWriter() {
        return writer;
    }

    public SegmentStore getStore() {
        return store;
    }

    /**
     * Clear the caches
     */
    public synchronized void clearCache() {
        segmentCache.invalidateAll();
        stringCache.clear();
    }

    /**
     * Get the string cache, if there is one.
     *
     * @return the string cache or {@code null} if none is configured
     */
    StringCache getStringCache() {
        return stringCache;
    }

    /**
     * Get a segment from the cache
     * @param id  segment id
     * @return  segment with the given {@code id} or {@code null} if not in the cache
     */
    Segment getCachedSegment(SegmentId id) {
        try {
            return segmentCache.get(id);
        } catch (ExecutionException e) {
            log.error("Error reading from segment cache", e);
            return null;
        }
    }

    /**
     * Read a segment from the underlying segment store.
     * @param id  segment id
     * @return  segment with the given id
     * @throws SegmentNotFoundException  if no segment with the given {@code id} exists.
     */
    Segment readSegment(SegmentId id) {
        try {
            Segment segment = store.readSegment(id);
            setSegment(id, segment);
            return segment;
        } catch (SegmentNotFoundException snfe) {
            long delta = System.currentTimeMillis() - id.getCreationTime();
            log.error("Segment not found: {}. Creation date delta is {} ms.",
                    id, delta, snfe);
            throw snfe;
        }
    }

    void setSegment(SegmentId id, Segment segment) {
        id.setSegment(segment);
        segmentCache.put(id, segment, segment.size());
    }

    // FIXME OAK-4102: Break cyclic dependency of FileStore and SegmentTracker
    // Improve retrieving current GC generation. (OAK-4102)
    // See also the comments in FileStore regarding initialisation and cyclic dependencies.
    public int getGcGeneration() {
        if (store instanceof FileStore) {
            return ((FileStore) store).getGcGeneration();
        } else {
            return 0;
        }
    }

    /**
     * Returns all segment identifiers that are currently referenced in memory.
     *
     * @return referenced segment identifiers
     */
    public synchronized Set<SegmentId> getReferencedSegmentIds() {
        Set<SegmentId> ids = newHashSet();
        for (SegmentIdTable table : tables) {
            table.collectReferencedIds(ids);
        }
        return ids;
    }

    /**
     * 
     * @param msb
     * @param lsb
     * @return the segment id
     */
    public SegmentId getSegmentId(long msb, long lsb) {
        int index = ((int) msb) & (tables.length - 1);
        return tables[index].getSegmentId(msb, lsb);
    }

    SegmentId newDataSegmentId() {
        return newSegmentId(DATA);
    }

    SegmentId newBulkSegmentId() {
        return newSegmentId(BULK);
    }

    private SegmentId newSegmentId(long type) {
        segmentCounter.incrementAndGet();
        long msb = (random.nextLong() & MSB_MASK) | VERSION;
        long lsb = (random.nextLong() & LSB_MASK) | type;
        return getSegmentId(msb, lsb);
    }

    // FIXME OAK-4285: Align cleanup of segment id tables with the new cleanup strategy
    // ith clean brutal we need to remove those ids that have been cleaned
    // i.e. those whose segment was from an old generation
    public synchronized void clearSegmentIdTables(Predicate<SegmentId> canRemove) {
        for (SegmentIdTable table : tables) {
            table.clearSegmentIdTables(canRemove);
        }
    }

}
