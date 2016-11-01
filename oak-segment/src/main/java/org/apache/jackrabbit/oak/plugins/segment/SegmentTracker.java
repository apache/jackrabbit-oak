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

import static com.google.common.collect.Queues.newArrayDeque;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.Boolean.getBoolean;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.cache.CacheLIRS.EvictionCallback;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.plugins.blob.ReferenceCollector;
import org.apache.jackrabbit.oak.plugins.segment.compaction.CompactionStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.RemovalCause;

/**
 * Tracker of references to segment identifiers and segment instances
 * that are currently kept in memory.
 * <p>
 * It is also responsible to cache segment objects in memory.
 */
@Deprecated
public class SegmentTracker {

    /** Logger instance */
    private static final Logger log =
            LoggerFactory.getLogger(SegmentTracker.class);

    /**
     * Disable the {@link #stringCache} if {@code true} and fall back to
     * the previous {@link Segment#strings} caching mechanism.
     */
    private static final boolean DISABLE_STRING_CACHE = getBoolean("oak.segment.disableStringCache");

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
     * Serialized map that contains the link between old record
     * identifiers and identifiers of the corresponding records
     * after compaction.
     */
    private final AtomicReference<CompactionMap> compactionMap;

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
     * Number of segments
     */
    private final AtomicInteger segmentCounter = new AtomicInteger();

    private final SegmentVersion segmentVersion;

    @Deprecated
    public SegmentTracker(SegmentStore store, int cacheSizeMB,
            SegmentVersion version) {
        this.segmentVersion = version;

        for (int i = 0; i < tables.length; i++) {
            tables[i] = new SegmentIdTable(this);
        }

        this.store = store;
        this.compactionMap = new AtomicReference<CompactionMap>(
                CompactionMap.EMPTY);
        this.writer = createSegmentWriter("sys");
        StringCache c;
        if (DISABLE_STRING_CACHE) {
            c = null;
        } else {
            long cache = Long.getLong(STRING_CACHE_SIZE, (long) cacheSizeMB);
            c = new StringCache(cache * MB);
        }
        stringCache = c;
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

    @Deprecated
    public SegmentTracker(SegmentStore store, SegmentVersion version) {
        this(store, DEFAULT_MEMORY_CACHE_SIZE, version);
    }

    @Deprecated
    public SegmentTracker(SegmentStore store) {
        this(store, DEFAULT_MEMORY_CACHE_SIZE, SegmentVersion.V_11);
    }

    /**
     * Increment and get the number of segments
     * @return
     */
    int getNextSegmentNo() {
        return segmentCounter.incrementAndGet();
    }

    /**
     * @return  a new {@link SegmentWriter} instance for writing to this store.
     */
    @Deprecated
    public final SegmentWriter createSegmentWriter(String wid) {
        return new SegmentWriter(store, segmentVersion, wid);
    }

    @Nonnull
    @Deprecated
    public CacheStats getSegmentCacheStats() {
        return new CacheStats(segmentCache, "Segment Cache", null, -1);
    }

    @CheckForNull
    @Deprecated
    public CacheStats getStringCacheStats() {
        return stringCache == null
            ? null
            : stringCache.getStats();
    }

    @Deprecated
    public SegmentWriter getWriter() {
        return writer;
    }

    @Deprecated
    public SegmentStore getStore() {
        return store;
    }

    /**
     * Clear the caches
     */
    @Deprecated
    public synchronized void clearCache() {
        segmentCache.invalidateAll();
        if (stringCache != null) {
            stringCache.clear();
        }
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

    @Deprecated
    public void setCompactionMap(PartialCompactionMap map) {
        compactionMap.set(compactionMap.get().cons(map));
    }

    @Nonnull
    @Deprecated
    public CompactionMap getCompactionMap() {
        return compactionMap.get();
    }

    /**
     * Returns all segment identifiers that are currently referenced in memory.
     *
     * @return referenced segment identifiers
     */
    @Deprecated
    public synchronized Set<SegmentId> getReferencedSegmentIds() {
        Set<SegmentId> ids = newHashSet();
        for (SegmentIdTable table : tables) {
            table.collectReferencedIds(ids);
        }
        return ids;
    }

    /**
     * Finds all external blob references that are currently accessible
     * in this repository and adds them to the given collector. Useful
     * for collecting garbage in an external data store.
     * <p>
     * Note that this method only collects blob references that are already
     * stored in the repository (at the time when this method is called), so
     * the garbage collector will need some other mechanism for tracking
     * in-memory references and references stored while this method is
     * running.
     */
    @Deprecated
    public void collectBlobReferences(ReferenceCollector collector) {
        try {
            Set<UUID> processed = newHashSet();
            for (SegmentId sid : getReferencedSegmentIds()) {
                if (sid.isDataSegmentId()) {
                    processed.add(sid.asUUID());
                }
            }
            Queue<UUID> queue = newArrayDeque(processed);
            writer.flush(); // force the current segment to have root record info
            while (!queue.isEmpty()) {
                UUID uid = queue.remove();
                SegmentId id = getSegmentId(uid.getMostSignificantBits(),
                        uid.getLeastSignificantBits());
                Segment segment = null;
                try {
                    segment = id.getSegment();
                } catch (SegmentNotFoundException ex) {
                    // gc'ed
                }
                if (segment == null) {
                    continue;
                }
                segment.collectBlobReferences(collector);
                for (SegmentId refid : segment.getReferencedIds()) {
                    UUID rid = refid.asUUID();
                    if (refid.isDataSegmentId() && !processed.contains(rid)) {
                        queue.add(rid);
                        processed.add(rid);
                    }
                }
            }
        } catch (IOException e) {
            log.error("Error while flushing pending segments", e);
            throw new IllegalStateException("Unexpected IOException", e);
        }
    }

    /**
     * 
     * @param msb
     * @param lsb
     * @return the segment id
     */
    @Deprecated
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
        long msb = (random.nextLong() & MSB_MASK) | VERSION;
        long lsb = (random.nextLong() & LSB_MASK) | type;
        return getSegmentId(msb, lsb);
    }

    @Deprecated
    public synchronized void clearSegmentIdTables(CompactionStrategy strategy) {
        for (SegmentIdTable table : tables) {
            table.clearSegmentIdTables(strategy);
        }
    }

}
