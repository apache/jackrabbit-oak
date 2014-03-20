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

import static com.google.common.collect.Lists.newLinkedList;
import static com.google.common.collect.Queues.newArrayDeque;
import static com.google.common.collect.Sets.newHashSet;
import static com.google.common.collect.Sets.newIdentityHashSet;

import java.security.SecureRandom;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

/**
 * Tracker of references to segment identifiers and segment instances
 * that are currently kept in memory.
 */
public class SegmentTracker {

    private static final long MSB_MASK = ~(0xfL << 12);

    private static final long VERSION = (0x4L << 12);

    private static final long LSB_MASK = ~(0xfL << 60);

    private static final long DATA = 0xAL << 60;

    private static final long BULK = 0xBL << 60;

    private static final int MB = 1024 * 1024;

    private static final int DEFAULT_MEMORY_CACHE_SIZE = 256;

    /**
     * The random number source for generating new segment identifiers.
     */
    private final SecureRandom random = new SecureRandom();

    private final SegmentStore store;

    private final SegmentWriter writer;

    private final long cacheSize;

    /**
     * Hash table of weak references to segment identifiers that are
     * currently being accessed. The size of the table is always a power
     * of two, which optimizes the {@link #expand()} operation. The table is
     * indexed by the random identifier bits, which guarantees uniform
     * distribution of entries. Each table entry is either {@code null}
     * (when there are no matching identifiers) or a list of weak references
     * to the matching identifiers.
     */
    private final SegmentIdTable[] tables = new SegmentIdTable[32];

    private final LinkedList<Segment> dataSegments = newLinkedList();

    private final LinkedList<Segment> bulkSegments = newLinkedList();

    private long currentSize = 0;

    public SegmentTracker(SegmentStore store, int cacheSizeMB) {
        for (int i = 0; i < tables.length; i++) {
            tables[i] = new SegmentIdTable(this);
        }

        this.store = store;
        this.writer = new SegmentWriter(store, this);
        this.cacheSize = cacheSizeMB * MB;
    }

    public SegmentTracker(SegmentStore store) {
        this(store, DEFAULT_MEMORY_CACHE_SIZE);
    }

    public SegmentWriter getWriter() {
        return writer;
    }

    public SegmentStore getStore() {
        return store;
    }

    Segment getSegment(SegmentId id) {
        Segment segment = store.readSegment(id);
        id.setSegment(segment);

        LinkedList<Segment> segments = dataSegments;
        if (id.isBulkSegmentId()) {
            segments = bulkSegments;
        }

        synchronized (this) {
            // TODO: use a scan-resistant cache
            segments.addFirst(segment);
            currentSize += segment.getCacheSize();
            while (currentSize > cacheSize / 2 && segments.size() > 1) {
                Segment remove = dataSegments.removeLast();
                remove.getSegmentId().setSegment(null);
                currentSize -= remove.getCacheSize();
            }
        }

        return segment;
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
    public void collectBlobReferences(ReferenceCollector collector) {
        Set<SegmentId> processed = newIdentityHashSet();
        Queue<SegmentId> queue = newArrayDeque(getReferencedSegmentIds());
        writer.flush(); // force the current segment to have root record info
        while (!queue.isEmpty()) {
            SegmentId id = queue.remove();
            if (id.isDataSegmentId() && !processed.add(id)) {
                Segment segment = id.getSegment();

                segment.collectBlobReferences(collector);

                for (SegmentId refid : segment.getReferencedIds()) {
                    if (refid.isDataSegmentId() && !processed.contains(refid)) {
                        queue.add(refid);
                    }
                }
            }
        }
    }

    /**
     * 
     * @param msb
     * @param lsb
     * @return
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
        long msb = (random.nextLong() & MSB_MASK) | VERSION;
        long lsb = (random.nextLong() & LSB_MASK) | type;
        return getSegmentId(msb, lsb);
    }

}
