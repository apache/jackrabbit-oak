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

import static com.google.common.collect.Sets.newHashSet;

import java.security.SecureRandom;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

import com.google.common.base.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tracker of references to segment identifiers and segment instances
 * that are currently kept in memory.
 * <p>
 * It is also responsible to cache segment objects in memory.
 */
public class SegmentTracker {
    private static final Logger log = LoggerFactory.getLogger(SegmentTracker.class);

    private static final long MSB_MASK = ~(0xfL << 12);

    private static final long VERSION = (0x4L << 12);

    private static final long LSB_MASK = ~(0xfL << 60);

    private static final long DATA = 0xAL << 60;

    private static final long BULK = 0xBL << 60;

    /**
     * The random number source for generating new segment identifiers.
     */
    private final SecureRandom random = new SecureRandom();

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
     * Number of segment tracked since this tracker was instantiated
     */
    private final AtomicInteger segmentCounter = new AtomicInteger();

    public SegmentTracker(@Nonnull SegmentStore store) {
        for (int i = 0; i < tables.length; i++) {
            tables[i] = new SegmentIdTable(store);
        }
    }

    /**
     * Number of segment tracked since this tracker was instantiated
     * @return count
     */
    int getSegmentCount() {
        return segmentCounter.get();
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
    // Instead of removing, mark affected ids as gc'ed so the SNFE caused by
    // any subsequent access can report a precise cause
    public synchronized void clearSegmentIdTables(Predicate<SegmentId> canRemove) {
        for (SegmentIdTable table : tables) {
            table.clearSegmentIdTables(canRemove);
        }
    }

}
