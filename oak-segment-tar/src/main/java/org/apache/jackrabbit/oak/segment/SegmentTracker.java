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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Sets.newHashSet;

import java.security.SecureRandom;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nonnull;

/**
 * Tracker of references to segment identifiers and segment instances
 * that are currently kept in memory and factory for creating {@link SegmentId}
 * instances.
 */
public class SegmentTracker implements SegmentIdProvider {
    private static final long MSB_MASK = ~(0xfL << 12);

    private static final long VERSION = (0x4L << 12);

    private static final long LSB_MASK = ~(0xfL << 60);

    private static final long DATA = 0xAL << 60;

    private static final long BULK = 0xBL << 60;

    /**
     * The random number source for generating new segment identifiers.
     */
    @Nonnull
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
    @Nonnull
    private final SegmentIdTable[] tables = new SegmentIdTable[32];

    /**
     * Number of segment tracked since this tracker was instantiated
     */
    @Nonnull
    private final AtomicInteger segmentCounter = new AtomicInteger();

    @Nonnull
    private final SegmentIdFactory segmentIdFactory;

    public SegmentTracker(@Nonnull SegmentIdFactory segmentIdFactory) {
        this.segmentIdFactory = checkNotNull(segmentIdFactory);
        for (int i = 0; i < tables.length; i++) {
            tables[i] = new SegmentIdTable();
        }
    }

    @Override
    public int getSegmentIdCount() {
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
     * Get an existing {@code SegmentId} with the given {@code msb} and {@code
     * lsb} or create a new one if no such id exists with this tracker.
     *
     * @param msb   most significant bits of the segment id
     * @param lsb   least  significant bits of the segment id
     * @return the segment id
     */
    @Override
    @Nonnull
    public SegmentId newSegmentId(long msb, long lsb) {
        int index = ((int) msb) & (tables.length - 1);
        return tables[index].newSegmentId(msb, lsb, segmentIdFactory);
    }

    /**
     * Create and track a new segment id for data segments.
     *
     * @return the segment id
     */
    @Override
    @Nonnull
    public SegmentId newDataSegmentId() {
        return newSegmentId(DATA);
    }

    /**
     * Create and track a new segment id for bulk segments.
     *
     * @return the segment id
     */
    @Override
    @Nonnull
    public SegmentId newBulkSegmentId() {
        return newSegmentId(BULK);
    }

    @Nonnull
    private SegmentId newSegmentId(long type) {
        segmentCounter.incrementAndGet();
        long msb = (random.nextLong() & MSB_MASK) | VERSION;
        long lsb = (random.nextLong() & LSB_MASK) | type;
        return newSegmentId(msb, lsb);
    }

    public synchronized void clearSegmentIdTables(@Nonnull Set<UUID> reclaimed, @Nonnull String gcInfo) {
        for (SegmentIdTable table : tables) {
            table.clearSegmentIdTables(reclaimed, gcInfo);
        }
    }

}
