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

import static java.lang.Integer.getInteger;
import static java.lang.Integer.rotateLeft;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Segment identifier. There are two types of segments: data segments, and bulk
 * segments. Data segments have a header and may reference other segments; bulk
 * segments do not.
 */
@Deprecated
public class SegmentId implements Comparable<SegmentId> {

    /** Logger instance */
    private static final Logger log = LoggerFactory.getLogger(SegmentId.class);

    /**
     * Sample rate bit mask of {@link SegmentTracker#segmentCache}. Lower values
     * will cause more frequent accesses to that cache instead of the short
     * circuit through {@link SegmentId#segment}. Access to that cache is slower
     * but allows tracking access statistics. Should be 2^x - 1 (for example
     * 1023, 255, 15,...).
     */
    private static final int SEGMENT_CACHE_SAMPLE_MASK = getInteger("SegmentCacheSampleRate", 1023);

    /**
     * The initial random value for the pseudo random number generator. Initial
     * values of 0 - 0xffff will ensure a long period, but other values don't.
     */
    private static volatile int random = (int) (System.currentTimeMillis() & 0xffff);
    
    /**
     * Checks whether this is a data segment identifier.
     *
     * @return {@code true} for a data segment, {@code false} otherwise
     */
    @Deprecated
    public static boolean isDataSegmentId(long lsb) {
        return (lsb >>> 60) == 0xAL;
    }

    private final SegmentTracker tracker;

    private final long msb;

    private final long lsb;

    private long creationTime;

    /**
     * A reference to the segment object, if it is available in memory. It is
     * used for fast lookup. The segment tracker will set or reset this field.
     * <p>
     * Needs to be volatile so {@link #setSegment(Segment)} doesn't need to
     * be synchronized as this would lead to deadlocks.
     */
    private volatile Segment segment;

    private SegmentId(SegmentTracker tracker, long msb, long lsb,
            Segment segment, long creationTime) {
        this.tracker = tracker;
        this.msb = msb;
        this.lsb = lsb;
        this.segment = segment;
        this.creationTime = creationTime;
    }

    @Deprecated
    public SegmentId(SegmentTracker tracker, long msb, long lsb) {
        this(tracker, msb, lsb, null, System.currentTimeMillis());
    }

    /**
     * Checks whether this is a data segment identifier.
     *
     * @return {@code true} for a data segment, {@code false} otherwise
     */
    @Deprecated
    public boolean isDataSegmentId() {
        return isDataSegmentId(lsb);
    }

    /**
     * Checks whether this is a bulk segment identifier.
     *
     * @return {@code true} for a bulk segment, {@code false} otherwise
     */
    @Deprecated
    public boolean isBulkSegmentId() {
        return (lsb >>> 60) == 0xBL;
    }

    @Deprecated
    public boolean equals(long msb, long lsb) {
        return this.msb == msb && this.lsb == lsb;
    }

    @Deprecated
    public long getMostSignificantBits() {
        return msb;
    }

    @Deprecated
    public long getLeastSignificantBits() {
        return lsb;
    }

    /**
     * Get a random integer. A fast, but lower quality pseudo random number
     * generator is used.
     * 
     * @return a random value.
     */
    private static int randomInt() {
        // There is a race here on concurrent access. However, given the usage the resulting
        // bias seems preferable to the performance penalty of synchronization
        return random = 0xc3e157c1 - rotateLeft(random, 19);
    }

    @Deprecated
    public Segment getSegment() {
        // Sample the segment cache once in a while to get some cache hit/miss statistics
        if ((randomInt() & SEGMENT_CACHE_SAMPLE_MASK) == 0) {
            Segment segment = tracker.getCachedSegment(this);
            if (segment != null) {
                return segment;
            }
        }

        // Fall back to short circuit via this.segment if not in the cache
        Segment segment = this.segment;
        if (segment == null) {
            synchronized (this) {
                segment = this.segment;
                if (segment == null) {
                    log.debug("Loading segment {}", this);
                    segment = tracker.readSegment(this);
                }
            }
        }
        return segment;
    }

    void setSegment(Segment segment) {
        this.segment = segment;
    }

    @Deprecated
    public SegmentTracker getTracker() {
        return tracker;
    }

    @Deprecated
    public long getCreationTime() {
        return creationTime;
    }

    /**
     * Pins this segment so it won't be cleaned by the {@code CLEAN_OLD} strategy.
     */
    void pin() {
        creationTime = Long.MAX_VALUE;
    }

    /**
     * @return  this segment id as UUID
     */
    @Deprecated
    public UUID asUUID() {
        return new UUID(msb, lsb);
    }

    // --------------------------------------------------------< Comparable >--

    @Override
    @Deprecated
    public int compareTo(SegmentId that) {
        int d = Long.valueOf(this.msb).compareTo(Long.valueOf(that.msb));
        if (d == 0) {
            d = Long.valueOf(this.lsb).compareTo(Long.valueOf(that.lsb));
        }
        return d;
    }

    // ------------------------------------------------------------< Object >--

    @Override
    @Deprecated
    public String toString() {
        return new UUID(msb, lsb).toString();
    }

    @Override
    @Deprecated
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        } else if (object instanceof SegmentId) {
            SegmentId that = (SegmentId) object;
            return msb == that.msb && lsb == that.lsb;
        }
        return false;
    }

    @Override
    @Deprecated
    public int hashCode() {
        return (int) lsb;
    }

}
