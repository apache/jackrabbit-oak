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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Segment identifier.
 */
public class SegmentId implements Comparable<SegmentId> {

    /** Logger instance */
    private static final Logger log = LoggerFactory.getLogger(SegmentId.class);

    /**
     * Checks whether this is a data segment identifier.
     *
     * @return {@code true} for a data segment, {@code false} otherwise
     */
    public static boolean isDataSegmentId(long lsb) {
        return (lsb >>> 60) == 0xAL;
    }

    private final SegmentTracker tracker;

    private final long msb;

    private final long lsb;

    private volatile Segment segment;

    public SegmentId(SegmentTracker tracker, long msb, long lsb, Segment segment) {
        this.tracker = tracker;
        this.msb = msb;
        this.lsb = lsb;
        this.segment = segment;
    }

    public SegmentId(SegmentTracker tracker, long msb, long lsb) {
        this(tracker, msb, lsb, null);
    }

    /**
     * Checks whether this is a data segment identifier.
     *
     * @return {@code true} for a data segment, {@code false} otherwise
     */
    public boolean isDataSegmentId() {
        return isDataSegmentId(lsb);
    }

    /**
     * Checks whether this is a bulk segment identifier.
     *
     * @return {@code true} for a bulk segment, {@code false} otherwise
     */
    public boolean isBulkSegmentId() {
        return (lsb >>> 60) == 0xBL; 
    }

    public boolean equals(long msb, long lsb) {
        return this.msb == msb && this.lsb == lsb;
    }

    public long getMostSignificantBits() {
        return msb;
    }

    public long getLeastSignificantBits() {
        return lsb;
    }

    public Segment getSegment() {
        Segment segment = this.segment;
        if (segment == null) {
            synchronized (this) {
                segment = this.segment;
                if (segment == null) {
                    log.debug("Loading segment {}", this);
                    segment = tracker.getSegment(this);
                }
            }
        }
        segment.access();
        return segment;
    }

    synchronized void setSegment(Segment segment) {
        this.segment = segment;
    }

    public SegmentTracker getTracker() {
        return tracker;
    }

    //--------------------------------------------------------< Comparable >--

    @Override
    public int compareTo(SegmentId that) {
        int d = Long.valueOf(this.msb).compareTo(Long.valueOf(that.msb));
        if (d == 0) {
            d = Long.valueOf(this.lsb).compareTo(Long.valueOf(that.lsb)); 
        }
        return d;
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        return new UUID(msb, lsb).toString();
    }

    @Override
    public boolean equals(Object object) {
        return this == object;
    }

    @Override
    public int hashCode() {
        return (int) lsb;
    }

}
