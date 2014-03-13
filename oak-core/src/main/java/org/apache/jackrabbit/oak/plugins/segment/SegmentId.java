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

import static com.google.common.base.Preconditions.checkState;

import java.lang.ref.SoftReference;
import java.util.UUID;

/**
 * Segment identifier.
 */
public class SegmentId implements Comparable<SegmentId> {

    private static final long MSB_MASK = ~(0xfL << 12);

    private static final long VERSION = (0x4L << 12);

    private static final long LSB_MASK = ~(0xfL << 60);

    private static final long DATA = 0xAL << 60;

    private static final long BULK = 0xBL << 60;

    private final SegmentStore store;

    private final long msb;

    private final long lsb;

    private SoftReference<Segment> reference = null;

    SegmentId(SegmentStore store, long msb, long lsb, Segment segment) {
        this.store = store;
        this.msb = msb;
        this.lsb = lsb;
        this.reference = new SoftReference<Segment>(segment);
    }

    SegmentId(SegmentStore store, long msb, long lsb) {
        this.store = store;
        this.msb = msb;
        this.lsb = lsb;
        this.reference = null;
    }

    public SegmentStore getStore() {
        return store;
    }

    /**
     * Checks whether this is a data segment identifier.
     *
     * @return {@code true} for a data segment, {@code false} otherwise
     */
    public boolean isDataSegmentId() {
        return (lsb & ~LSB_MASK) == DATA; 
    }

    /**
     * Checks whether this is a bulk segment identifier.
     *
     * @return {@code true} for a bulk segment, {@code false} otherwise
     */
    public boolean isBulkSegmentId() {
        return (lsb & ~LSB_MASK) == BULK; 
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

    public synchronized Segment getSegment() {
        Segment segment = null;
        if (reference != null) {
            segment = reference.get();
        }
        if (segment == null) {
            segment = store.readSegment(this);
            checkState(segment != null);
            reference = new SoftReference<Segment>(segment);
        }
        return segment;
    }

    synchronized void setSegment(Segment segment) {
        reference = new SoftReference<Segment>(segment);
    }

    //--------------------------------------------------------< Comparable >--

    @Override
    public int compareTo(SegmentId o) {
        return 0;
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
