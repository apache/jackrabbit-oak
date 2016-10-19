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

import java.util.UUID;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Segment identifier. There are two types of segments: data segments, and bulk
 * segments. Data segments have a header and may reference other segments; bulk
 * segments do not.
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

    @Nonnull
    private final SegmentStore store;

    private final long msb;

    private final long lsb;

    private final long creationTime;

    /**
     * The gc generation of this segment or -1 if unknown.
     */
    private int gcGeneration = -1;

    /**
     * The gc info of this segment if it has been reclaimed or {@code null} otherwise.
     */
    @CheckForNull
    private String gcInfo;

    /**
     * A reference to the segment object, if it is available in memory. It is
     * used for fast lookup.
     */
    private volatile Segment segment;

    public SegmentId(@Nonnull SegmentStore store, long msb, long lsb) {
        this.store = store;
        this.msb = msb;
        this.lsb = lsb;
        this.creationTime = System.currentTimeMillis();
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

    public long getMostSignificantBits() {
        return msb;
    }

    public long getLeastSignificantBits() {
        return lsb;
    }

    /**
     * Get the segment identified by this instance. The segment is memoised in this instance's
     * {@link #segment} field.
     * @return  the segment identified by this instance.
     * @see #loaded(Segment)
     * @see #unloaded()
     */
    @Nonnull
    public Segment getSegment() {
        Segment segment = this.segment;
        if (segment == null) {
            synchronized (this) {
                segment = this.segment;
                if (segment == null) {
                    try {
                        log.debug("Loading segment {}", this);
                        segment = store.readSegment(this);
                    } catch (SegmentNotFoundException snfe) {
                        log.error("Segment not found: {}. {}", this, gcInfo(), snfe);
                        throw snfe;
                    }
                }
            }
        }
        return segment;
    }

    @Nonnull
    private String gcInfo() {
        StringBuilder sb = new StringBuilder();
        sb.append("SegmentId age=").append(System.currentTimeMillis() - creationTime).append("ms");
        if (gcInfo != null) {
            sb.append(",").append(gcInfo);
        }
        if (gcGeneration >= 0) {
            sb.append(",").append("segment-generation=").append(gcGeneration);
        }
        return sb.toString();
    }

    /* For testing only */
    @CheckForNull
    String getGcInfo() {
        return gcInfo;
    }

    /**
     * Notify this id about the reclamation of its segment (e.g. by
     * the garbage collector).
     * @param gcInfo  details about the reclamation. This information
     *                is logged along with the {@code SegmentNotFoundException}
     *                when attempting to resolve the segment of this id.
     */
    public void reclaimed(@Nonnull String gcInfo) {
        this.gcInfo = gcInfo;
    }

    /**
     * This method should only be called from lower level caches to notify this instance that the
     * passed {@code segment} has been loaded and should be memoised.
     * @param segment  segment with this id. If the id doesn't match the behaviour is undefined.
     * @see #getSegment()
     * @see #unloaded()
     */
    void loaded(@Nonnull Segment segment) {
        this.segment = segment;
        this.gcGeneration = segment.getGcGeneration();
    }

    /**
     * This method should only be called from lower level caches to notify this instance that the
     * passed {@code segment} has been unloaded and should no longer be memoised.
     * @see #getSegment()
     * @see #loaded(Segment)
     */
    void unloaded() {
        this.segment = null;
    }

    /**
     * Determine whether this instance belongs to the passed {@code store}
     * @param store
     * @return  {@code true} iff this instance belongs to {@code store}
     */
    public boolean sameStore(@Nonnull SegmentStore store) {
        return this.store == store;
    }

    public long getCreationTime() {
        return creationTime;
    }

    /**
     * @return  this segment id as UUID
     */
    public UUID asUUID() {
        return new UUID(msb, lsb);
    }

    /**
     * Get the underlying segment's gc generation. Might cause the segment to
     * get loaded if the generation info is missing
     * @return the segment's gc generation
     */
    public int getGcGeneration() {
        if (gcGeneration < 0) {
            getSegment();
        }
        return gcGeneration;
    }

    // --------------------------------------------------------< Comparable >--

    @Override
    public int compareTo(SegmentId that) {
        int d = Long.valueOf(this.msb).compareTo(that.msb);
        if (d == 0) {
            d = Long.valueOf(this.lsb).compareTo(that.lsb);
        }
        return d;
    }

    // ------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        return new UUID(msb, lsb).toString();
    }

    @Override
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
    public int hashCode() {
        return (int) lsb;
    }

}
