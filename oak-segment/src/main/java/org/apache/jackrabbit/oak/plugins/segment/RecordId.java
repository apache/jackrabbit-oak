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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Integer.parseInt;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.RECORD_ALIGN_BITS;

import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The record id. This includes the segment id and the offset within the
 * segment.
 */
@Deprecated
public final class RecordId implements Comparable<RecordId> {

    private static final Pattern PATTERN = Pattern.compile(
            "([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
            + "(:(0|[1-9][0-9]*)|\\.([0-9a-f]{4}))");

    @Deprecated
    public static RecordId[] EMPTY_ARRAY = new RecordId[0];

    @Deprecated
    public static RecordId fromString(SegmentTracker factory, String id) {
        Matcher matcher = PATTERN.matcher(id);
        if (matcher.matches()) {
            UUID uuid = UUID.fromString(matcher.group(1));
            SegmentId segmentId = factory.getSegmentId(
                    uuid.getMostSignificantBits(),
                    uuid.getLeastSignificantBits());

            int offset;
            if (matcher.group(3) != null) {
                offset = parseInt(matcher.group(3));
            } else {
                offset = parseInt(matcher.group(4), 16) << RECORD_ALIGN_BITS;
            }

            return new RecordId(segmentId, offset);
        } else {
            throw new IllegalArgumentException("Bad record identifier: " + id);
        }
    }

    private final SegmentId segmentId;

    private final int offset;

    @Deprecated
    public RecordId(SegmentId segmentId, int offset) {
        checkArgument(offset < Segment.MAX_SEGMENT_SIZE);
        checkArgument((offset % (1 << RECORD_ALIGN_BITS)) == 0);
        this.segmentId = checkNotNull(segmentId);
        this.offset = offset;
    }

    @Deprecated
    public SegmentId getSegmentId() {
        return segmentId;
    }

    @Deprecated
    public int getOffset() {
        return offset;
    }

    /**
     * @return  the segment id part of this record id as UUID
     */
    @Deprecated
    public UUID asUUID() {
        return segmentId.asUUID();
    }

    @Deprecated
    public Segment getSegment() {
        return segmentId.getSegment();
    }

    //--------------------------------------------------------< Comparable >--

    @Override
    @Deprecated
    public int compareTo(RecordId that) {
        checkNotNull(that);
        int diff = segmentId.compareTo(that.segmentId);
        if (diff == 0) {
            diff = offset - that.offset;
        }
        return diff;
    }

    //------------------------------------------------------------< Object >--

    @Override
    @Deprecated
    public String toString() {
        return String.format("%s.%04x", segmentId, offset >> RECORD_ALIGN_BITS);
    }

    /**
     * Returns the record id string representation used in Oak 1.0.
     */
    @Deprecated
    public String toString10() {
        return String.format("%s:%d", segmentId, offset);
    }

    @Override
    @Deprecated
    public int hashCode() {
        return segmentId.hashCode() ^ offset;
    }

    @Override
    @Deprecated
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        } else if (object instanceof RecordId) {
            RecordId that = (RecordId) object;
            return offset == that.offset && segmentId.equals(that.segmentId);
        } else {
            return false;
        }
    }

}
