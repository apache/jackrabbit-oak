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
import static java.lang.Integer.parseInt;
import static org.apache.jackrabbit.oak.segment.CacheWeights.OBJECT_HEADER_SIZE;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

/**
 * The record id. This includes the segment id and the offset within the
 * segment.
 */
public final class RecordId implements Comparable<RecordId> {

    /**
     * A {@code null} record id not identifying any record.
     */
    public static final RecordId NULL = new RecordId(SegmentId.NULL, 0);

    private static final Pattern PATTERN = Pattern.compile(
            "([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
            + "(:(0|[1-9][0-9]*)|\\.([0-9a-f]{8}))");

    static final int SERIALIZED_RECORD_ID_BYTES = 20;

    public static RecordId[] EMPTY_ARRAY = new RecordId[0];

    public static RecordId fromString(SegmentIdProvider idProvider, String id) {
        Matcher matcher = PATTERN.matcher(id);
        if (matcher.matches()) {
            UUID uuid = UUID.fromString(matcher.group(1));
            SegmentId segmentId = idProvider.newSegmentId(
                    uuid.getMostSignificantBits(),
                    uuid.getLeastSignificantBits());

            int offset;
            if (matcher.group(3) != null) {
                offset = parseInt(matcher.group(3));
            } else {
                offset = parseInt(matcher.group(4), 16);
            }

            return new RecordId(segmentId, offset);
        } else {
            throw new IllegalArgumentException("Bad record identifier: " + id);
        }
    }

    private final SegmentId segmentId;

    private final int offset;

    public RecordId(SegmentId segmentId, int offset) {
        this.segmentId = checkNotNull(segmentId);
        this.offset = offset;
    }

    public SegmentId getSegmentId() {
        return segmentId;
    }

    public int getRecordNumber() {
        return offset;
    }

    /**
     * @return  the segment id part of this record id as UUID
     */
    public UUID asUUID() {
        return segmentId.asUUID();
    }

    @Nonnull
    public Segment getSegment() {
        return segmentId.getSegment();
    }

    /**
     * Serialise this record id into an array of bytes: {@code (msb, lsb, offset >> 2)}
     * @return  this record id as byte array
     */
    @Nonnull
    ByteBuffer getBytes() {
        byte[] buffer = new byte[SERIALIZED_RECORD_ID_BYTES];
        BinaryUtils.writeLong(buffer, 0, segmentId.getMostSignificantBits());
        BinaryUtils.writeLong(buffer, 8, segmentId.getLeastSignificantBits());
        BinaryUtils.writeInt(buffer, 16, offset);
        return ByteBuffer.wrap(buffer);
    }

    //--------------------------------------------------------< Comparable >--

    @Override
    public int compareTo(@Nonnull RecordId that) {
        checkNotNull(that);
        int diff = segmentId.compareTo(that.segmentId);
        if (diff == 0) {
            diff = offset - that.offset;
        }
        return diff;
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        return String.format("%s.%08x", segmentId, offset);
    }

    /**
     * Returns the record id string representation used in Oak 1.0.
     */
    public String toString10() {
        return String.format("%s:%d", segmentId, offset);
    }

    @Override
    public int hashCode() {
        return segmentId.hashCode() ^ offset;
    }

    @Override
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

    public int estimateMemoryUsage() {
        return OBJECT_HEADER_SIZE + 16 + segmentId.estimateMemoryUsage();
    }

}
