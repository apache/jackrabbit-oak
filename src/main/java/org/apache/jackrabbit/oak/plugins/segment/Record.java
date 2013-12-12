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

import static com.google.common.base.Objects.equal;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.UUID;

import javax.annotation.Nonnull;

/**
 * Record within a segment.
 */
class Record {

    static boolean fastEquals(Object a, Object b) {
        return a instanceof Record && fastEquals((Record) a, b);
    }

    static boolean fastEquals(Record a, Object b) {
        return b instanceof Record && fastEquals(a, (Record) b);
    }

    static boolean fastEquals(Record a, Record b) {
        return a.offset == b.offset && equal(a.uuid, b.uuid);
    }

    /**
     * The segment that contains this record, or initially some other segment
     * in the same store. The reference is lazily updated when the
     * {@link #getSegment()} method is first called to prevent the potentially
     * costly pre-loading of segments that might actually not be needed.
     */
    private volatile Segment segment;

    /**
     * Identifier of the segment that contains this record. The value of
     * this identifier never changes, but the exact instance reference may
     * get updated by the {@link #getSegment()} method to indicate that
     * lazy initialization has happened.
     */
    private volatile UUID uuid;

    /**
     * Segment offset of this record.
     */
    private final int offset;

    /**
     * Creates a new object for the identified record. The segment from which
     * the record identifier was read is also given as it either directly
     * contains the identified record (common case) or can be used to look
     * up the segment that contains the record.
     *
     * @param segment from which the record identifier was read
     * @param id record identified
     */
    protected Record(@Nonnull Segment segment, @Nonnull RecordId id) {
        this.segment = checkNotNull(segment);

        checkNotNull(id);
        if (equal(id.getSegmentId(), segment.getSegmentId())) {
            this.uuid = segment.getSegmentId();
        } else {
            this.uuid = id.getSegmentId();
        }
        this.offset = id.getOffset();
    }

    protected Record(@Nonnull Segment segment, int offset) {
        this.segment = checkNotNull(segment);
        this.uuid = segment.getSegmentId();
        this.offset = offset;
    }

    /**
     * Returns the store that contains this record.
     *
     * @return containing segment store
     */
    @Nonnull
    protected SegmentStore getStore() {
        return segment.getStore();
    }

    /**
     * Returns the segment that contains this record.
     *
     * @return segment that contains this record
     */
    protected synchronized Segment getSegment() {
        if (uuid != segment.getSegmentId()) {
            segment = segment.getSegment(uuid);
            checkState(uuid.equals(segment.getSegmentId()));
            uuid = segment.getSegmentId();
        }
        return segment;
    }

    /**
     * Returns the identifier of this record.
     *
     * @return record identifier
     */
    public RecordId getRecordId() {
        return new RecordId(uuid, offset);
    }

    /**
     * Returns the segment offset of this record.
     *
     * @return segment offset of this record
     */
    protected final int getOffset() {
        return offset;
    }

    /**
     * Returns the segment offset of the given byte position in this record.
     *
     * @param position byte position within this record
     * @return segment offset of the given byte position
     */
    protected final int getOffset(int position) {
        return getOffset() + position;
    }

    /**
     * Returns the segment offset of a byte position in this record.
     * The position is calculated from the given number of raw bytes and
     * record identifiers.
     *
     * @param bytes number of raw bytes before the position
     * @param ids number of record identifiers before the position
     * @return segment offset of the specified byte position
     */
    protected final int getOffset(int bytes, int ids) {
        return getOffset(bytes + ids * Segment.RECORD_ID_BYTES);
    }

    //------------------------------------------------------------< Object >--

    @Override
    public boolean equals(Object object) {
        if (object == this) {
            return true;
        } else if (object instanceof Record) {
            Record that = (Record) object;
            return offset == that.offset && uuid.equals(that.uuid);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return uuid.hashCode() ^ offset;
    }

    @Override
    public String toString() {
        return getRecordId().toString();
    }

}
