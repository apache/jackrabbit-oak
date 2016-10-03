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

import javax.annotation.Nonnull;

/**
 * Record within a segment.
 */
class Record {

    static boolean fastEquals(Object a, Object b) {
        return a instanceof Record && fastEquals((Record) a, b);
    }

    private static boolean fastEquals(@Nonnull Record a, Object b) {
        return b instanceof Record && fastEquals(a, (Record) b);
    }

    private static boolean fastEquals(@Nonnull Record a, @Nonnull Record b) {
        return a == b || (a.recordNumber == b.recordNumber && a.segmentId.equals(b.segmentId));
    }

    /**
     * Identifier of the segment that contains this record.
     */
    private final SegmentId segmentId;

    /**
     * Segment recordNumber of this record.
     */
    private final int recordNumber;

    /**
     * Creates a new object for the identified record.
     *
     * @param id record identified
     */
    protected Record(@Nonnull RecordId id) {
        this(id.getSegmentId(), id.getRecordNumber());
    }

    protected Record(@Nonnull SegmentId segmentId, int recordNumber) {
        this.segmentId = segmentId;
        this.recordNumber = recordNumber;
    }

    /**
     * Returns the segment that contains this record.
     *
     * @return segment that contains this record
     */
    protected Segment getSegment() {
        return segmentId.getSegment();
    }

    protected int getRecordNumber() {
        return recordNumber;
    }

    /**
     * Returns the identifier of this record.
     *
     * @return record identifier
     */
    public RecordId getRecordId() {
        return new RecordId(segmentId, recordNumber);
    }

    //------------------------------------------------------------< Object >--

    @Override
    public boolean equals(Object that) {
        return fastEquals(this, that);
    }

    @Override
    public int hashCode() {
        return segmentId.hashCode() ^ recordNumber;
    }

    @Override
    public String toString() {
        return getRecordId().toString();
    }

}
