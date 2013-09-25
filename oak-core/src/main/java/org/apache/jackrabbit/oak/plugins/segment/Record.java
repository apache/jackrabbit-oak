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

/**
 * Record within a segment.
 */
class Record {

    /**
     * Identifier of this record.
     */
    private final RecordId id;

    protected Record(RecordId id) {
        this.id = id;
    }

    /**
     * Returns the identifier of this record.
     *
     * @return record identifier
     */
    public RecordId getRecordId() {
        return id;
    }

    /**
     * Returns the segment offset of this record.
     *
     * @return segment offset of this record
     */
    protected int getOffset() {
        return id.getOffset();
    }

    /**
     * Returns the segment offset of the given byte position in this record.
     *
     * @param position byte position within this record
     * @return segment offset of the given byte position
     */
    protected int getOffset(int position) {
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
    protected int getOffset(int bytes, int ids) {
        return getOffset(bytes + ids * Segment.RECORD_ID_BYTES);
    }

}
