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

import static com.google.common.base.Preconditions.checkElementIndex;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;

import java.nio.ByteBuffer;
import java.util.UUID;

import com.google.common.cache.Weigher;

class Segment {

    /**
     * Number of bytes used for storing a record identifier. One byte
     * is used for identifying the segment and two for the record offset
     * within that segment.
     */
    static final int RECORD_ID_BYTES = 1 + 2;

    /**
     * The limit on segment references within one segment. Since record
     * identifiers use one byte to indicate the referenced segment, a single
     * segment can hold references to up to 256 segments.
     */
    static final int SEGMENT_REFERENCE_LIMIT = 1 << 8; // 256

    /**
     * The number of bytes (or bits of address space) to use for the
     * alignment boundary of segment records.
     */
    static final int RECORD_ALIGN_BITS = 2;
    static final int RECORD_ALIGN_BYTES = 1 << RECORD_ALIGN_BITS; // 4

    /**
     * Maximum segment size. Record identifiers are stored as three-byte
     * sequences with the first byte indicating the segment and the next
     * two the offset within that segment. Since all records are aligned
     * at four-byte boundaries, the two bytes can address up to 256kB of
     * record data.
     */
    static final int MAX_SEGMENT_SIZE = 1 << (16 + RECORD_ALIGN_BITS); // 256kB

    /**
     * The size limit for small values. The variable length of small values
     * is encoded as a single byte with the high bit as zero, which gives us
     * seven bits for encoding the length of the value.
     */
    static final int SMALL_LIMIT = 1 << 7;

    /**
     * The size limit for medium values. The variable length of medium values
     * is encoded as two bytes with the highest bits of the first byte set to
     * one and zero, which gives us 14 bits for encoding the length of the
     * value. And since small values are never stored as medium ones, we can
     * extend the size range to cover that many longer values.
     */
    static final int MEDIUM_LIMIT = 1 << (16-2) + SMALL_LIMIT;

    static final Weigher<UUID, Segment> WEIGHER =
            new Weigher<UUID, Segment>() {
                @Override
                public int weigh(UUID key, Segment value) {
                    return value.size();
                }
            };

    private final UUID uuid;

    private final byte[] data;

    private final UUID[] uuids;

    Segment(UUID uuid, byte[] data, UUID[] uuids) {
        this.uuid = uuid;
        this.data = data;
        this.uuids = uuids;
    }

    public UUID getSegmentId() {
        return uuid;
    }

    public byte[] getData() {
        return data;
    }

    public UUID[] getUUIDs() {
        return uuids;
    }

    public int size() {
        return data.length;
    }

    public byte readByte(int position) {
        int pos = position - (MAX_SEGMENT_SIZE - data.length);
        checkElementIndex(pos, data.length);
        return data[pos];
    }

    /**
     * Reads the given number of bytes starting from the given position
     * in this segment.
     *
     * @param position position within segment
     * @param buffer target buffer
     * @param offset offset within target buffer
     * @param length number of bytes to read
     */
    public void readBytes(int position, byte[] buffer, int offset, int length) {
        int pos = position - (MAX_SEGMENT_SIZE - data.length);
        checkPositionIndexes(pos, pos + length, data.length);
        checkNotNull(buffer);
        checkPositionIndexes(offset, offset + length, buffer.length);

        System.arraycopy(data, pos, buffer, offset, length);
    }

    RecordId readRecordId(int position) {
        int pos = position - (MAX_SEGMENT_SIZE - data.length);
        checkPositionIndexes(pos, pos + RECORD_ID_BYTES, data.length);
        return new RecordId(
                uuids[data[pos] & 0xff],
                (data[pos + 1] & 0xff) << (8 + Segment.RECORD_ALIGN_BITS)
                | (data[pos + 2] & 0xff) << Segment.RECORD_ALIGN_BITS);
    }

    public int readInt(int position) {
        int pos = position - (MAX_SEGMENT_SIZE - data.length);
        checkPositionIndexes(pos, pos + 4, data.length);
        return ByteBuffer.wrap(data).getInt(pos);
    }

    public long readLong(int position) {
        int pos = position - (Segment.MAX_SEGMENT_SIZE - data.length);
        checkPositionIndexes(pos, pos + 8, data.length);
        return ByteBuffer.wrap(data).getLong(pos);
    }

}
