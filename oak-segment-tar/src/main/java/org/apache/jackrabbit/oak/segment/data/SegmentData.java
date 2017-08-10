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

package org.apache.jackrabbit.oak.segment.data;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import com.google.common.base.Charsets;
import org.apache.commons.io.HexDump;

/**
 * Access the data of a segment.
 * <p>
 * A segment is composed of a header and the proper data. The header has a
 * fixed- and a variable-length part.
 * <p>
 * The fixed-length part of the header contains a {@link #getSignature()
 * signature}, which is a string that uniquely identifies the rest of the
 * content as a segment; a {@link #getVersion()}, which specifies the version of
 * the binary format used to serialize the content of the segment; a {@link
 * #getFullGeneration() full generation}, which describes the generation of the
 * segment with regards to full compaction; a {@link #getGeneration()
 * generation}, which identifies the generation of the segment with regards to
 * full or tail compaction; a {@link #isCompacted() compacted flag}, which
 * determines if the segment was written by a compaction operation; the {@link
 * #getRecordReferencesCount() number of record references}, which is the number
 * of record entries in the segment; the {@link #getSegmentReferencesCount()
 * number of segment references}, which is the number of identifiers of other
 * segments used by this segment.
 * <p>
 * The variable part of the header contains the list of segment references and
 * the list of record references. A segment references is composed by the {@link
 * #getSegmentReferenceMsb(int) most significant bits} and {@link
 * #getSegmentReferenceLsb(int) lsb} of the segment identifier. A record
 * reference is composed of a {@link #getRecordReferenceNumber(int) record
 * number}, a {@link #getRecordReferenceType(int) record type} and a {@link
 * #getRecordReferenceOffset(int) record offset}.
 * <p>
 * The most prominent use for a segment is to hold record data. Many methods of
 * this class allows access to the record data. These methods accept an integer
 * representing an absolute position pointing to the record data. The absolute
 * position, though, is virtual: it is computed on a virtual segment 256K long.
 * This offset is usually obtained by accessing the {@link
 * #getRecordReferenceOffset(int) record offset} of a record reference entry.
 * The class will normalize the offset for the actual size of the segment, which
 * can be smaller than 256K. It is acceptable to displace the offset of a record
 * reference entry by a positive amount. This can be useful to access a field of
 * a composite record saved at a specific offset.
 */
public class SegmentData {

    private static final int HEADER_SIZE = 32;

    private static final int SIGNATURE_OFFSET = 0;

    private static final int SIGNATURE_LENGTH = 3;

    private static final int VERSION_OFFSET = 3;

    private static final int FULL_GENERATION_OFFSET = 4;

    private static final int GENERATION_OFFSET = 10;

    private static final int SEGMENT_REFERENCES_COUNT_OFFSET = 14;

    private static final int SEGMENT_REFERENCE_LENGTH = 16;

    private static final int RECORD_REFERENCES_COUNT_OFFSET = 18;

    private static final int RECORD_REFERENCE_LENGTH = 9;

    // Relative to a segment reference - BEGIN

    private static final int SEGMENT_REFERENCE_MSB_OFFSET = 0;

    private static final int SEGMENT_REFERENCE_LSB_OFFSET = 8;

    // Relative to a segment reference - END

    // Relative to a record reference - BEGIN

    private static final int RECORD_REFERENCE_NUMBER_OFFSET = 0;

    private static final int RECORD_REFERENCE_TYPE_OFFSET = 4;

    private static final int RECORD_REFERENCE_OFFSET_OFFSET = 5;

    // Relative to a record reference - END

    private static final int MAX_SMALL_LENGTH_VALUE = 1 << 7;

    private static final int MAX_MEDIUM_LENGTH_VALUE = (1 << 14) + MAX_SMALL_LENGTH_VALUE;

    private static final int MAX_SEGMENT_SIZE = 1 << 18;

    public static SegmentData newSegmentData(byte[] buffer) {
        return new SegmentData(ByteBuffer.wrap(buffer));
    }

    public static SegmentData newSegmentData(ByteBuffer buffer) {
        return new SegmentData(buffer);
    }

    private SegmentData(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    private final ByteBuffer buffer;

    public byte getVersion() {
        return buffer.get(VERSION_OFFSET);
    }

    public String getSignature() {
        byte[] signature = new byte[SIGNATURE_LENGTH];

        for (int i = 0; i < SIGNATURE_LENGTH; i++) {
            signature[i] = buffer.get(SIGNATURE_OFFSET + i);
        }

        return new String(signature, Charsets.UTF_8);
    }

    public int getFullGeneration() {
        return buffer.getInt(FULL_GENERATION_OFFSET) & 0x7fffffff;
    }

    public boolean isCompacted() {
        return buffer.getInt(FULL_GENERATION_OFFSET) < 0;
    }

    public int getGeneration() {
        return buffer.getInt(GENERATION_OFFSET);
    }

    public int getSegmentReferencesCount() {
        return buffer.getInt(SEGMENT_REFERENCES_COUNT_OFFSET);
    }

    public int getRecordReferencesCount() {
        return buffer.getInt(RECORD_REFERENCES_COUNT_OFFSET);
    }

    private int getRecordReferenceBase(int i) {
        return HEADER_SIZE + getSegmentReferencesCount() * SEGMENT_REFERENCE_LENGTH + i * RECORD_REFERENCE_LENGTH;
    }

    public int getRecordReferenceNumber(int i) {
        return buffer.getInt(getRecordReferenceBase(i) + RECORD_REFERENCE_NUMBER_OFFSET);
    }

    public byte getRecordReferenceType(int i) {
        return buffer.get(getRecordReferenceBase(i) + RECORD_REFERENCE_TYPE_OFFSET);
    }

    public int getRecordReferenceOffset(int i) {
        return buffer.getInt(getRecordReferenceBase(i) + RECORD_REFERENCE_OFFSET_OFFSET);
    }

    private int getSegmentReferenceBase(int i) {
        return HEADER_SIZE + i * SEGMENT_REFERENCE_LENGTH;
    }

    public long getSegmentReferenceMsb(int i) {
        return buffer.getLong(getSegmentReferenceBase(i) + SEGMENT_REFERENCE_MSB_OFFSET);
    }

    public long getSegmentReferenceLsb(int i) {
        return buffer.getLong(getSegmentReferenceBase(i) + SEGMENT_REFERENCE_LSB_OFFSET);
    }

    private int index(int recordReferenceOffset) {
        return buffer.limit() - (MAX_SEGMENT_SIZE - recordReferenceOffset);
    }

    public long readLength(int recordReferenceOffset) {
        return internalReadLength(index(recordReferenceOffset));
    }

    private long internalReadLength(int index) {
        int head = buffer.get(index) & 0xff;

        if ((head & 0x80) == 0) {
            return head;
        }

        if ((head & 0x40) == 0) {
            return MAX_SMALL_LENGTH_VALUE + (buffer.getShort(index) & 0x3fff);
        }

        return MAX_MEDIUM_LENGTH_VALUE + (buffer.getLong(index) & 0x3fffffffffffffffL);
    }

    public StringData readString(int recordReferenceOffset) {
        return internalReadString(index(recordReferenceOffset));
    }

    private StringData internalReadString(int index) {
        long length = internalReadLength(index);

        if (length < MAX_SMALL_LENGTH_VALUE) {
            return internalReadString(index + Byte.BYTES, (int) length);
        }

        if (length < MAX_MEDIUM_LENGTH_VALUE) {
            return internalReadString(index + Short.BYTES, (int) length);
        }

        if (length < Integer.MAX_VALUE) {
            return new StringData(internalReadRecordId(index + Long.BYTES), (int) length);
        }

        throw new IllegalStateException("String is too long: " + length);
    }

    private StringData internalReadString(int index, int length) {
        ByteBuffer duplicate = buffer.duplicate();
        duplicate.position(index);
        duplicate.limit(index + length);
        String string = Charsets.UTF_8.decode(duplicate).toString();
        return new StringData(string, length);
    }

    public RecordIdData readRecordId(int recordReferenceOffset) {
        return internalReadRecordId(index(recordReferenceOffset));
    }

    private RecordIdData internalReadRecordId(int index) {
        int segmentReference = buffer.getShort(index) & 0xffff;
        int recordNumber = buffer.getInt(index + Short.BYTES);
        return new RecordIdData(segmentReference, recordNumber);
    }

    public byte readByte(int recordReferenceOffset) {
        return buffer.get(index(recordReferenceOffset));
    }

    public int readInt(int recordReferenceOffset) {
        return buffer.getInt(index(recordReferenceOffset));
    }

    public short readShort(int recordReferenceOffset) {
        return buffer.getShort(index(recordReferenceOffset));
    }

    public long readLong(int recordReferenceOffset) {
        return buffer.getLong(index(recordReferenceOffset));
    }

    public ByteBuffer readBytes(int recordReferenceOffset, int size) {
        return internalReadBytes(index(recordReferenceOffset), size);
    }

    private ByteBuffer internalReadBytes(int index, int size) {
        ByteBuffer duplicate = buffer.duplicate();
        duplicate.position(index);
        duplicate.limit(index + size);
        return duplicate.slice();
    }

    public int size() {
        return buffer.remaining();
    }

    public void hexDump(OutputStream stream) throws IOException {
        byte[] data = new byte[buffer.remaining()];
        buffer.duplicate().get(data);
        HexDump.dump(data, 0, stream, 0);
    }

    public void binDump(OutputStream stream) throws IOException {
        ByteBuffer data = buffer.duplicate();
        try (WritableByteChannel channel = Channels.newChannel(stream)) {
            while (data.hasRemaining()) {
                channel.write(data);
            }
        }
    }

    public int estimateMemoryUsage() {
        return buffer.isDirect() ? 0 : buffer.remaining();
    }

}
