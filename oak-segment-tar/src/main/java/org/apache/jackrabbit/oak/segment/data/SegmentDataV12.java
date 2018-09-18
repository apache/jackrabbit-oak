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

import com.google.common.base.Charsets;

class SegmentDataV12 implements SegmentData {

    private static final int HEADER_SIZE = 32;

    private static final int SIGNATURE_OFFSET = 0;

    private static final int SIGNATURE_LENGTH = 3;

    private static final int VERSION_OFFSET = 3;

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

    final ByteBuffer buffer;

    SegmentDataV12(ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public byte getVersion() {
        return buffer.get(VERSION_OFFSET);
    }

    @Override
    public String getSignature() {
        byte[] signature = new byte[SIGNATURE_LENGTH];

        for (int i = 0; i < SIGNATURE_LENGTH; i++) {
            signature[i] = buffer.get(SIGNATURE_OFFSET + i);
        }

        return new String(signature, Charsets.UTF_8);
    }

    @Override
    public int getFullGeneration() {
        return getGeneration();
    }

    @Override
    public boolean isCompacted() {
        return true;
    }

    @Override
    public int getGeneration() {
        return buffer.getInt(GENERATION_OFFSET);
    }

    @Override
    public int getSegmentReferencesCount() {
        return buffer.getInt(SEGMENT_REFERENCES_COUNT_OFFSET);
    }

    @Override
    public int getRecordReferencesCount() {
        return buffer.getInt(RECORD_REFERENCES_COUNT_OFFSET);
    }

    private int getRecordReferenceBase(int i) {
        return HEADER_SIZE + getSegmentReferencesCount() * SEGMENT_REFERENCE_LENGTH + i * RECORD_REFERENCE_LENGTH;
    }

    @Override
    public int getRecordReferenceNumber(int i) {
        return buffer.getInt(getRecordReferenceBase(i) + RECORD_REFERENCE_NUMBER_OFFSET);
    }

    @Override
    public byte getRecordReferenceType(int i) {
        return buffer.get(getRecordReferenceBase(i) + RECORD_REFERENCE_TYPE_OFFSET);
    }

    @Override
    public int getRecordReferenceOffset(int i) {
        return buffer.getInt(getRecordReferenceBase(i) + RECORD_REFERENCE_OFFSET_OFFSET);
    }

    private int getSegmentReferenceBase(int i) {
        return HEADER_SIZE + i * SEGMENT_REFERENCE_LENGTH;
    }

    @Override
    public long getSegmentReferenceMsb(int i) {
        return buffer.getLong(getSegmentReferenceBase(i) + SEGMENT_REFERENCE_MSB_OFFSET);
    }

    @Override
    public long getSegmentReferenceLsb(int i) {
        return buffer.getLong(getSegmentReferenceBase(i) + SEGMENT_REFERENCE_LSB_OFFSET);
    }

    private int index(int recordReferenceOffset) {
        return SegmentDataUtils.index(buffer, recordReferenceOffset);
    }

    @Override
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

    @Override
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

    @Override
    public RecordIdData readRecordId(int recordReferenceOffset) {
        return internalReadRecordId(index(recordReferenceOffset));
    }

    private RecordIdData internalReadRecordId(int index) {
        int segmentReference = buffer.getShort(index) & 0xffff;
        int recordNumber = buffer.getInt(index + Short.BYTES);
        return new RecordIdData(segmentReference, recordNumber);
    }

    @Override
    public byte readByte(int recordReferenceOffset) {
        return buffer.get(index(recordReferenceOffset));
    }

    @Override
    public int readInt(int recordReferenceOffset) {
        return buffer.getInt(index(recordReferenceOffset));
    }

    @Override
    public short readShort(int recordReferenceOffset) {
        return buffer.getShort(index(recordReferenceOffset));
    }

    @Override
    public long readLong(int recordReferenceOffset) {
        return buffer.getLong(index(recordReferenceOffset));
    }

    @Override
    public ByteBuffer readBytes(int recordReferenceOffset, int size) {
        return SegmentDataUtils.readBytes(buffer, index(recordReferenceOffset), size);
    }

    @Override
    public int size() {
        return buffer.remaining();
    }

    @Override
    public void hexDump(OutputStream stream) throws IOException {
        SegmentDataUtils.hexDump(buffer, stream);
    }

    @Override
    public void binDump(OutputStream stream) throws IOException {
        SegmentDataUtils.binDump(buffer, stream);
    }

    @Override
    public int estimateMemoryUsage() {
        return SegmentDataUtils.estimateMemoryUsage(buffer);
    }

}
