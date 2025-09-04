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
import java.nio.charset.StandardCharsets;

import org.apache.jackrabbit.oak.commons.Buffer;

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
public interface SegmentData {

    int MAX_SMALL_LENGTH_VALUE = 1 << 7;

    int MAX_MEDIUM_LENGTH_VALUE = (1 << 14) + MAX_SMALL_LENGTH_VALUE;

    static SegmentData newSegmentData(Buffer buffer) {
        return SegmentDataLoader.newSegmentData(buffer);
    }

    static SegmentData newRawSegmentData(Buffer buffer) {
        return SegmentDataLoader.newRawSegmentData(buffer);
    }

    byte getVersion();

    String getSignature();

    int getFullGeneration();

    boolean isCompacted();

    int getGeneration();

    int getSegmentReferencesCount();

    int getRecordReferencesCount();

    int getRecordReferenceNumber(int i);

    byte getRecordReferenceType(int i);

    int getRecordReferenceOffset(int i);

    long getSegmentReferenceMsb(int i);

    long getSegmentReferenceLsb(int i);

    default long readLength(int recordReferenceOffset) {
        int head = readByte(recordReferenceOffset) & 0xff;

        if ((head & 0x80) == 0) {
            return head;
        }

        if ((head & 0x40) == 0) {
            return MAX_SMALL_LENGTH_VALUE + (readShort(recordReferenceOffset) & 0x3fff);
        }

        return MAX_MEDIUM_LENGTH_VALUE + (readLong(recordReferenceOffset) & 0x3fffffffffffffffL);
    }

    default StringData readString(int recordReferenceOffset) {
        long length = readLength(recordReferenceOffset);

        if (length >= Integer.MAX_VALUE) {
            throw new IllegalStateException("String is too long: " + length + "; possibly trying to read a "
                                            + "BLOB using getString; can not convert BLOB to String");
        }

        if (length >= MAX_MEDIUM_LENGTH_VALUE) {
            return new StringData(readRecordId(recordReferenceOffset + Long.BYTES), (int) length);
        }

        int index = length >= MAX_SMALL_LENGTH_VALUE
                    ? recordReferenceOffset + Short.BYTES
                    : recordReferenceOffset + Byte.BYTES;
        Buffer buffer = readBytes(index, (int) length);
        String string = buffer.decode(StandardCharsets.UTF_8).toString();
        return new StringData(string, (int)length);
    }

    default RecordIdData readRecordId(int recordReferenceOffset) {
        int segmentReference = readShort(recordReferenceOffset) & 0xffff;
        int recordNumber = readInt(recordReferenceOffset + Short.BYTES);
        return new RecordIdData(segmentReference, recordNumber);
    }

    byte readByte(int recordReferenceOffset);

    int readInt(int recordReferenceOffset);

    short readShort(int recordReferenceOffset);

    long readLong(int recordReferenceOffset);

    Buffer readBytes(int recordReferenceOffset, int size);

    int size();

    void hexDump(OutputStream stream) throws IOException;

    void binDump(OutputStream stream) throws IOException;

    int estimateMemoryUsage();

}
