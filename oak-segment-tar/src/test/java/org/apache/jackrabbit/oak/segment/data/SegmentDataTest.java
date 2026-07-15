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
package org.apache.jackrabbit.oak.segment.data;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.Segment;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SegmentDataTest {
    @Test
    public void readSmallLength() {
        Buffer buffer = Buffer.wrap(new byte[]{0b1111111});
        SegmentData segmentData = new SegmentDataV12(buffer);
        long length = segmentData.readLength(Segment.MAX_SEGMENT_SIZE - buffer.limit());
        assertEquals(127, length);
    }

    @Test
    public void readMediumLength() {
        //                                Medium prefix --
        //                                    Arbitrary    ------          --------
        Buffer buffer = Buffer.wrap(new byte[]{(byte) 0b10_000111, (byte)0b10101010});
        SegmentData segmentData = new SegmentDataV12(buffer);
        long length = segmentData.readLength(Segment.MAX_SEGMENT_SIZE - buffer.limit());

        //                               ------ --------
        assertEquals((1 << 7) + 0b000111_10101010, length);
    }

    @Test
    public void readLongLength() {
        //   Long prefix --
        //     Arbitrary    ------    --------    --------    --------    --------    --------    --------    --------
        Buffer buffer = Buffer.wrap(new byte[]{
                (byte) 0b11_111000, 0b00001111, 0b00101010, 0b00001010, 0b01010101, 0b00010001, 0b01111110, 0b00000000
        });
        SegmentData segmentData = new SegmentDataV12(buffer);
        long length = segmentData.readLength(Segment.MAX_SEGMENT_SIZE - buffer.limit());

        //                                           ------ -------- -------- -------- -------- -------- -------- --------
        assertEquals((1 << 14) + (1 << 7) + 0b111000_00001111_00101010_00001010_01010101_00010001_01111110_00000000L, length);
    }

    @Test(expected = IllegalStateException.class)
    public void readStringWithLengthLargerThanIntegerMaxValueThrows() {
        Buffer buffer = Buffer.wrap(new byte[]{
                (byte)0b111111111,
                (byte)0b111111111,
                (byte)0b111111111,
                (byte)0b111111111,
                (byte)0b111111111,
                (byte)0b111111111,
                (byte)0b111111111,
                (byte)0b111111111,
        });
        SegmentData segmentData = new SegmentDataV12(buffer);
        segmentData.readString(Segment.MAX_SEGMENT_SIZE - buffer.limit());
    }

    @Test
    public void readStringWithSmallLength() {
        Buffer buffer = Buffer.wrap(new byte[]{5, (byte)'H', (byte)'e', (byte)'l', (byte)'l', (byte)'o'});
        SegmentData segmentData = new SegmentDataV12(buffer);
        StringData str = segmentData.readString(Segment.MAX_SEGMENT_SIZE - buffer.limit());
        assertEquals("Hello", str.getString());
        assertEquals(5, str.getLength());
        assertNull(str.getRecordId());
    }

    @Test
    public void readStringWithMediumLength() {
        int stringLength = (1 << 7) + 42;
        Buffer buffer = Buffer.wrap(padRight(
                //                   --                  Medium prefix
                //                      ------        -- 42
                new byte[] {(byte) 0b10_000000, (byte)42},
                stringLength + 2,
                (byte)'x' // 'x' × stringLength
        ));
        SegmentData segmentData = new SegmentDataV12(buffer);

        StringData str = segmentData.readString(Segment.MAX_SEGMENT_SIZE - buffer.limit());
        assertEquals("x".repeat(stringLength), str.getString());
        assertEquals(stringLength, str.getLength());
        assertNull(str.getRecordId());
    }

    @Test
    public void readStringStoredInExternalRecord() {
        int stringLength = (1 << 14) + (1 << 7) + 42;

        //   Long prefix --
        //            42    ------    --------    --------    --------    --------    --------    --------    --------
        Buffer buffer = Buffer.wrap(new byte[]{
                (byte) 0b11_000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00000000, 0b00101010,

                // seg ref       record number
                // -------  ----------------------
                0x11, 0x22, 0x33, 0x44, 0x55, 0x66
        });

        SegmentData segmentData = new SegmentDataV12(buffer);
        StringData str = segmentData.readString(Segment.MAX_SEGMENT_SIZE - buffer.limit());
        assertNull(str.getString());
        assertEquals(0x1122, str.getRecordId().getSegmentReference());
        assertEquals(0x33445566, str.getRecordId().getRecordNumber());
    }

    @Test
    public void readRecordId() {
        //                                      seg. ref.       record number
        //                                     ----------  ----------------------
        Buffer buffer = Buffer.wrap(new byte[]{0x01, 0x02, 0x03, 0x04, 0x05, 0x06});
        SegmentData segmentData = new SegmentDataV12(buffer);
        var recordId = segmentData.readRecordId(Segment.MAX_SEGMENT_SIZE - buffer.limit());
        assertEquals(0x01_02, recordId.getSegmentReference());
        assertEquals(0x03_04_05_06, recordId.getRecordNumber());
    }

    private byte[] padRight(byte[] bytes, int length, byte padding) {
        byte[] padded = new byte[length];
        System.arraycopy(bytes, 0, padded, 0, bytes.length);
        for (int i = bytes.length; i < length; i++) {
            padded[i] = padding;
        }
        return padded;
    }
}
