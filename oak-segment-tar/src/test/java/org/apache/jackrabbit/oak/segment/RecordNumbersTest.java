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

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.data.SegmentData;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RecordNumbersTest {
    @Test
    public void shouldReadRecordNumbersFromSegmentData() {
        SegmentData sampleSegmentData = SegmentData.newSegmentData(Buffer.wrap(new byte[] {
                48, 97, 75, 13, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                -116, -71, -35, -50, 87, -7, 64, 125, -96, -24, -82, 112, 44, -36, 63, 67, 0, 0, 0, 0, 4, 0, 3, -1, -40,
                0, 0, 0, 1, 5, 0, 3, -1, -48, 0, 0, 0, 2, 4, 0, 3, -1, -56, 0, 0, 0, 3, 5, 0, 3, -1, -64, 0, 0, 0, 0, 0,
                0, 0, 0, 0, 0, 0, 0, 42, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 5, 0, 0, 10, -48, -66, -83, 11, -22, -48,
                -66, 37, 123, 34, 119, 105, 100, 34, 58, 34, 116, 34, 44, 34, 115, 110, 111, 34, 58, 50, 44, 34, 116,
                34, 58, 49, 55, 50, 51, 55, 49, 51, 51, 53, 57, 54, 54, 54, 125, 0, 0
        }).asReadOnlyBuffer());

        RecordNumbers recordNumbers = RecordNumbers.fromSegmentData(sampleSegmentData);

        assertEquals(262104, recordNumbers.getOffset(0));
        assertEquals(262096, recordNumbers.getOffset(1));
        assertEquals(262088, recordNumbers.getOffset(2));
        assertEquals(262080, recordNumbers.getOffset(3));
        assertEquals(-1, recordNumbers.getOffset(4));
    }
}
