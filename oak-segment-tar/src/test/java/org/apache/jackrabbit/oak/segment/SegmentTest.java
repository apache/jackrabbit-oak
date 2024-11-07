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

import org.apache.jackrabbit.oak.segment.data.SegmentData;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SegmentTest {
    @Test
    public void createsSegmentFromCustomSegmentData() {
        var segmentId = mock(SegmentId.class);
        var segmentData = mock(SegmentData.class);
        when(segmentData.getVersion()).thenReturn((byte) 12);
        var recordNumbers = mock(RecordNumbers.class);
        var segmentReferences = mock(SegmentReferences.class);

        var segment = new Segment(segmentId, segmentData, recordNumbers, segmentReferences);

        assertEquals(SegmentVersion.V_12, segment.getSegmentVersion());
    }

    @Test
    public void creatingSegmentWithInvalidVersionNumberThrows() {
        var segmentId = mock(SegmentId.class);
        var segmentData = mock(SegmentData.class);
        when(segmentData.getVersion()).thenReturn((byte) 42);
        var recordNumbers = mock(RecordNumbers.class);
        var segmentReferences = mock(SegmentReferences.class);

        assertThrows(IllegalArgumentException.class, () -> new Segment(segmentId, segmentData, recordNumbers, segmentReferences));
    }
}
