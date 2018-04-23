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

package org.apache.jackrabbit.oak.segment.file.proc;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend.Segment;
import org.junit.Test;

public class SegmentNodeTest {

    @Test
    public void shouldHandleMissingSegment() {
        Backend backend = mock(Backend.class);
        when(backend.getSegment("s")).thenReturn(Optional.empty());

        assertEquals(MissingSegmentNode.class, SegmentNode.newSegmentNode(backend, "s").getClass());
    }

    @Test
    public void shouldHandleDataSegment() {
        Segment segment = mock(Segment.class);
        when(segment.isDataSegment()).thenReturn(true);

        Backend backend = mock(Backend.class);
        when(backend.getSegment("s")).thenReturn(Optional.of(segment));

        assertEquals(DataSegmentNode.class, SegmentNode.newSegmentNode(backend, "s").getClass());
    }

    @Test
    public void shouldHandleBulkSegment() {
        Segment segment = mock(Segment.class);
        when(segment.isDataSegment()).thenReturn(false);

        Backend backend = mock(Backend.class);
        when(backend.getSegment("s")).thenReturn(Optional.of(segment));

        assertEquals(BulkSegmentNode.class, SegmentNode.newSegmentNode(backend, "s").getClass());
    }

}
