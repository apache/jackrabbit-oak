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
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.util.Optional;

import com.google.common.collect.Iterables;
import org.apache.commons.io.input.NullInputStream;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend.Segment;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

public class DataSegmentNodeTest {

    private static Segment mockSegment() {
        Segment segment = mock(Segment.class);
        when(segment.getInfo()).thenReturn(Optional.empty());
        return segment;
    }

    private static Backend mockBackend() {
        return mock(Backend.class);
    }

    @Test
    public void shouldHaveGenerationProperty() {
        Segment segment = mockSegment();
        when(segment.getGeneration()).thenReturn(1);

        PropertyState property = new DataSegmentNode(mockBackend(), "s", segment).getProperty("generation");

        assertEquals(Type.LONG, property.getType());
        assertEquals(1, property.getValue(Type.LONG).intValue());
    }

    @Test
    public void shouldHaveFullGenerationProperty() {
        Segment segment = mockSegment();
        when(segment.getFullGeneration()).thenReturn(1);

        PropertyState property = new DataSegmentNode(mockBackend(), "s", segment).getProperty("fullGeneration");

        assertEquals(Type.LONG, property.getType());
        assertEquals(1, property.getValue(Type.LONG).intValue());
    }

    @Test
    public void shouldHaveCompactedProperty() {
        Segment segment = mockSegment();
        when(segment.isCompacted()).thenReturn(true);

        PropertyState property = new DataSegmentNode(mockBackend(), "s", segment).getProperty("compacted");

        assertEquals(Type.BOOLEAN, property.getType());
        assertTrue(property.getValue(Type.BOOLEAN));
    }

    @Test
    public void shouldHaveLengthProperty() {
        Segment segment = mockSegment();
        when(segment.getLength()).thenReturn(1);

        PropertyState property = new DataSegmentNode(mockBackend(), "s", segment).getProperty("length");

        assertEquals(Type.LONG, property.getType());
        assertEquals(1, property.getValue(Type.LONG).intValue());
    }

    @Test
    public void shouldHaveDataProperty() {
        InputStream stream = new NullInputStream(1);

        Segment segment = mockSegment();
        when(segment.getLength()).thenReturn(1);

        Backend backend = mockBackend();
        when(backend.getSegmentData("s")).thenReturn(Optional.of(stream));

        PropertyState property = new DataSegmentNode(backend, "s", segment).getProperty("data");

        assertEquals(Type.BINARY, property.getType());
        assertSame(stream, property.getValue(Type.BINARY).getNewStream());
        assertEquals(1, property.getValue(Type.BINARY).length());
    }

    @Test
    public void shouldHaveIdProperty() {
        PropertyState property = new DataSegmentNode(mockBackend(), "s", mockSegment()).getProperty("id");

        assertEquals(Type.STRING, property.getType());
        assertEquals("s", property.getValue(Type.STRING));
    }

    @Test
    public void shouldHaveVersionProperty() {
        Segment segment = mockSegment();
        when(segment.getVersion()).thenReturn(1);

        PropertyState property = new DataSegmentNode(mockBackend(), "s", segment).getProperty("version");

        assertEquals(Type.LONG, property.getType());
        assertEquals(1, property.getValue(Type.LONG).longValue());
    }

    @Test
    public void shouldHaveIsDataSegmentProperty() {
        PropertyState property = new DataSegmentNode(mockBackend(), "s", mockSegment()).getProperty("isDataSegment");

        assertEquals(Type.BOOLEAN, property.getType());
        assertTrue(property.getValue(Type.BOOLEAN));
    }

    @Test
    public void shouldHaveInfoProperty() {
        Segment segment = mockSegment();
        when(segment.getInfo()).thenReturn(Optional.of("info"));

        PropertyState property = new DataSegmentNode(mockBackend(), "s", segment).getProperty("info");

        assertEquals(Type.STRING, property.getType());
        assertEquals("info", property.getValue(Type.STRING));
    }

    @Test
    public void shouldHaveExistsProperty() {
        PropertyState property = new DataSegmentNode(mockBackend(), "s", mockSegment()).getProperty("exists");

        assertEquals(Type.BOOLEAN, property.getType());
        assertEquals(true, property.getValue(Type.BOOLEAN));
    }

    @Test
    public void shouldExposeReferences() {
        NodeState n = new DataSegmentNode(mockBackend(), "s", mockSegment());
        assertTrue(Iterables.contains(n.getChildNodeNames(), "references"));
    }

    @Test
    public void shouldExposeRecordsNode() {
        NodeState n = new DataSegmentNode(mockBackend(), "s", mockSegment());
        assertTrue(Iterables.contains(n.getChildNodeNames(), "records"));
    }

}
