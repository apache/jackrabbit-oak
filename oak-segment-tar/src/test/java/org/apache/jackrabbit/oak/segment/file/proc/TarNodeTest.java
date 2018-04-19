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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.Set;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend;
import org.apache.jackrabbit.oak.segment.file.proc.Proc.Backend.Segment;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

public class TarNodeTest {

    @Test
    public void shouldExposeSegmentId() {
        Segment segment = mock(Segment.class);

        Backend backend = mock(Backend.class);
        when(backend.segmentExists("t", "s")).thenReturn(true);
        when(backend.getSegment("s")).thenReturn(Optional.of(segment));

        NodeState n = new TarNode(backend, "t");

        assertTrue(n.hasChildNode("s"));
        assertTrue(n.getChildNode("s").exists());
    }

    @Test
    public void shouldNotExposeNonExistingSegmentId() {
        NodeState n = new TarNode(mock(Backend.class), "t");

        assertFalse(n.hasChildNode("s"));
        assertFalse(n.getChildNode("s").exists());
    }

    @Test
    public void shouldExposeAllSegmentIds() {
        Set<String> names = Sets.newHashSet("s1", "s2", "s3");

        Backend backend = mock(Backend.class);
        when(backend.getSegmentIds("t")).thenReturn(names);

        assertEquals(names, Sets.newHashSet(new TarNode(backend, "t").getChildNodeNames()));
    }

    @Test
    public void shouldHaveNameProperty() {
        Backend backend = mock(Backend.class);
        when(backend.getTarSize("t")).thenReturn(Optional.empty());

        PropertyState property = new TarNode(backend, "t").getProperty("name");

        assertEquals(Type.STRING, property.getType());
        assertEquals("t", property.getValue(Type.STRING));
    }

    @Test
    public void shouldHaveSizeProperty() {
        Backend backend = mock(Backend.class);
        when(backend.getTarSize("t")).thenReturn(Optional.of(1L));

        PropertyState property = new TarNode(backend, "t").getProperty("size");

        assertEquals(Type.LONG, property.getType());
        assertEquals(1L, property.getValue(Type.LONG).longValue());
    }

}
