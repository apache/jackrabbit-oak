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

import static junit.framework.Assert.assertEquals;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.util.Calendar;
import java.util.Collections;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.util.ISO8601;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

/**
 * Test case for ensuring that segment size remains within bounds.
 */
public class SegmentSizeTest {

    @Test
    public void testNodeSize() {
        NodeBuilder builder = EMPTY_NODE.builder();
        assertEquals(40, getSize(builder));
        assertEquals(4, getAmortizedSize(builder));

        builder = EMPTY_NODE.builder();
        builder.setProperty("foo", "bar");
        assertEquals(44, getSize(builder));
        assertEquals(8, getAmortizedSize(builder));

        builder = EMPTY_NODE.builder();
        builder.setProperty("foo", "bar");
        builder.setProperty("baz", 123);
        assertEquals(60, getSize(builder));
        assertEquals(12, getAmortizedSize(builder));

        builder = EMPTY_NODE.builder();
        builder.child("foo");
        assertEquals(60, getSize(builder));
        assertEquals(12, getAmortizedSize(builder));

        builder = EMPTY_NODE.builder();
        builder.child("foo");
        builder.child("bar");
        assertEquals(88, getSize(builder));
        assertEquals(40, getAmortizedSize(builder));
    }

    @Test
    public void testDuplicateStrings() {
        String string = "More than just a few bytes of example content.";

        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty(PropertyStates.createProperty(
                "test", Collections.nCopies(1, string), Type.STRINGS));
        int base = getSize(builder);

        builder.setProperty(PropertyStates.createProperty(
                "test", Collections.nCopies(12, string), Type.STRINGS));
        assertEquals(base + 12 * Segment.RECORD_ID_BYTES, getSize(builder));

        builder.setProperty(PropertyStates.createProperty(
                "test", Collections.nCopies(100, string), Type.STRINGS));
        assertEquals(base + 100 * Segment.RECORD_ID_BYTES, getSize(builder));
    }

    @Test
    public void testDuplicateDates() {
        String now = ISO8601.format(Calendar.getInstance());

        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty(PropertyStates.createProperty(
                "test", Collections.nCopies(1, now), Type.DATES));
        int base = getSize(builder);

        builder.setProperty(PropertyStates.createProperty(
                "test", Collections.nCopies(12, now), Type.DATES));
        assertEquals(base + 12 * Segment.RECORD_ID_BYTES, getSize(builder));

        builder.setProperty(PropertyStates.createProperty(
                "test", Collections.nCopies(100, now), Type.DATES));
        assertEquals(base + 100 * Segment.RECORD_ID_BYTES, getSize(builder));
    }

    @Test
    public void testAccessControlNodes() {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("jcr:primaryType", "rep:ACL", Type.NAME);
        assertEquals(40, getSize(builder));
        assertEquals(4, getAmortizedSize(builder));

        NodeBuilder deny = builder.child("deny");
        deny.setProperty("jcr:primaryType", "rep:DenyACE", Type.NAME);
        deny.setProperty("rep:principalName", "everyone");
        deny.setProperty(PropertyStates.createProperty(
                "rep:privileges", ImmutableList.of("jcr:read"), Type.NAMES));
        assertEquals(164, getSize(builder));
        assertEquals(28, getAmortizedSize(builder));

        NodeBuilder allow = builder.child("allow");
        allow.setProperty("jcr:primaryType", "rep:GrantACE");
        allow.setProperty("rep:principalName", "administrators");
        allow.setProperty(PropertyStates.createProperty(
                "rep:privileges", ImmutableList.of("jcr:all"), Type.NAMES));
        assertEquals(284, getSize(builder));
        assertEquals(72, getAmortizedSize(builder));

        NodeBuilder deny0 = builder.child("deny0");
        deny0.setProperty("jcr:primaryType", "rep:DenyACE", Type.NAME);
        deny0.setProperty("rep:principalName", "everyone");
        deny0.setProperty("rep:glob", "*/activities/*");
        builder.setProperty(PropertyStates.createProperty(
                "rep:privileges", ImmutableList.of("jcr:read"), Type.NAMES));
        assertEquals(376, getSize(builder));
        assertEquals(108, getAmortizedSize(builder));

        NodeBuilder allow0 = builder.child("allow0");
        allow0.setProperty("jcr:primaryType", "rep:GrantACE");
        allow0.setProperty("rep:principalName", "user-administrators");
        allow0.setProperty(PropertyStates.createProperty(
                "rep:privileges", ImmutableList.of("jcr:all"), Type.NAMES));
        assertEquals(432, getSize(builder));
        assertEquals(136, getAmortizedSize(builder));
    }

    @Test
    public void testFlatNodeUpdate() {
        SegmentStore store = new MemoryStore();
        SegmentWriter writer = store.getTracker().getWriter();

        NodeBuilder builder = EMPTY_NODE.builder();
        for (int i = 0; i < 1000; i++) {
            builder.child("child" + i);
        }

        SegmentNodeState state = writer.writeNode(builder.getNodeState());
        writer.flush();
        Segment segment = store.readSegment(state.getRecordId().getSegmentId());
        assertEquals(27520, segment.size());

        writer.flush(); // force flushing of the previous segment

        builder = state.builder();
        builder.child("child1000");
        state = writer.writeNode(builder.getNodeState());
        writer.flush();
        segment = store.readSegment(state.getRecordId().getSegmentId());
        assertEquals(496, segment.size());
    }

    private int getSize(NodeBuilder builder) {
        SegmentStore store = new MemoryStore();
        SegmentWriter writer = store.getTracker().getWriter();
        RecordId id = writer.writeNode(builder.getNodeState()).getRecordId();
        writer.flush();
        Segment segment = store.readSegment(id.getSegmentId());
        return segment.size();
    }

    private int getAmortizedSize(NodeBuilder builder) {
        SegmentStore store = new MemoryStore();
        SegmentWriter writer = store.getTracker().getWriter();
        NodeState state = builder.getNodeState();
        RecordId id = writer.writeNode(state).getRecordId();
        writer.flush();
        Segment segment = store.readSegment(id.getSegmentId());
        int base = segment.size();

        store = new MemoryStore(); // avoid cross-segment caching
        writer = store.getTracker().getWriter();
        writer.writeNode(state);
        id = writer.writeNode(state).getRecordId();
        writer.flush();
        segment = store.readSegment(id.getSegmentId());
        return segment.size() - base - 4;
    }

}
