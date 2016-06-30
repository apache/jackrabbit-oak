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

import static junit.framework.Assert.assertEquals;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.segment.SegmentWriterBuilder.segmentWriterBuilder;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

import java.io.File;
import java.io.IOException;
import java.util.Calendar;
import java.util.Collections;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.util.ISO8601;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;


/**
 * Test case for ensuring that segment size remains within bounds.
 */
public class SegmentSizeTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private FileStore store;

    @Before
    public void setup() throws IOException {
        store = fileStoreBuilder(folder.getRoot()).build();
    }

    @After
    public void tearDown() {
        store.close();
    }

    @Test
    public void testNodeSize() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        expectSize(80, builder);
        expectAmortizedSize(8, builder);

        builder = EMPTY_NODE.builder();
        builder.setProperty("foo", "bar");
        expectSize(96, builder);
        expectAmortizedSize(12, builder);

        builder = EMPTY_NODE.builder();
        builder.setProperty("foo", "bar");
        builder.setProperty("baz", 123);
        expectSize(128, builder);
        expectAmortizedSize(20, builder);

        builder = EMPTY_NODE.builder();
        builder.child("foo");
        expectSize(112, builder);
        expectAmortizedSize(20, builder);

        builder = EMPTY_NODE.builder();
        builder.child("foo");
        builder.child("bar");
        expectSize(144, builder);
        expectAmortizedSize(52, builder);
    }

    @Test
    public void testDuplicateStrings() throws IOException {
        String string = "More than just a few bytes of example content.";

        SegmentWriter writer = new MemoryStore().getWriter();
        SegmentNodeBuilder builder = writer.writeNode(EMPTY_NODE).builder();

        builder.setProperty(PropertyStates.createProperty(
                "test", Collections.nCopies(1, string), Type.STRINGS));
        RecordId id1 = builder.getNodeState().getRecordId();

        builder.setProperty(PropertyStates.createProperty(
                "test", Collections.nCopies(12, string), Type.STRINGS));
        RecordId id2 = builder.getNodeState().getRecordId();
        assertEquals(20 + 12 * Segment.RECORD_ID_BYTES,
                id1.getOffset() - id2.getOffset());

        builder.setProperty(PropertyStates.createProperty(
                "test", Collections.nCopies(100, string), Type.STRINGS));
        RecordId id3 = builder.getNodeState().getRecordId();
        assertEquals(20 + 100 * Segment.RECORD_ID_BYTES,
                id2.getOffset() - id3.getOffset());
    }

    @Test
    public void testDuplicateDates() throws IOException {
        String now = ISO8601.format(Calendar.getInstance());

        SegmentWriter writer = new MemoryStore().getWriter();
        SegmentNodeBuilder builder = writer.writeNode(EMPTY_NODE).builder();

        builder.setProperty(PropertyStates.createProperty(
                "test", Collections.nCopies(1, now), Type.DATES));
        RecordId id1 = builder.getNodeState().getRecordId();

        builder.setProperty(PropertyStates.createProperty(
                "test", Collections.nCopies(12, now), Type.DATES));
        RecordId id2 = builder.getNodeState().getRecordId();
        assertEquals(20 + 12 * Segment.RECORD_ID_BYTES,
                id1.getOffset() - id2.getOffset());

        builder.setProperty(PropertyStates.createProperty(
                "test", Collections.nCopies(100, now), Type.DATES));
        RecordId id3 = builder.getNodeState().getRecordId();
        assertEquals(20 + 100 * Segment.RECORD_ID_BYTES,
                id2.getOffset() - id3.getOffset());
    }

    @Test
    public void testAccessControlNodes() throws IOException {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("jcr:primaryType", "rep:ACL", Type.NAME);
        expectSize(96, builder);
        expectAmortizedSize(8, builder);

        NodeBuilder deny = builder.child("deny");
        deny.setProperty("jcr:primaryType", "rep:DenyACE", Type.NAME);
        deny.setProperty("rep:principalName", "everyone");
        deny.setProperty(PropertyStates.createProperty(
                "rep:privileges", ImmutableList.of("jcr:read"), Type.NAMES));
        expectSize(240, builder);
        expectAmortizedSize(40, builder);

        NodeBuilder allow = builder.child("allow");
        allow.setProperty("jcr:primaryType", "rep:GrantACE");
        allow.setProperty("rep:principalName", "administrators");
        allow.setProperty(PropertyStates.createProperty(
                "rep:privileges", ImmutableList.of("jcr:all"), Type.NAMES));
        expectSize(368, builder);
        expectAmortizedSize(96, builder);

        NodeBuilder deny0 = builder.child("deny0");
        deny0.setProperty("jcr:primaryType", "rep:DenyACE", Type.NAME);
        deny0.setProperty("rep:principalName", "everyone");
        deny0.setProperty("rep:glob", "*/activities/*");
        builder.setProperty(PropertyStates.createProperty(
                "rep:privileges", ImmutableList.of("jcr:read"), Type.NAMES));
        expectSize(480, builder);
        expectAmortizedSize(136, builder);

        NodeBuilder allow0 = builder.child("allow0");
        allow0.setProperty("jcr:primaryType", "rep:GrantACE");
        allow0.setProperty("rep:principalName", "user-administrators");
        allow0.setProperty(PropertyStates.createProperty(
                "rep:privileges", ImmutableList.of("jcr:all"), Type.NAMES));
        expectSize(544, builder);
        expectAmortizedSize(176, builder);
    }

    @Test
    public void testFlatNodeUpdate() throws IOException {
        MemoryStore store = new MemoryStore();
        SegmentWriter writer = store.getWriter();

        NodeBuilder builder = EMPTY_NODE.builder();
        for (int i = 0; i < 1000; i++) {
            builder.child("child" + i);
        }

        SegmentNodeState state = writer.writeNode(builder.getNodeState());
        writer.flush();
        Segment segment = store.readSegment(state.getRecordId().getSegmentId());
        assertEquals(31584, segment.size());

        writer.flush(); // force flushing of the previous segment

        builder = state.builder();
        builder.child("child1000");
        state = writer.writeNode(builder.getNodeState());
        writer.flush();
        segment = store.readSegment(state.getRecordId().getSegmentId());
        assertEquals(560, segment.size());
    }

    private void expectSize(int expectedSize, NodeBuilder builder) throws IOException {
        SegmentWriter writer = segmentWriterBuilder("test").build(store);
        RecordId id = writer.writeNode(builder.getNodeState()).getRecordId();
        writer.flush();
        Segment segment = id.getSegment();
        assertEquals("Unexpected size of segment " + id + " info=" + segment.getSegmentInfo(),
            expectedSize, segment.size());
    }

    private void expectAmortizedSize(int expectedSize, NodeBuilder builder) throws IOException {
        SegmentWriter writer = segmentWriterBuilder("test").build(store);
        NodeState state = builder.getNodeState();
        RecordId id1 = writer.writeNode(state).getRecordId();
        RecordId id2 = writer.writeNode(state).getRecordId();
        assertEquals(expectedSize, id1.getOffset() - id2.getOffset());
    }

}
