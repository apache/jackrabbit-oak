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

package org.apache.jackrabbit.oak.plugins.segment;

import static java.util.Collections.nCopies;
import static org.apache.jackrabbit.oak.api.Type.LONGS;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.NAMES;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.segment.ListRecord.LEVEL_SIZE;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.MEDIUM_LIMIT;
import static org.apache.jackrabbit.oak.plugins.segment.Segment.SMALL_LIMIT;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Random;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.Test;

public class RecordUsageAnalyserTest {
    private SegmentStore store;
    private SegmentWriter writer;
    private RecordUsageAnalyser analyser = new RecordUsageAnalyser();

    @Before
    public void setup() {
        store = mock(SegmentStore.class);
        SegmentTracker tracker = new SegmentTracker(store);
        when(store.getTracker()).thenReturn(tracker);
        writer = new SegmentWriter(store, store.getTracker());
        analyser = new RecordUsageAnalyser();
    }

    @Test
    public void emptyNode() {
        SegmentNodeState node = writer.writeNode(EMPTY_NODE);
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 0, 0, 4, 3);
    }

    @Test
    public void nodeWithInt() {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("one", 1);

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 0, 6, 8, 6);
    }

    @Test
    public void nodeWithString() {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("two", "222");

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 0, 8, 8, 6);
    }

    @Test
    public void nodeWithMediumString() {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("medium", Strings.repeat("a", SMALL_LIMIT + 1));

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 0, 138, 8, 6);
    }

    @Test
    public void nodeWithLargeString() {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("large", Strings.repeat("b", MEDIUM_LIMIT + 1));

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 15, 16530, 8, 6);
    }

    @Test
    public void nodeWithSameString() {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("two", "two");

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 0, 4, 8, 6);
    }

    @Test
    public void nodeWithInts() {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("multi", ImmutableList.of(1L, 2L, 3L, 4L), LONGS);

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 12, 21, 8, 6);
    }

    @Test
    public void nodeWithManyInts() {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("multi", nCopies(LEVEL_SIZE + 1, 1L), LONGS);

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 771, 15, 8, 6);
    }

    @Test
    public void nodeWithManyIntsAndOne() {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("multi", nCopies(LEVEL_SIZE + 2, 1L), LONGS);

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 777, 15, 8, 6);
    }

    @Test
    public void nodeWithStrings() {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("multi", ImmutableList.of("one", "one", "two", "two", "three"), STRINGS);

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 15, 27, 8, 6);
    }

    @Test
    public void nodeWithBlob() {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("blob", new ArrayBasedBlob(new byte[]{1, 2, 3, 4}));

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 0, 10, 8, 6);
    }

    @Test
    public void nodeWithMediumBlob() {
        NodeBuilder builder = EMPTY_NODE.builder();

        byte[] bytes = new byte[SMALL_LIMIT + 1];
        new Random().nextBytes(bytes);
        builder.setProperty("mediumBlob", new ArrayBasedBlob(bytes));

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 0, 142, 8, 6);
    }

    @Test
    public void nodeWithLargeBlob() {
        NodeBuilder builder = EMPTY_NODE.builder();

        byte[] bytes = new byte[MEDIUM_LIMIT + 1];
        new Random().nextBytes(bytes);
        builder.setProperty("largeBlob", new ArrayBasedBlob(bytes));

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 15, 16534, 8, 6);
    }

    @Test
    public void nodeWithPrimaryType() {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("jcr:primaryType", "type", NAME);

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 0, 5, 7, 3);
    }

    @Test
    public void nodeWithMixinTypes() {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("jcr:mixinTypes", ImmutableList.of("type1", "type2"), NAMES);

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 0, 12, 10, 3);
    }

    @Test
    public void singleChild() {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("child");

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 0, 0, 6, 11, 9);
    }

    @Test
    public void multiChild() {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("child1");
        builder.setChildNode("child2");

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 24, 0, 14, 8, 12);
    }

    @Test
    public void manyChild() {
        NodeBuilder builder = EMPTY_NODE.builder();
        for (int k = 0; k < MapRecord.BUCKETS_PER_LEVEL + 1; k++) {
            builder.setChildNode("child" + k);
        }

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 457, 0, 254, 8, 105);
    }

    @Test
    public void changedChild() {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setChildNode("child1");
        builder.setChildNode("child2");

        SegmentNodeState node = writer.writeNode(builder.getNodeState());
        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 24, 0, 14, 8, 12);

        builder = node.builder();
        builder.child("child1").setProperty("p", "q");

        when(store.containsSegment(node.getRecordId().getSegmentId())).thenReturn(true);
        node = (SegmentNodeState) builder.getNodeState();

        analyser.analyseNode(node.getRecordId());
        assertSizes(analyser, 41, 0, 18, 16, 24);
    }

    private static void assertSizes(RecordUsageAnalyser analyser,
            long maps, long lists, long values, long templates, long nodes) {
        assertEquals("maps sizes mismatch", maps, analyser.getMapSize());
        assertEquals("lists sizes mismatch", lists, analyser.getListSize());
        assertEquals("value sizes mismatch", values, analyser.getValueSize());
        assertEquals("template sizes mismatch", templates, analyser.getTemplateSize());
        assertEquals("nodes sizes mismatch", nodes, analyser.getNodeSize());
    }

}
