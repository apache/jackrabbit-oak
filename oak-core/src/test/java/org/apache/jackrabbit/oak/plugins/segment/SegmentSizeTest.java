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

import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

/**
 * Test case for ensuring that segment size remains within bounds.
 */
public class SegmentSizeTest {

    private SegmentStore store = new MemoryStore();

    private SegmentWriter writer = new SegmentWriter(store);

    @Test
    public void testNodeSize() {
        NodeBuilder builder = MemoryNodeState.EMPTY_NODE.builder();
        assertNodeSize(16, builder.getNodeState());

        builder = MemoryNodeState.EMPTY_NODE.builder();
        builder.setProperty("foo", "bar");
        assertNodeSize(70, builder.getNodeState());

        builder = MemoryNodeState.EMPTY_NODE.builder();
        builder.setProperty("foo", "bar");
        builder.setProperty("baz", 123);
        assertNodeSize(124, builder.getNodeState());

        builder = MemoryNodeState.EMPTY_NODE.builder();
        builder.child("foo");
        assertNodeSize(59, builder.getNodeState());

        builder = MemoryNodeState.EMPTY_NODE.builder();
        builder.child("foo");
        builder.child("bar");
        assertNodeSize(102, builder.getNodeState());

    }

    private void assertNodeSize(int expected, NodeState state) {
        RecordId id = writer.writeNode(state);
        writer.flush();
        Segment segment = store.readSegment(id.getSegmentId());
        assertEquals(expected, segment.getData().length);
    }
}
