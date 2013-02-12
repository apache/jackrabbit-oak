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

import java.util.Collections;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

/**
 * Test case for ensuring that segment size remains within bounds.
 */
public class SegmentSizeTest {

    private static final int BYTES_PER_REFERENCE = 4;

    @Test
    public void testNodeSize() {
        NodeBuilder builder = MemoryNodeState.EMPTY_NODE.builder();
        assertEquals(16, getSize(builder.getNodeState().builder()));

        builder = MemoryNodeState.EMPTY_NODE.builder();
        builder.setProperty("foo", "bar");
        assertEquals(48, getSize(builder));

        builder = MemoryNodeState.EMPTY_NODE.builder();
        builder.setProperty("foo", "bar");
        builder.setProperty("baz", 123);
        assertEquals(80, getSize(builder));

        builder = MemoryNodeState.EMPTY_NODE.builder();
        builder.child("foo");
        assertEquals(48, getSize(builder));

        builder = MemoryNodeState.EMPTY_NODE.builder();
        builder.child("foo");
        builder.child("bar");
        assertEquals(80, getSize(builder));
    }

    @Test
    public void testDuplicateStrings() {
        String string = "More than just a few bytes of example content.";

        NodeBuilder builder = MemoryNodeState.EMPTY_NODE.builder();
        builder.setProperty(PropertyStates.createProperty(
                "test", Collections.nCopies(1, string), Type.STRINGS));
        int base = getSize(builder);

        builder.setProperty(PropertyStates.createProperty(
                "test", Collections.nCopies(10, string), Type.STRINGS));
        assertEquals(base + 10 * BYTES_PER_REFERENCE, getSize(builder));

        builder.setProperty(PropertyStates.createProperty(
                "test", Collections.nCopies(100, string), Type.STRINGS));
        assertEquals(base + 100 * BYTES_PER_REFERENCE, getSize(builder));
    }

    private int getSize(NodeBuilder builder) {
        SegmentStore store = new MemoryStore();
        SegmentWriter writer = new SegmentWriter(store);
        RecordId id = writer.writeNode(builder.getNodeState());
        writer.flush();
        Segment segment = store.readSegment(id.getSegmentId());
        return segment.getData().length;
    }

}
