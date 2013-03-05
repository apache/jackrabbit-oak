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

import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

/**
 * Test case for ensuring that segment size remains within bounds.
 */
public class CompareAgainstBaseStateTest {

    private final SegmentStore store = new MemoryStore();

    private final SegmentWriter writer = new SegmentWriter(store);

    private final NodeStateDiff diff = EasyMock.createMock(NodeStateDiff.class);

    private final NodeBuilder builder = MemoryNodeState.EMPTY_NODE.builder();

    @Before
    public void setUp() {
        builder.setProperty("foo", "abc");
        builder.setProperty("bar", 123);
        builder.child("baz");
    }

    @Test
    public void testSameState() {
        NodeState node = persist(builder);

        EasyMock.replay(diff);

        node.compareAgainstBaseState(node, diff);
        EasyMock.verify(diff);
    }

    @Test
    public void testEqualState() {
        NodeState before = persist(builder);
        NodeState after = persist(builder);

        EasyMock.replay(diff);

        after.compareAgainstBaseState(before, diff);
        EasyMock.verify(diff);
    }

    @Test
    public void testPropertyAdded() {
        NodeState before = persist(builder);
        builder.setProperty("test", "test");
        NodeState after = persist(builder);

        diff.propertyAdded(after.getProperty("test"));
        EasyMock.replay(diff);

        after.compareAgainstBaseState(before, diff);
        EasyMock.verify(diff);
    }

    @Test
    public void testPropertyChanged() {
        NodeState before = persist(builder);
        builder.setProperty("foo", "test");
        NodeState after = persist(builder);

        diff.propertyChanged(
                before.getProperty("foo"), after.getProperty("foo"));
        EasyMock.replay(diff);

        after.compareAgainstBaseState(before, diff);
        EasyMock.verify(diff);
    }

    @Test
    public void testPropertyDeleted() {
        NodeState before = persist(builder);
        builder.removeProperty("foo");
        NodeState after = persist(builder);

        diff.propertyDeleted(before.getProperty("foo"));
        EasyMock.replay(diff);

        after.compareAgainstBaseState(before, diff);
        EasyMock.verify(diff);
    }

    @Test
    public void testChildNodeAdded() {
        NodeState before = persist(builder);
        builder.child("test");
        NodeState after = persist(builder);

        diff.childNodeAdded("test", after.getChildNode("test"));
        EasyMock.replay(diff);

        after.compareAgainstBaseState(before, diff);
        EasyMock.verify(diff);
    }

    @Test
    public void testChildNodeChanged() {
        NodeState before = persist(builder);
        builder.child("baz").setProperty("test", "test");
        NodeState after = persist(builder);

        diff.childNodeChanged(
                "baz", before.getChildNode("baz"), after.getChildNode("baz"));
        EasyMock.replay(diff);

        after.compareAgainstBaseState(before, diff);
        EasyMock.verify(diff);
    }

    @Test
    public void testChildNodeDeleted() {
        NodeState before = persist(builder);
        builder.removeNode("baz");
        NodeState after = persist(builder);

        diff.childNodeDeleted("baz", before.getChildNode("baz"));
        EasyMock.replay(diff);

        after.compareAgainstBaseState(before, diff);
        EasyMock.verify(diff);
    }

    private NodeState persist(NodeBuilder builder) {
        NodeState state = writer.writeNode(builder.getNodeState());
        writer.flush();
        return state;
    }

}
