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

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.easymock.EasyMock.createControl;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import java.io.IOException;

import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.junit.Before;
import org.junit.Test;

/**
 * Test case for segment node state comparisons.
 */
public class CompareAgainstBaseStateTest {

    private final NodeStateDiff diff =
            createControl().createMock("diff", NodeStateDiff.class);

    private NodeBuilder builder;

    public CompareAgainstBaseStateTest() throws IOException {
        MemoryStore store = new MemoryStore();
        RecordId id = store.getWriter().writeNode(EMPTY_NODE);
        SegmentNodeState node = new SegmentNodeState(store.getReader(), store.getWriter(), store.getBlobStore(), id);
        builder = node.builder();
    }

    @Before
    public void setUp() {
        builder.setProperty("foo", "abc");
        builder.setProperty("bar", 123);
        builder.child("baz");
    }

    @Test
    public void testSameState() {
        NodeState node = builder.getNodeState();

        replay(diff);

        node.compareAgainstBaseState(node, diff);
        verify(diff);
    }

    @Test
    public void testEqualState() {
        NodeState before = builder.getNodeState();
        NodeState after = before.builder().getNodeState();

        replay(diff);

        after.compareAgainstBaseState(before, diff);
        verify(diff);
    }

    @Test
    public void testPropertyAdded() {
        NodeState before = builder.getNodeState();
        builder = before.builder();
        builder.setProperty("test", "test");
        NodeState after = builder.getNodeState();

        expect(diff.propertyAdded(after.getProperty("test"))).andReturn(true);
        replay(diff);

        after.compareAgainstBaseState(before, diff);
        verify(diff);
    }

    @Test
    public void testPropertyChanged() {
        NodeState before = builder.getNodeState();
        builder = before.builder();
        builder.setProperty("foo", "test");
        NodeState after = builder.getNodeState();

        expect(diff.propertyChanged(
                before.getProperty("foo"), after.getProperty("foo"))).andReturn(true);
        replay(diff);

        after.compareAgainstBaseState(before, diff);
        verify(diff);
    }

    @Test
    public void testPropertyDeleted() {
        NodeState before = builder.getNodeState();
        builder = before.builder();
        builder.removeProperty("foo");
        NodeState after = builder.getNodeState();

        expect(diff.propertyDeleted(before.getProperty("foo"))).andReturn(true);
        replay(diff);

        after.compareAgainstBaseState(before, diff);
        verify(diff);
    }

    @Test
    public void testChildNodeAdded() {
        NodeState before = builder.getNodeState();
        builder = before.builder();
        builder.child("test");
        NodeState after = builder.getNodeState();

        expect(diff.childNodeAdded("test", after.getChildNode("test"))).andReturn(true);
        replay(diff);

        after.compareAgainstBaseState(before, diff);
        verify(diff);
    }

    @Test
    public void testChildNodeChanged() {
        NodeState before = builder.getNodeState();
        builder.child("baz").setProperty("test", "test");
        NodeState after = builder.getNodeState();

        expect(diff.childNodeChanged(
                "baz", before.getChildNode("baz"), after.getChildNode("baz"))).andReturn(true);
        replay(diff);

        after.compareAgainstBaseState(before, diff);
        verify(diff);
    }

    @Test
    public void testChildNodeDeleted() {
        NodeState before = builder.getNodeState();
        builder.getChildNode("baz").remove();
        NodeState after = builder.getNodeState();

        expect(diff.childNodeDeleted("baz", before.getChildNode("baz"))).andReturn(true);
        replay(diff);

        after.compareAgainstBaseState(before, diff);
        verify(diff);
    }
    
    @Test
    public void testChildNodeDeletedAndMultipleAdded() {
        NodeState before = builder.getNodeState();
        builder.getChildNode("baz").remove();
        builder.child("x");
        builder.child("y");
        NodeState after = builder.getNodeState();

        expect(diff.childNodeDeleted("baz", before.getChildNode("baz"))).andReturn(true);
        expect(diff.childNodeAdded("x", after.getChildNode("x"))).andReturn(true);
        expect(diff.childNodeAdded("y", after.getChildNode("x"))).andReturn(true);
        replay(diff);

        after.compareAgainstBaseState(before, diff);
        verify(diff);
    }

}
