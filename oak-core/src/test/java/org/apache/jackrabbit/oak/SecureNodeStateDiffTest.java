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

package org.apache.jackrabbit.oak;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.RecursingNodeStateDiff;
import org.apache.jackrabbit.oak.plugins.observation.SecurableNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.junit.Before;
import org.junit.Test;

public class SecureNodeStateDiffTest {
    private NodeState base;

    @Before
    public void setUp() {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("a", 1L);
        builder.setProperty("b", 2L);
        builder.setProperty("c", 3L);
        builder.child("x");
        builder.child("y");
        builder.child("z");
        builder.child("NA1").child("x1");
        builder.child("NA2").child("x2").child("y3");
        builder.child("NA3").child("x3").child("NA3a").child("y3");
        base = builder.getNodeState();
    }

    @Test
    public void testRemoveNode() {
        NodeBuilder builder = base.builder();
        builder.child("x").remove();
        new AssertingNodeStateDiff(base, builder.getNodeState()).expect("-x");
    }

    @Test
    public void testAddNode() {
        NodeBuilder builder = base.builder();
        builder.child("v");
        new AssertingNodeStateDiff(base, builder.getNodeState()).expect("+v");
    }

    @Test
    public void testRemoveProperty() {
        NodeBuilder builder = base.builder();
        builder.removeProperty("a");
        new AssertingNodeStateDiff(base, builder.getNodeState()).expect("-a");
    }

    @Test
    public void testAddProperty() {
        NodeBuilder builder = base.builder();
        builder.setProperty("d", "d");
        new AssertingNodeStateDiff(base, builder.getNodeState()).expect("+d");
    }

    @Test
    public void testChangeProperty() {
        NodeBuilder builder = base.builder();
        builder.setProperty("c", 42);
        new AssertingNodeStateDiff(base, builder.getNodeState()).expect("^c");
    }

    @Test
    public void testChangeNode() {
        NodeBuilder builder = base.builder();
        builder.child("NA1").setProperty("p", "p");
        new AssertingNodeStateDiff(base, builder.getNodeState()).expect("^NA1+p");
    }

    @Test
    public void testAddInaccessibleChild() {
        NodeBuilder builder = base.builder();
        builder.child("NA3").child("x3").child("NA3a").child("y3").child("NA3a");
        new AssertingNodeStateDiff(base, builder.getNodeState()).expect("");
    }

    @Test
    public void testChangeInaccessibleChild() {
        NodeBuilder builder = base.builder();
        builder.child("NA3").child("x3").child("NA3a").child("y3").remove();
        new AssertingNodeStateDiff(base, builder.getNodeState()).expect("^NA3^x3^NA3a-y3");
    }

    @Test
    public void testRemoveInaccessibleChild() {
        NodeBuilder builder = base.builder();
        builder.child("NA3").child("x3").child("NA3a").remove();
        new AssertingNodeStateDiff(base, builder.getNodeState()).expect("");
    }

    private static class SecureNodeStateDiff extends SecurableNodeStateDiff {
        protected SecureNodeStateDiff(SecurableNodeStateDiff parent) {
            super(parent);
        }

        public static NodeStateDiff wrap(RecursingNodeStateDiff diff) {
            return new SecureNodeStateDiff(diff);
        }

        private SecureNodeStateDiff(RecursingNodeStateDiff diff) {
            super(diff);
        }

        @Override
        protected SecurableNodeStateDiff create(SecurableNodeStateDiff parent,
                String name, NodeState before, NodeState after) {
            return new SecureNodeStateDiff(parent);
        }

        @Override
        protected boolean canRead(PropertyState before, PropertyState after) {
            return canRead(before) && canRead(after);
        }

        private static boolean canRead(PropertyState property) {
            return property == null || canRead(property.getName());
        }

        @Override
        protected boolean canRead(String name, NodeState before, NodeState after) {
            return canRead(name);
        }

        private static boolean canRead(String name) {
            return name == null || !name.startsWith("NA");
        }
    }

    private static class AssertingNodeStateDiff extends RecursingNodeStateDiff {
        private final StringBuilder actual = new StringBuilder();
        private final NodeState before;
        private final NodeState after;

        public AssertingNodeStateDiff(NodeState before, NodeState after) {
            this.before = before;
            this.after = after;
        }

        public void expect(String expected) {
            after.compareAgainstBaseState(before, SecureNodeStateDiff.wrap(this));
            assertEquals(expected, actual.toString());
        }

        @Override
        public boolean propertyAdded(PropertyState after) {
            actual.append('+').append(after.getName());
            return true;
        }

        @Override
        public boolean propertyChanged(PropertyState before, PropertyState after) {
            actual.append('^').append(after.getName());
            return true;
        }

        @Override
        public boolean propertyDeleted(PropertyState before) {
            actual.append('-').append(before.getName());
            return true;
        }

        @Override
        public boolean childNodeAdded(String name, NodeState after) {
            actual.append('+').append(name);
            return true;
        }

        @Override
        public boolean childNodeChanged(String name, NodeState before, NodeState after) {
            actual.append('^').append(name);
            return true;
        }

        @Override
        public boolean childNodeDeleted(String name, NodeState before) {
            actual.append('-').append(name);
            return true;
        }
    }
}
