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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.core.ImmutableTree;
import org.apache.jackrabbit.oak.plugins.observation.SecurableValidator;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
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
    public void testRemoveNode() throws CommitFailedException {
        NodeBuilder builder = base.builder();
        builder.child("x").remove();
        new AssertingNodeStateDiff(base, builder.getNodeState()).expect("-x");
    }

    @Test
    public void testAddNode() throws CommitFailedException {
        NodeBuilder builder = base.builder();
        builder.child("v");
        new AssertingNodeStateDiff(base, builder.getNodeState()).expect("+v");
    }

    @Test
    public void testRemoveProperty() throws CommitFailedException {
        NodeBuilder builder = base.builder();
        builder.removeProperty("a");
        new AssertingNodeStateDiff(base, builder.getNodeState()).expect("-a");
    }

    @Test
    public void testAddProperty() throws CommitFailedException {
        NodeBuilder builder = base.builder();
        builder.setProperty("d", "d");
        new AssertingNodeStateDiff(base, builder.getNodeState()).expect("+d");
    }

    @Test
    public void testChangeProperty() throws CommitFailedException {
        NodeBuilder builder = base.builder();
        builder.setProperty("c", 42);
        new AssertingNodeStateDiff(base, builder.getNodeState()).expect("^c");
    }

    @Test
    public void testChangeNode() throws CommitFailedException {
        NodeBuilder builder = base.builder();
        builder.child("NA1").setProperty("p", "p");
        new AssertingNodeStateDiff(base, builder.getNodeState()).expect("+p");
    }

    @Test
    public void testAddInaccessibleChild() throws CommitFailedException {
        NodeBuilder builder = base.builder();
        builder.child("NA3").child("x3").child("NA3a").child("y3").child("NA3a");
        new AssertingNodeStateDiff(base, builder.getNodeState()).expect("");
    }

    @Test
    public void testChangeInaccessibleChild() throws CommitFailedException {
        NodeBuilder builder = base.builder();
        builder.child("NA3").child("x3").child("NA3a").child("y3").remove();
        new AssertingNodeStateDiff(base, builder.getNodeState()).expect("-y3");
    }

    @Test
    public void testRemoveInaccessibleChild() throws CommitFailedException {
        NodeBuilder builder = base.builder();
        builder.child("NA3").child("x3").child("NA3a").remove();
        new AssertingNodeStateDiff(base, builder.getNodeState()).expect("");
    }

    private static class SecureValidator extends SecurableValidator {
        public static SecureValidator wrap(Tree before, Tree after, Validator secureValidator) {
            return new SecureValidator(before, after, secureValidator);
        }

        private SecureValidator(Tree before, Tree after, Validator secureValidator) {
            super(before, after, secureValidator);
        }

        @Override
        protected SecurableValidator create(Tree beforeTree, Tree afterTree,
                Validator secureValidator) {
            return wrap(beforeTree, afterTree, secureValidator);
        }

        @Override
        protected boolean canRead(Tree beforeParent, PropertyState before, Tree afterParent,
                PropertyState after) {
            return canRead(before) && canRead(after);
        }

        private static boolean canRead(PropertyState property) {
            return property == null || canRead(property.getName());
        }

        @Override
        protected boolean canRead(Tree before, Tree after) {
            return canRead(before.getName());
        }

        private static boolean canRead(String name) {
            return name == null || !name.startsWith("NA");
        }
    }

    private static class AssertingNodeStateDiff extends DefaultValidator {
        private final StringBuilder actual = new StringBuilder();
        private final NodeState before;
        private final NodeState after;

        public AssertingNodeStateDiff(NodeState before, NodeState after) {
            this.before = before;
            this.after = after;
        }

        public void expect(String expected) throws CommitFailedException {
            SecureValidator secureDiff = SecureValidator.wrap(
                    new ImmutableTree(before), new ImmutableTree(after), this);
            CommitFailedException exception = EditorDiff.process(secureDiff, before, after);
            if (exception != null) {
                throw exception;
            }
            assertEquals(expected, actual.toString());
        }

        @Override
        public void propertyAdded(PropertyState after) {
            actual.append('+').append(after.getName());
        }

        @Override
        public void propertyChanged(PropertyState before, PropertyState after) {
            actual.append('^').append(after.getName());
        }

        @Override
        public void propertyDeleted(PropertyState before) {
            actual.append('-').append(before.getName());
        }

        @Override
        public Validator childNodeAdded(String name, NodeState after) {
            actual.append('+').append(name);
            return this;
        }

        @Override
        public Validator childNodeChanged(String name, NodeState before, NodeState after) {
            return this;
        }

        @Override
        public Validator childNodeDeleted(String name, NodeState before) {
            actual.append('-').append(name);
            return this;
        }
    }
}
