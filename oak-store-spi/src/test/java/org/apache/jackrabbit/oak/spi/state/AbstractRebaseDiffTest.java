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

package org.apache.jackrabbit.oak.spi.state;

import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.junit.Assert;
import org.junit.Test;

public class AbstractRebaseDiffTest {
    private final NodeState base; {
        NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
        builder.setChildNode("a");
        builder.setChildNode("b");
        builder.setChildNode("c");
        builder.setProperty("x", 1);
        builder.setProperty("y", 1);
        builder.setProperty("z", 1);
        base = builder.getNodeState();
    }

    @Test
    public void addExistingProperty() {
        NodeBuilder headBuilder = base.builder();
        headBuilder.setProperty("p", 1);
        headBuilder.setProperty("p", 1);
        NodeState head = headBuilder.getNodeState();

        NodeBuilder branchBuilder = base.builder();
        branchBuilder.setProperty("p", 1);
        branchBuilder.setProperty("p", 0);
        NodeState branch = branchBuilder.getNodeState();

        RebaseDiff rebaseDiff = new RebaseDiff(head.builder()) {
            @Override
            protected void addExistingProperty(NodeBuilder builder, PropertyState before, PropertyState after) {
                assertEquals(createProperty("p", 0), after);
                resolve();
            }
        };
        branch.compareAgainstBaseState(base, rebaseDiff);
        assertTrue(rebaseDiff.isResolved());
    }

    @Test
    public void changeDeletedProperty() {
        NodeBuilder headBuilder = base.builder();
        headBuilder.removeProperty("x");
        headBuilder.removeProperty("y");
        NodeState head = headBuilder.getNodeState();

        NodeBuilder branchBuilder = base.builder();
        branchBuilder.setProperty("x", 1);
        branchBuilder.setProperty("y", 0);
        NodeState branch = branchBuilder.getNodeState();

        RebaseDiff rebaseDiff = new RebaseDiff(head.builder()) {
            @Override
            protected void changeDeletedProperty(NodeBuilder builder, PropertyState after, PropertyState base) {
                assertEquals(createProperty("y", 0), after);
                resolve();
            }
        };
        branch.compareAgainstBaseState(base, rebaseDiff);
        assertTrue(rebaseDiff.isResolved());
    }

    @Test
    public void changeChangedProperty() {
        NodeBuilder headBuilder = base.builder();
        headBuilder.setProperty("x", 11);
        headBuilder.setProperty("y", 22);
        NodeState head = headBuilder.getNodeState();

        NodeBuilder branchBuilder = base.builder();
        branchBuilder.setProperty("x", 11);
        branchBuilder.setProperty("y", 0);
        NodeState branch = branchBuilder.getNodeState();

        RebaseDiff rebaseDiff = new RebaseDiff(head.builder()) {
            @Override
            protected void changeChangedProperty(NodeBuilder builder, PropertyState before, PropertyState after) {
                assertEquals(createProperty("y", 0), after);
                resolve();
            }
        };
        branch.compareAgainstBaseState(base, rebaseDiff);
        assertTrue(rebaseDiff.isResolved());
    }

    @Test
    public void deleteDeletedProperty() {
        NodeBuilder headBuilder = base.builder();
        headBuilder.removeProperty("x");
        NodeState head = headBuilder.getNodeState();

        NodeBuilder branchBuilder = base.builder();
        branchBuilder.removeProperty("x");
        NodeState branch = branchBuilder.getNodeState();

        RebaseDiff rebaseDiff = new RebaseDiff(head.builder()) {
            @Override
            protected void deleteDeletedProperty(NodeBuilder builder, PropertyState before) {
                assertEquals(createProperty("x", 1), before);
                resolve();
            }
        };
        branch.compareAgainstBaseState(base, rebaseDiff);
        assertTrue(rebaseDiff.isResolved());
    }

    @Test
    public void deleteChangedProperty() {
        NodeBuilder headBuilder = base.builder();
        headBuilder.setProperty("x", 11);
        NodeState head = headBuilder.getNodeState();

        NodeBuilder branchBuilder = base.builder();
        branchBuilder.removeProperty("x");
        NodeState branch = branchBuilder.getNodeState();

        RebaseDiff rebaseDiff = new RebaseDiff(head.builder()) {
            @Override
            protected void deleteChangedProperty(NodeBuilder builder, PropertyState before) {
                assertEquals(createProperty("x", 1), before);
                resolve();
            }
        };
        branch.compareAgainstBaseState(base, rebaseDiff);
        assertTrue(rebaseDiff.isResolved());
    }

    @Test
    public void addExistingNode() {
        NodeBuilder headBuilder = base.builder();
        headBuilder.setChildNode("n");
        headBuilder.setChildNode("m").setChildNode("m1");
        headBuilder.getChildNode("m").setProperty("a", 1);
        headBuilder.getChildNode("m").setProperty("p", 1);
        headBuilder.getChildNode("m").setChildNode("mm").setChildNode("mm1");
        NodeState head = headBuilder.getNodeState();

        NodeBuilder branchBuilder = base.builder();
        branchBuilder.setChildNode("n");
        branchBuilder.setChildNode("m").setChildNode("m2");
        branchBuilder.getChildNode("m").setProperty("a", 1);
        branchBuilder.getChildNode("m").setProperty("q", 1);
        branchBuilder.getChildNode("m").setChildNode("mm").setChildNode("mm2");
        NodeState branch = branchBuilder.getNodeState();

        NodeBuilder builder = head.builder();
        RebaseDiff rebaseDiff = new RebaseDiff(builder);
        branch.compareAgainstBaseState(base, rebaseDiff);

        NodeBuilder expectedBuilder = base.builder();
        expectedBuilder.setChildNode("n");
        expectedBuilder.setChildNode("n");
        expectedBuilder.setChildNode("m").setChildNode("m1");
        expectedBuilder.getChildNode("m").setChildNode("m2");
        expectedBuilder.getChildNode("m").setProperty("a", 1);
        expectedBuilder.getChildNode("m").setProperty("p", 1);
        expectedBuilder.getChildNode("m").setProperty("q", 1);
        expectedBuilder.getChildNode("m").setChildNode("mm").setChildNode("mm1");
        expectedBuilder.getChildNode("m").getChildNode("mm").setChildNode("mm2");

        assertEquals(expectedBuilder.getNodeState(), builder.getNodeState());
    }

    @Test
    public void addExistingNodeWithConflict() {
        NodeBuilder headBuilder = base.builder();
        headBuilder.setChildNode("n");
        headBuilder.setChildNode("m").setChildNode("m1");
        headBuilder.getChildNode("m").setProperty("p", 1);
        headBuilder.getChildNode("m").setChildNode("mm").setChildNode("mmm").setProperty("pp", 1);
        NodeState head = headBuilder.getNodeState();

        NodeBuilder branchBuilder = base.builder();
        branchBuilder.setChildNode("n");
        branchBuilder.setChildNode("m").setChildNode("m2");
        branchBuilder.getChildNode("m").setProperty("q", 1);
        branchBuilder.getChildNode("m").setChildNode("mm").setChildNode("mmm").setProperty("pp", 2);
        NodeState branch = branchBuilder.getNodeState();

        NodeBuilder builder = head.builder();

        class ConflictResolver extends RebaseDiff {
            private final ConflictResolver parent;

            private ConflictResolver(NodeBuilder builder) {
                this(null, builder);
            }

            public ConflictResolver(ConflictResolver parent, NodeBuilder builder) {
                super(builder);
                this.parent = parent;
            }

            @Override
            protected void resolve() {
                if (parent == null) {
                    super.resolve();
                } else {
                    parent.resolve();
                }
            }

            @Override
            protected AbstractRebaseDiff createDiff(NodeBuilder builder, String name) {
                return new ConflictResolver(
                        this, builder.getChildNode(name));
            }

            @Override
            protected void addExistingProperty(NodeBuilder builder,
                    PropertyState before, PropertyState after) {
                assertEquals(createProperty("pp", 1), before);
                assertEquals(createProperty("pp", 2), after);
                resolve();
            }
        }

        RebaseDiff rebaseDiff = new ConflictResolver(builder);
        branch.compareAgainstBaseState(base, rebaseDiff);
        assertTrue(rebaseDiff.isResolved());
    }

    @Test
    public void changeDeletedNode() {
        NodeBuilder headBuilder = base.builder();
        headBuilder.getChildNode("a").remove();
        NodeState head = headBuilder.getNodeState();

        NodeBuilder branchBuilder = base.builder();
        branchBuilder.getChildNode("a").setChildNode("aa");
        NodeState branch = branchBuilder.getNodeState();

        RebaseDiff rebaseDiff = new RebaseDiff(head.builder()) {
            @Override
            protected void changeDeletedNode(NodeBuilder builder, String name, NodeState after, NodeState base) {
                assertEquals("a", name);
                resolve();
            }
        };
        branch.compareAgainstBaseState(base, rebaseDiff);
        assertTrue(rebaseDiff.isResolved());
    }

    @Test
    public void deleteDeletedNode() {
        NodeBuilder headBuilder = base.builder();
        headBuilder.getChildNode("a").remove();
        NodeState head = headBuilder.getNodeState();

        NodeBuilder branchBuilder = base.builder();
        branchBuilder.getChildNode("a").remove();
        NodeState branch = branchBuilder.getNodeState();

        RebaseDiff rebaseDiff = new RebaseDiff(head.builder()) {
            @Override
            protected void deleteDeletedNode(NodeBuilder builder, String name, NodeState before) {
                assertEquals("a", name);
                resolve();
            }
        };
        branch.compareAgainstBaseState(base, rebaseDiff);
        assertTrue(rebaseDiff.isResolved());
    }

    @Test
    public void deleteChangedNode() {
        NodeBuilder headBuilder = base.builder();
        headBuilder.getChildNode("a").setChildNode("aa");
        NodeState head = headBuilder.getNodeState();

        NodeBuilder branchBuilder = base.builder();
        branchBuilder.getChildNode("a").remove();
        NodeState branch = branchBuilder.getNodeState();

        RebaseDiff rebaseDiff = new RebaseDiff(head.builder()) {
            @Override
            protected void deleteChangedNode(NodeBuilder builder, String name, NodeState before) {
                assertEquals("a", name);
                resolve();
            }
        };
        branch.compareAgainstBaseState(base, rebaseDiff);
        assertTrue(rebaseDiff.isResolved());
    }

    //------------------------------------------------------------< RebaseDiff >---

    private static class RebaseDiff extends AbstractRebaseDiff {
        private boolean resolved;

        protected RebaseDiff(NodeBuilder builder) {
            super(builder);
        }

        protected void resolve() {
            resolved = true;
        }

        public boolean isResolved() {
            return resolved;
        }

        @Override
        protected AbstractRebaseDiff createDiff(NodeBuilder builder, String name) {
            return new RebaseDiff(builder.getChildNode(name));
        }

        @Override
        protected void addExistingProperty(NodeBuilder builder, PropertyState before, PropertyState after) {
            Assert.fail("addExistingProperty " + after);
        }

        @Override
        protected void changeDeletedProperty(NodeBuilder builder, PropertyState after, PropertyState base) {
            Assert.fail("changeDeletedProperty " + after);
        }

        @Override
        protected void changeChangedProperty(NodeBuilder builder, PropertyState before, PropertyState after) {
            Assert.fail("changeChangedProperty " + after);
        }

        @Override
        protected void deleteDeletedProperty(NodeBuilder builder, PropertyState before) {
            Assert.fail("deleteDeletedProperty " + before);
        }

        @Override
        protected void deleteChangedProperty(NodeBuilder builder, PropertyState before) {
            Assert.fail("deleteChangedProperty " + before);
        }

        @Override
        protected void addExistingNode(NodeBuilder builder, String name, NodeState before, NodeState after) {
            Assert.fail("addExistingNode " + name + '=' + after);
        }

        @Override
        protected void changeDeletedNode(NodeBuilder builder, String name, NodeState after, NodeState base) {
            Assert.fail("changeDeletedNode " + name + '=' + after);
        }

        @Override
        protected void deleteDeletedNode(NodeBuilder builder, String name, NodeState before) {
            Assert.fail("deleteDeletedNode " + name + '=' + before);
        }

        @Override
        protected void deleteChangedNode(NodeBuilder builder, String name, NodeState before) {
            Assert.fail("deleteChangedNode " + name + '=' + before);
        }
    }
}
