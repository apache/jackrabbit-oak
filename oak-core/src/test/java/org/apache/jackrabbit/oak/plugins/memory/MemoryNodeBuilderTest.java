/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.memory;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.util.Collection;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class MemoryNodeBuilderTest {

    private final NodeState base;

    public MemoryNodeBuilderTest(NodeState base) {
        this.base = base;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> fixtures() {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("a", 1L);
        builder.setProperty("b", 2L);
        builder.setProperty("c", 3L);
        builder.child("x").child("q");
        builder.child("y");
        builder.child("z");
        NodeState base = builder.getNodeState();
        return ImmutableList.of(
            new Object[] { base },
            new Object[] { ModifiedNodeState.squeeze(base) }
        );
    }

    @Test
    public void testConnectOnAddProperty() {
        NodeBuilder root = base.builder();
        NodeBuilder childA = root.child("x");
        NodeBuilder childB = root.child("x");

        assertFalse(childA.hasProperty("test"));
        childB.setProperty("test", "foo");
        assertTrue(childA.hasProperty("test"));
    }

    @Test
    public void testConnectOnUpdateProperty() {
        NodeBuilder root = base.builder();
        NodeBuilder childA = root.child("x");
        NodeBuilder childB = root.child("x");

        childB.setProperty("test", "foo");

        childA.setProperty("test", "bar");
        assertEquals("bar", childA.getProperty("test").getValue(STRING));
        assertEquals("bar", childB.getProperty("test").getValue(STRING));
    }

    @Test
    public void testConnectOnRemoveProperty() {
        NodeBuilder root = base.builder();
        NodeBuilder childA = root.child("x");
        NodeBuilder childB = root.child("x");

        childB.setProperty("test", "foo");

        childA.removeProperty("test");
        assertFalse(childA.hasProperty("test"));
        assertFalse(childB.hasProperty("test"));

        childA.setProperty("test", "bar");
        assertEquals("bar", childA.getProperty("test").getValue(STRING));
        assertEquals("bar", childB.getProperty("test").getValue(STRING));
    }

    @Test
    public void testConnectOnAddNode() {
        NodeBuilder root = base.builder();
        NodeBuilder childA = root.child("x");
        NodeBuilder childB = root.child("x");

        assertFalse(childA.hasChildNode("test"));
        assertFalse(childB.hasChildNode("test"));

        childB.child("test");
        assertTrue(childA.hasChildNode("test"));
        assertTrue(childB.hasChildNode("test"));
    }

    @Test
    public void testWriteOnRemoveNode() {
        for (String name : new String[] {"x", "new"}) {
            NodeBuilder root = base.builder();
            NodeBuilder child = root.child(name);

            root.getChildNode(name).remove();
            try {
                child.setProperty("q", "w");
                fail();
            } catch (IllegalStateException e) {
                // expected
            }

            root.child(name);
            assertEquals(0, child.getChildNodeCount(1)); // reconnect!
        }
    }

    @Test
    public void testAddRemovedNodeAgain() {
        NodeBuilder root = base.builder();

        root.getChildNode("x").remove();
        NodeBuilder x = root.child("x");

        x.child("q");
        assertTrue(x.hasChildNode("q"));
    }

    @Test
    public void testReset() {
        NodeBuilder root = base.builder();
        NodeBuilder child = root.child("x");
        child.child("new");

        assertTrue(child.hasChildNode("new"));
        assertTrue(root.child("x").hasChildNode("new"));

        ((MemoryNodeBuilder) root).reset(base);
        assertFalse(child.hasChildNode("new"));
        assertFalse(root.child("x").hasChildNode("new"));
    }

    @Test
    public void testReset2() {
        NodeBuilder root = base.builder();
        NodeBuilder x = root.child("x");
        x.child("y");

        ((MemoryNodeBuilder) root).reset(base);
        assertTrue(root.hasChildNode("x"));
        assertFalse(x.hasChildNode("y"));
    }

    @Test
    public void testUnmodifiedEqualsBase() {
        NodeBuilder root = base.builder();
        NodeBuilder x = root.child("x");
        assertEquals(x.getBaseState(), x.getNodeState());
    }

    @Test
    public void transitiveRemove() {
        NodeBuilder root = base.builder();
        NodeBuilder x = root.getChildNode("x");
        NodeBuilder q = x.getChildNode("q");

        assertTrue(x.exists());
        assertTrue(q.exists());

        root.getChildNode("x").remove();
        assertFalse(q.exists());
        assertFalse(x.exists());
    }

    @Test
    public void testExistingStatus() {
        NodeBuilder root = base.builder();
        NodeBuilder x = root.child("x");
        assertTrue(x.exists());
        assertFalse(x.isNew());
        assertFalse(x.isModified());
    }

    @Test
    public void testModifiedStatus() {
        NodeBuilder root = base.builder();
        NodeBuilder x = root.child("x");
        x.setProperty("p", "pValue");
        assertTrue(x.exists());
        assertFalse(x.isNew());
        assertTrue(x.isModified());
    }

    @Test
    public void testReplacedStatus() {
        NodeBuilder root = base.builder();
        NodeBuilder x = root.getChildNode("x");
        x.setChildNode("new");
        assertFalse(x.isReplaced());
    }

    @Test
    public void testReplacedStatus2() {
        NodeBuilder x = base.builder().getChildNode("x");
        NodeBuilder q = x.getChildNode("q");
        q.remove();
        assertFalse(q.isReplaced());
        x.setChildNode("q").setProperty("a", "b");
        assertTrue(q.isReplaced());
    }

    @Test
    public void testReplacedStatus3() {
        NodeBuilder x = base.builder().getChildNode("x");
        NodeBuilder q = x.getChildNode("q");
        assertFalse(q.isReplaced());
        x.setChildNode("q").setProperty("a", "b");
        assertTrue(q.isReplaced());
    }

    @Test
    public void removeParent() {
        NodeBuilder x = base.builder().getChildNode("x");
        NodeBuilder y = x.setChildNode("y");
        x.remove();
        assertFalse(x.exists());
    }

    @Test
    public void testRemovedStatus() {
        NodeBuilder root = base.builder();
        NodeBuilder x = root.child("x");
        root.getChildNode("x").remove();
        assertFalse(x.exists());
        assertFalse(x.isNew());
        assertFalse(x.isModified());
    }

    @Test
    public void testNewStatus() {
        NodeBuilder root = base.builder();
        NodeBuilder n = root.child("n");
        assertTrue(n.exists());
        assertTrue(n.isNew());
        assertFalse(n.isModified());
    }

    @Test
    public void getExistingChildTest() {
        NodeBuilder rootBuilder = base.builder();
        NodeBuilder x = rootBuilder.getChildNode("x");
        assertTrue(x.exists());
        assertTrue(x.getNodeState().exists());
    }

    @Test
    public void getNonExistingChildTest() {
        NodeBuilder rootBuilder = base.builder();
        NodeBuilder any = rootBuilder.getChildNode("any");
        assertFalse(any.getChildNode("other").exists());
        assertFalse(any.exists());
        try {
            any.setChildNode("any");
            fail();
        } catch (IllegalStateException expected) {}
    }

    @Test
    public void addExistingChildTest() {
        NodeBuilder rootBuilder = base.builder();
        NodeBuilder x = rootBuilder.setChildNode("x");
        assertTrue(x.exists());
        assertTrue(x.getBaseState().exists());
    }

    @Test
    public void addNewChildTest() {
        NodeBuilder rootBuilder = base.builder();
        NodeBuilder x = rootBuilder.setChildNode("any");
        assertTrue(x.exists());
        assertTrue(x.getNodeState().exists());
    }

    @Test
    public void existingChildTest() {
        NodeBuilder rootBuilder = base.builder();
        NodeBuilder x = rootBuilder.child("x");
        assertTrue(x.exists());
        assertTrue(x.getBaseState().exists());
    }

    @Test
    public void newChildTest() {
        NodeBuilder rootBuilder = base.builder();
        NodeBuilder x = rootBuilder.child("any");
        assertTrue(x.exists());
        assertTrue(x.getNodeState().exists());
    }

    @Test
    public void setNodeTest() {
        NodeBuilder rootBuilder = EMPTY_NODE.builder();

        // +"/a":{"c":{"c"="cValue"}}
        rootBuilder.setChildNode("a", createBC(true));

        NodeState c = rootBuilder.getNodeState().getChildNode("a").getChildNode("c");
        assertTrue(c.hasProperty("c"));

        rootBuilder.child("a").child("c").setProperty("c2", "c2Value");

        c = rootBuilder.getNodeState().getChildNode("a").getChildNode("c");
        assertTrue(c.hasProperty("c"));
        assertTrue(c.hasProperty("c2"));
    }

    @Test
    public void setTest() {
        Assume.assumeTrue(EMPTY_NODE.builder() instanceof MemoryNodeBuilder);
        MemoryNodeBuilder rootBuilder = (MemoryNodeBuilder) EMPTY_NODE.builder();
        assertFalse(base.equals(rootBuilder.getNodeState()));
        rootBuilder.set(base);
        assertTrue(base.equals(rootBuilder.getNodeState()));

        MemoryNodeBuilder xBuilder = (MemoryNodeBuilder) rootBuilder.getChildNode("x");
        NodeState yState = base.getChildNode("y");
        assertFalse(yState.equals(xBuilder.getNodeState()));
        xBuilder.set(yState);
        assertTrue(yState.equals(xBuilder.getNodeState()));
    }

    @Test
    public void testMove() {
        NodeBuilder rootBuilder = base.builder();
        assertTrue(rootBuilder.getChildNode("y").moveTo(rootBuilder.child("x"), "yy"));

        NodeState newRoot = rootBuilder.getNodeState();
        assertFalse(newRoot.hasChildNode("y"));
        assertTrue(newRoot.hasChildNode("x"));
        assertTrue(newRoot.getChildNode("x").hasChildNode("q"));
        assertTrue(newRoot.getChildNode("x").hasChildNode("yy"));
    }

    @Test
    public void testRename() {
        NodeBuilder rootBuilder = base.builder();
        assertTrue(rootBuilder.getChildNode("y").moveTo(rootBuilder, "yy"));

        NodeState newRoot = rootBuilder.getNodeState();
        assertFalse(newRoot.hasChildNode("y"));
        assertTrue(newRoot.hasChildNode("yy"));
    }

    @Test
    public void testMoveToSelf() {
        NodeBuilder rootBuilder = base.builder();
        assertFalse(rootBuilder.getChildNode("y").moveTo(rootBuilder, "y"));

        NodeState newRoot = rootBuilder.getNodeState();
        assertTrue(newRoot.hasChildNode("y"));
    }

    @Test
    public void testMoveToDescendant() {
        NodeBuilder rootBuilder = base.builder();
        assertTrue(rootBuilder.getChildNode("x").moveTo(rootBuilder.getChildNode("x"), "xx"));
        assertFalse(rootBuilder.hasChildNode("x"));
    }

    @Test
    public void assertion_OAK781() {
        NodeBuilder rootBuilder = EMPTY_NODE.builder();
        rootBuilder.child("a").setChildNode("b", createBC(false));

        NodeState r = rootBuilder.getNodeState();
        NodeState a = r.getChildNode("a");
        NodeState b = a.getChildNode("b");
        NodeState c = b.getChildNode("c");

        assertTrue(a.exists());
        assertFalse(b.exists());
        assertTrue(c.exists());

        // No assertion must fail in .child("c")
        rootBuilder.child("a").child("b").child("c");

        rootBuilder = new MemoryNodeBuilder(r);

        // No assertion must fail in .child("c")
        rootBuilder.child("a").child("b").child("c");
    }

    @Test
    public void modifyChildNodeOfNonExistingNode() {
        NodeBuilder rootBuilder = EMPTY_NODE.builder();

        // +"/a":{"b":{"c":{"c"="cValue"}}} where b.exists() == false
        rootBuilder.child("a").setChildNode("b", createBC(false));

        NodeState r = rootBuilder.getNodeState();
        NodeState a = r.getChildNode("a");
        NodeState b = a.getChildNode("b");
        NodeState c = b.getChildNode("c");

        assertTrue(a.exists());
        assertFalse(b.exists());
        assertTrue(c.exists());
        assertTrue(c.hasProperty("c"));

        rootBuilder.child("a").getChildNode("b").child("c").setProperty("c2", "c2Value");

        r = rootBuilder.getNodeState();
        a = r.getChildNode("a");
        b = a.getChildNode("b");
        c = b.getChildNode("c");

        assertTrue(a.exists());
        assertFalse(b.exists());
        assertTrue(c.exists());

        // Node c is modified
        assertTrue(c.hasProperty("c"));
        assertTrue(c.hasProperty("c2"));
    }

    @Test
    public void shadowNonExistingNode1() {
        NodeBuilder rootBuilder = EMPTY_NODE.builder();

        // +"/a":{"b":{"c":{"c"="cValue"}}} where b.exists() == false
        rootBuilder.child("a").setChildNode("b", createBC(false));

        NodeState r = rootBuilder.getNodeState();
        NodeState a = r.getChildNode("a");
        NodeState b = a.getChildNode("b");
        NodeState c = b.getChildNode("c");

        assertTrue(a.exists());
        assertFalse(b.exists());
        assertTrue(c.exists());
        assertTrue(c.hasProperty("c"));

        rootBuilder.child("a").setChildNode("b").child("c").setProperty("c2", "c2Value");

        r = rootBuilder.getNodeState();
        a = r.getChildNode("a");
        b = a.getChildNode("b");
        c = b.getChildNode("c");

        assertTrue(a.exists());
        assertTrue(b.exists());    // node b is shadowed by above child("b")
        assertTrue(c.exists());

        // node c is shadowed by subtree b
        assertFalse(c.hasProperty("c"));
        assertTrue(c.hasProperty("c2"));
    }

    @Test
    public void shadowNonExistingNode2() {
        NodeBuilder rootBuilder = EMPTY_NODE.builder();

        // +"/a":{"b":{"c":{"c":"cValue"}}} where b.exists() == false
        rootBuilder.child("a").setChildNode("b", createBC(false));

        NodeState r = rootBuilder.getNodeState();
        NodeState a = r.getChildNode("a");
        NodeState b = a.getChildNode("b");
        NodeState c = b.getChildNode("c");

        assertTrue(a.exists());
        assertFalse(b.exists());
        assertTrue(c.exists());
        assertTrue(c.hasProperty("c"));

        rootBuilder.child("a").child("b").child("c").setProperty("c2", "c2Value");

        r = rootBuilder.getNodeState();
        a = r.getChildNode("a");
        b = a.getChildNode("b");
        c = b.getChildNode("c");

        assertTrue(a.exists());
        assertTrue(b.exists());  // node b is shadowed by above child("b")
        assertTrue(c.exists());

        // node c is shadowed by subtree b
        assertFalse(c.hasProperty("c"));
        assertTrue(c.hasProperty("c2"));
    }

    @Test
    public void navigateNonExistingNode() {
        NodeBuilder rootBuilder = EMPTY_NODE.builder();

        // +"/a":{"b":{"c":{"c":"cValue"}}} where b.exists() == false
        rootBuilder.child("a").setChildNode("b", createBC(false));

        NodeState r = rootBuilder.getNodeState();
        NodeState a = r.getChildNode("a");
        NodeState b = a.getChildNode("b");
        NodeState c = b.getChildNode("c");

        assertTrue(a.exists());
        assertFalse(b.exists());
        assertTrue(c.exists());
        assertTrue(c.hasProperty("c"));

        NodeBuilder aBuilder = rootBuilder.getChildNode("a");
        NodeBuilder bBuilder = aBuilder.getChildNode("b");
        NodeBuilder cBuilder = bBuilder.getChildNode("c");

        assertTrue(aBuilder.exists());
        assertTrue(cBuilder.exists());

        cBuilder.setProperty("c2", "c2Value");
        r = rootBuilder.getNodeState();
        a = r.getChildNode("a");
        b = a.getChildNode("b");
        c = b.getChildNode("c");

        assertTrue(a.exists());
        assertFalse(b.exists());
        assertTrue(c.exists());
        assertTrue(c.hasProperty("c"));
        assertTrue(c.hasProperty("c2"));
    }

    @Test
    public void removeRoot() {
        assertFalse(base.builder().remove());
    }

    private static NodeState createBC(final boolean exists) {
        final NodeState C = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE)
            .setProperty("c", "cValue")
            .getNodeState();

        return new AbstractNodeState() {
            @Override
            public boolean exists() {
                return exists;
            }

            @Nonnull
            @Override
            public Iterable<? extends PropertyState> getProperties() {
                return ImmutableSet.of();
            }

            @Override
            public boolean hasChildNode(@Nonnull String name) {
                return "c".equals(name);
            }

            @Nonnull
            @Override
            public NodeState getChildNode(@Nonnull String name) {
                if ("c".equals(name)) {
                    return C;
                } else {
                    checkValidName(name);
                    return EmptyNodeState.MISSING_NODE;
                }
            }

            @Nonnull
            @Override
            public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
                if (exists) {
                    return ImmutableSet.of(new MemoryChildNodeEntry("c", C));
                } else {
                    return ImmutableSet.of();
                }
            }

            @Nonnull
            @Override
            public NodeBuilder builder() {
                return new MemoryNodeBuilder(this);
            }
        };
    }

}
