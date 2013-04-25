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

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.AbstractNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class MemoryNodeBuilderTest {

    private NodeState base;

    @Before
    public void setUp() {
        NodeBuilder builder = EMPTY_NODE.builder();
        builder.setProperty("a", 1L);
        builder.setProperty("b", 2L);
        builder.setProperty("c", 3L);
        builder.child("x").child("q");
        builder.child("y");
        builder.child("z");
        base = builder.getNodeState();
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
    public void testReadOnRemoveNode() {
        for (String name : new String[] {"x", "new"}) {
            NodeBuilder root = base.builder();
            NodeBuilder child = root.child(name);

            root.removeChildNode(name);
            try {
                child.getChildNodeCount();
                fail();
            } catch (IllegalStateException e) {
                // expected
            }

            root.child(name);
            assertEquals(0, child.getChildNodeCount()); // reconnect!
        }
    }

    @Test
    public void testWriteOnRemoveNode() {
        for (String name : new String[] {"x", "new"}) {
            NodeBuilder root = base.builder();
            NodeBuilder child = root.child(name);

            root.removeChildNode(name);
            try {
                child.setProperty("q", "w");
                fail();
            } catch (IllegalStateException e) {
                // expected
            }

            root.child(name);
            assertEquals(0, child.getChildNodeCount()); // reconnect!
        }
    }

    @Test
    public void testAddRemovedNodeAgain() {
        NodeBuilder root = base.builder();

        root.removeChildNode("x");
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

        root.reset(base);
        assertFalse(child.hasChildNode("new"));
        assertFalse(root.child("x").hasChildNode("new"));
    }

    @Test
    public void testReset2() {
        NodeBuilder root = base.builder();
        NodeBuilder x = root.child("x");
        x.child("y");

        root.reset(base);
        assertTrue(root.hasChildNode("x"));
        assertFalse(x.hasChildNode("y"));
    }

    @Test
    public void testUnmodifiedEqualsBase() {
        NodeBuilder root = base.builder();
        NodeBuilder x = root.child("x");
        assertEquals(x.getBaseState(), x.getNodeState());
    }

    @Test(expected = IllegalStateException.class)
    public void testReadOnRemovedNode() {
        NodeBuilder root = base.builder();
        NodeBuilder m = root.child("m");
        NodeBuilder n = m.child("n");

        root.removeChildNode("m");
        n.hasChildNode("any");
    }

    @Test
    public void testExistingStatus() {
        NodeBuilder root = base.builder();
        NodeBuilder x = root.child("x");
        assertTrue(x.isConnected());
        assertTrue(x.exists());
        assertFalse(x.isNew());
        assertFalse(x.isModified());
    }

    @Test
    public void testModifiedStatus() {
        NodeBuilder root = base.builder();
        NodeBuilder x = root.child("x");
        x.setProperty("p", "pValue");
        assertTrue(x.isConnected());
        assertTrue(x.exists());
        assertFalse(x.isNew());
        assertTrue(x.isModified());
    }

    @Test
    public void testRemovedStatus() {
        NodeBuilder root = base.builder();
        NodeBuilder x = root.child("x");
        root.removeChildNode("x");
        assertFalse(x.isConnected());
        try {
            assertTrue(x.exists());
            fail();
        } catch (IllegalStateException expected) {}
        try {
            assertFalse(x.isNew());
            fail();
        } catch (IllegalStateException expected) {}
        try {
            assertFalse(x.isModified());
            fail();
        } catch (IllegalStateException expected) {}
    }

    @Test
    public void testNewStatus() {
        NodeBuilder root = base.builder();
        NodeBuilder n = root.child("n");
        assertTrue(n.isConnected());
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
        assertFalse(any.isConnected());
        try {
            any.getChildNode("any");
            fail();
        } catch (IllegalStateException expected) {}
        try {
            any.exists();
            fail();
        } catch (IllegalStateException expected) {}
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
    @Ignore("OAK-781")
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
    @Ignore("OAK-781")
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
        assertFalse(bBuilder.isConnected());
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

            @Nonnull
            @Override
            public NodeState getChildNode(@Nonnull String name) {
                if ("c".equals(name)) {
                    return C;
                } else {
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
