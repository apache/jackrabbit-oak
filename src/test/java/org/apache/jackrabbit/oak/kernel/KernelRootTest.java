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
package org.apache.jackrabbit.oak.kernel;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.simple.SimpleKernelImpl;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Scalar;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class KernelRootTest {

    private final MicroKernel microkernel = new SimpleKernelImpl("mem:");
    private final KernelNodeStore store = new KernelNodeStore(microkernel);

    private KernelNodeState state;

    @Before
    public void setUp() {
        String jsop =
                "+\"test\":{\"a\":1,\"b\":2,\"c\":3,"
                        + "\"x\":{},\"y\":{},\"z\":{}}";
        String revision = microkernel.commit(
                "/", jsop, microkernel.getHeadRevision(), "test data");
        state = new KernelNodeState(microkernel, "/test", revision);
    }

    @Test
    public void getChild() {
        KernelRoot branch = new KernelRoot(store, "test");
        Tree root = branch.getTree("/");

        Tree child = root.getChild("any");
        assertNull(child);

        child = root.getChild("x");
        assertNotNull(child);
    }

    @Test
    public void getProperty() {
        KernelRoot branch = new KernelRoot(store, "test");
        Tree root = branch.getTree("/");
        PropertyState propertyState = root.getProperty("any");
        assertNull(propertyState);

        propertyState = root.getProperty("a");
        assertNotNull(propertyState);
        assertFalse(propertyState.isArray());
        assertEquals(Scalar.Type.LONG, propertyState.getScalar().getType());
        assertEquals(1, propertyState.getScalar().getLong());
    }

    @Test
    public void getChildren() {
        KernelRoot branch = new KernelRoot(store, "test");
        Tree root = branch.getTree("/");
        Iterable<Tree> children = root.getChildren();

        Set<String> expectedPaths = new HashSet<String>();
        Collections.addAll(expectedPaths, "x", "y", "z");

        for (Tree child : children) {
            assertTrue(expectedPaths.remove(child.getPath()));
        }
        assertTrue(expectedPaths.isEmpty());

        assertEquals(3, root.getChildrenCount());
    }

    @Test
    public void getProperties() {
        KernelRoot branch = new KernelRoot(store, "test");
        Tree root = branch.getTree("/");

        Map<String, Scalar> expectedProperties = new HashMap<String, Scalar>();
        expectedProperties.put("a", ScalarImpl.longScalar(1));
        expectedProperties.put("b", ScalarImpl.longScalar(2));
        expectedProperties.put("c", ScalarImpl.longScalar(3));

        Iterable<PropertyState> properties = root.getProperties();
        for (PropertyState property : properties) {
            Scalar value = expectedProperties.remove(property.getName());
            assertNotNull(value);
            assertFalse(property.isArray());
            assertEquals(value, property.getScalar());
        }

        assertTrue(expectedProperties.isEmpty());

        assertEquals(3, root.getPropertyCount());
    }

    @Test
    public void addChild() throws CommitFailedException {
        KernelRoot branch = new KernelRoot(store, "test");
        Tree root = branch.getTree("/");

        assertFalse(root.hasChild("new"));
        Tree added = root.addChild("new");
        assertNotNull(added);
        assertEquals("new", added.getName());
        assertTrue(root.hasChild("new"));

        branch.commit();

        root = branch.getTree("/");
        assertTrue(root.hasChild("new"));
    }

    @Test
    public void addExistingChild() {
        KernelRoot branch = new KernelRoot(store, "test");
        Tree root = branch.getTree("/");

        assertFalse(root.hasChild("new"));
        root.addChild("new");
        branch.mergeInto(microkernel, state);

        branch = new KernelRoot(store, "test");
        root = branch.getTree("/");
        assertTrue(root.hasChild("new"));
        Tree added = root.addChild("new");
        assertNotNull(added);
        assertEquals("new", added.getName());
    }

    @Test
    public void removeChild() {
        KernelRoot branch = new KernelRoot(store, "test");
        Tree root = branch.getTree("/");

        assertTrue(root.hasChild("x"));
        root.removeChild("x");
        assertFalse(root.hasChild("x"));

        NodeState newState = branch.mergeInto(microkernel, state);
        assertNull(newState.getChildNode("x"));
    }

    @Test
    public void setProperty() {
        KernelRoot root = new KernelRoot(store, "test");
        Tree tree = root.getTree("/");

        assertFalse(tree.hasProperty("new"));
        Scalar value = ScalarImpl.stringScalar("value");
        tree.setProperty("new", value);
        PropertyState property = tree.getProperty("new");
        assertNotNull(property);
        assertEquals("new", property.getName());
        assertEquals(value, property.getScalar());

        NodeState newState = root.mergeInto(microkernel, state);
        property = newState.getProperty("new");
        assertNotNull(property);
        assertEquals("new", property.getName());
        assertEquals(value, property.getScalar());
    }

    @Test
    public void removeProperty() {
        KernelRoot root = new KernelRoot(store, "test");
        Tree tree = root.getTree("/");

        assertTrue(tree.hasProperty("a"));
        tree.removeProperty("a");
        assertFalse(tree.hasProperty("a"));

        NodeState newState = root.mergeInto(microkernel, state);
        assertNull(newState.getProperty("a"));
    }

    @Test
    public void move() {
        KernelRoot root = new KernelRoot(store, "test");
        Tree tree = root.getTree("/");
        Tree y = tree.getChild("y");

        assertTrue(tree.hasChild("x"));
        root.move("x", "y/xx");
        assertFalse(tree.hasChild("x"));
        assertTrue(y.hasChild("xx"));

        NodeState newState = root.mergeInto(microkernel, state);
        assertNull(newState.getChildNode("x"));
        assertNotNull(newState.getChildNode("y"));
        assertNotNull(newState.getChildNode("y").getChildNode("xx"));
    }

    @Test
    public void rename() {
        KernelRoot root = new KernelRoot(store, "test");
        Tree tree = root.getTree("/");

        assertTrue(tree.hasChild("x"));
        root.move("x", "xx");
        assertFalse(tree.hasChild("x"));
        assertTrue(tree.hasChild("xx"));

        NodeState newState = root.mergeInto(microkernel, state);
        assertNull(newState.getChildNode("x"));
        assertNotNull(newState.getChildNode("xx"));
    }

    @Test
    public void copy() {
        KernelRoot root = new KernelRoot(store, "test");
        Tree tree = root.getTree("/");
        Tree y = tree.getChild("y");

        assertTrue(tree.hasChild("x"));
        root.copy("x", "y/xx");
        assertTrue(tree.hasChild("x"));
        assertTrue(y.hasChild("xx"));

        NodeState newState = root.mergeInto(microkernel, state);
        assertNotNull(newState.getChildNode("x"));
        assertNotNull(newState.getChildNode("y"));
        assertNotNull(newState.getChildNode("y").getChildNode("xx"));
    }

    @Test
    public void deepCopy() {
        KernelRoot root = new KernelRoot(store, "test");
        Tree tree = root.getTree("/");
        Tree y = tree.getChild("y");

        root.getTree("x").addChild("x1");
        root.copy("x", "y/xx");
        assertTrue(y.hasChild("xx"));
        assertTrue(y.getChild("xx").hasChild("x1"));

        NodeState newState = root.mergeInto(microkernel, state);
        assertNotNull(newState.getChildNode("x"));
        assertNotNull(newState.getChildNode("y"));
        assertNotNull(newState.getChildNode("y").getChildNode("xx"));
        assertNotNull(newState.getChildNode("y").getChildNode("xx").getChildNode("x1"));

        NodeState x = newState.getChildNode("x");
        NodeState xx = newState.getChildNode("y").getChildNode("xx");
        assertEquals(x, xx);
    }

    @Test
    public void getChildrenCount() {
        KernelRoot root = new KernelRoot(store, "test");
        Tree tree = root.getTree("/");
        assertEquals(3, tree.getChildrenCount());

        tree.removeChild("x");
        assertEquals(2, tree.getChildrenCount());

        tree.addChild("a");
        assertEquals(3, tree.getChildrenCount());

        tree.addChild("x");
        assertEquals(4, tree.getChildrenCount());
    }

    @Test
    public void getPropertyCount() {
        KernelRoot root = new KernelRoot(store, "test");
        Tree tree = root.getTree("/");
        assertEquals(3, tree.getPropertyCount());

        Scalar value = ScalarImpl.stringScalar("foo");
        tree.setProperty("a", value);
        assertEquals(3, tree.getPropertyCount());

        tree.removeProperty("a");
        assertEquals(2, tree.getPropertyCount());

        tree.setProperty("x", value);
        assertEquals(3, tree.getPropertyCount());

        tree.setProperty("a", value);
        assertEquals(4, tree.getPropertyCount());
    }

    @Test
    public void largeChildList() {
        KernelRoot root = new KernelRoot(store, "test");
        Tree tree = root.getTree("/");

        tree.addChild("large");
        tree = tree.getChild("large");
        for (int c = 0; c < 10000; c++) {
            tree.addChild("n" + c);
        }

        root.mergeInto(microkernel, state);
        root = new KernelRoot(store, "test");
        tree = root.getTree("/");
        tree = tree.getChild("large");

        int c = 0;
        for (Tree q : tree.getChildren()) {
            assertEquals("n" + c++, q.getName());
        }

    }

}
