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

public class KernelBranchTest {

    private final MicroKernel microkernel = new SimpleKernelImpl("mem:");

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
        KernelBranch branch = new KernelBranch(state);
        Tree tree = branch.getTree("/");
        Tree child = tree.getChild("any");
        assertNull(child);

        child = tree.getChild("x");
        assertNotNull(child);
    }

    @Test
    public void getProperty() {
        KernelBranch branch = new KernelBranch(state);
        Tree tree = branch.getTree("/");
        PropertyState propertyState = tree.getProperty("any");
        assertNull(propertyState);

        propertyState = tree.getProperty("a");
        assertNotNull(propertyState);
        assertFalse(propertyState.isArray());
        assertEquals(Scalar.Type.LONG, propertyState.getScalar().getType());
        assertEquals(1, propertyState.getScalar().getLong());
    }

    @Test
    public void getChildren() {
        KernelBranch branch = new KernelBranch(state);
        Tree tree = branch.getTree("/");
        Iterable<Tree> children = tree.getChildren();

        Set<String> expectedPaths = new HashSet<String>();
        Collections.addAll(expectedPaths, "x", "y", "z");

        for (Tree child : children) {
            assertTrue(expectedPaths.remove(child.getPath()));
        }
        assertTrue(expectedPaths.isEmpty());

        assertEquals(3, tree.getChildrenCount());
    }

    @Test
    public void getProperties() {
        KernelBranch branch = new KernelBranch(state);
        Tree tree = branch.getTree("/");

        Map<String, Scalar> expectedProperties = new HashMap<String, Scalar>();
        expectedProperties.put("a", ScalarImpl.longScalar(1));
        expectedProperties.put("b", ScalarImpl.longScalar(2));
        expectedProperties.put("c", ScalarImpl.longScalar(3));

        Iterable<PropertyState> properties = tree.getProperties();
        for (PropertyState property : properties) {
            Scalar value = expectedProperties.remove(property.getName());
            assertNotNull(value);
            assertFalse(property.isArray());
            assertEquals(value, property.getScalar());
        }

        assertTrue(expectedProperties.isEmpty());

        assertEquals(3, tree.getPropertyCount());
    }

    @Test
    public void addChild() {
        KernelBranch branch = new KernelBranch(state);
        Tree tree = branch.getTree("/");

        assertFalse(tree.hasChild("new"));
        Tree added = tree.addChild("new");
        assertNotNull(added);
        assertEquals("new", added.getName());
        assertTrue(tree.hasChild("new"));

        NodeState newState = branch.mergeInto(microkernel, state);
        assertNotNull(newState.getChildNode("new"));
    }

    @Test
    public void addExistingChild() {
        KernelBranch branch = new KernelBranch(state);
        Tree tree = branch.getTree("/");

        assertFalse(tree.hasChild("new"));
        tree.addChild("new");
        NodeState newState = branch.mergeInto(microkernel, state);

        branch = new KernelBranch(newState);
        tree = branch.getTree("/");
        assertTrue(tree.hasChild("new"));
        Tree added = tree.addChild("new");
        assertNotNull(added);
        assertEquals("new", added.getName());
    }

    @Test
    public void removeChild() {
        KernelBranch branch = new KernelBranch(state);
        Tree tree = branch.getTree("/");

        assertTrue(tree.hasChild("x"));
        tree.removeChild("x");
        assertFalse(tree.hasChild("x"));

        NodeState newState = branch.mergeInto(microkernel, state);
        assertNull(newState.getChildNode("x"));
    }

    @Test
    public void setProperty() {
        KernelBranch branch = new KernelBranch(state);
        Tree tree = branch.getTree("/");

        assertFalse(tree.hasProperty("new"));
        Scalar value = ScalarImpl.stringScalar("value");
        tree.setProperty("new", value);
        PropertyState property = tree.getProperty("new");
        assertNotNull(property);
        assertEquals("new", property.getName());
        assertEquals(value, property.getScalar());

        NodeState newState = branch.mergeInto(microkernel, state);
        property = newState.getProperty("new");
        assertNotNull(property);
        assertEquals("new", property.getName());
        assertEquals(value, property.getScalar());
    }

    @Test
    public void removeProperty() {
        KernelBranch branch = new KernelBranch(state);
        Tree tree = branch.getTree("/");

        assertTrue(tree.hasProperty("a"));
        tree.removeProperty("a");
        assertFalse(tree.hasProperty("a"));

        NodeState newState = branch.mergeInto(microkernel, state);
        assertNull(newState.getProperty("a"));
    }

    @Test
    public void move() {
        KernelBranch branch = new KernelBranch(state);
        Tree tree = branch.getTree("/");
        Tree y = tree.getChild("y");

        assertTrue(tree.hasChild("x"));
        branch.move("x", "y/xx");
        assertFalse(tree.hasChild("x"));
        assertTrue(y.hasChild("xx"));

        NodeState newState = branch.mergeInto(microkernel, state);
        assertNull(newState.getChildNode("x"));
        assertNotNull(newState.getChildNode("y"));
        assertNotNull(newState.getChildNode("y").getChildNode("xx"));
    }

    @Test
    public void rename() {
        KernelBranch branch = new KernelBranch(state);
        Tree tree = branch.getTree("/");

        assertTrue(tree.hasChild("x"));
        branch.move("x", "xx");
        assertFalse(tree.hasChild("x"));
        assertTrue(tree.hasChild("xx"));

        NodeState newState = branch.mergeInto(microkernel, state);
        assertNull(newState.getChildNode("x"));
        assertNotNull(newState.getChildNode("xx"));
    }

    @Test
    public void copy() {
        KernelBranch branch = new KernelBranch(state);
        Tree tree = branch.getTree("/");
        Tree y = tree.getChild("y");

        assertTrue(tree.hasChild("x"));
        branch.copy("x", "y/xx");
        assertTrue(tree.hasChild("x"));
        assertTrue(y.hasChild("xx"));

        NodeState newState = branch.mergeInto(microkernel, state);
        assertNotNull(newState.getChildNode("x"));
        assertNotNull(newState.getChildNode("y"));
        assertNotNull(newState.getChildNode("y").getChildNode("xx"));
    }

    @Test
    public void deepCopy() {
        KernelBranch branch = new KernelBranch(state);
        Tree tree = branch.getTree("/");
        Tree y = tree.getChild("y");

        branch.getTree("x").addChild("x1");
        branch.copy("x", "y/xx");
        assertTrue(y.hasChild("xx"));
        assertTrue(y.getChild("xx").hasChild("x1"));

        NodeState newState = branch.mergeInto(microkernel, state);
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
        KernelBranch branch = new KernelBranch(state);
        Tree tree = branch.getTree("/");
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
        KernelBranch branch = new KernelBranch(state);
        Tree tree = branch.getTree("/");
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
        KernelBranch branch = new KernelBranch(state);
        Tree tree = branch.getTree("/");

        tree.addChild("large");
        tree = tree.getChild("large");
        for (int c = 0; c < 10000; c++) {
            tree.addChild("n" + c);
        }

        KernelNodeState newState = branch.mergeInto(microkernel, state);
        branch = new KernelBranch(newState);
        tree = branch.getTree("/");
        tree = tree.getChild("large");

        int c = 0;
        for (Tree q : tree.getChildren()) {
            assertEquals("n" + c++, q.getName());
        }

    }

}
