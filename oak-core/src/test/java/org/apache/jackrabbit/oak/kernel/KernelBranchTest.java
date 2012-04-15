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
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Scalar;
import org.apache.jackrabbit.oak.api.TransientNodeState;
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
    public void getNode() {
        KernelBranch branch = new KernelBranch(state);
        TransientNodeState transientState = branch.getNode("/");
        TransientNodeState childState = transientState.getChildNode("any");
        assertNull(childState);

        childState = transientState.getChildNode("x");
        assertNotNull(childState);
    }

    @Test
    public void getProperty() {
        KernelBranch branch = new KernelBranch(state);
        TransientNodeState transientState = branch.getNode("/");
        PropertyState propertyState = transientState.getProperty("any");
        assertNull(propertyState);

        propertyState = transientState.getProperty("a");
        assertNotNull(propertyState);
        assertFalse(propertyState.isArray());
        assertEquals(Scalar.Type.LONG, propertyState.getScalar().getType());
        assertEquals(1, propertyState.getScalar().getLong());
    }

    @Test
    public void getNodes() {
        KernelBranch branch = new KernelBranch(state);
        TransientNodeState transientState = branch.getNode("/");
        Iterable<TransientNodeState> nodes = transientState.getChildNodes();

        Set<String> expectedPaths = new HashSet<String>();
        Collections.addAll(expectedPaths, "x", "y", "z");

        for (TransientNodeState node : nodes) {
            assertTrue(expectedPaths.remove(node.getPath()));
        }
        assertTrue(expectedPaths.isEmpty());

        assertEquals(3, transientState.getChildNodeCount());
    }

    @Test
    public void getProperties() {
        KernelBranch branch = new KernelBranch(state);
        TransientNodeState transientState = branch.getNode("/");

        Map<String, Scalar> expectedProperties = new HashMap<String, Scalar>();
        expectedProperties.put("a", ScalarImpl.longScalar(1));
        expectedProperties.put("b", ScalarImpl.longScalar(2));
        expectedProperties.put("c", ScalarImpl.longScalar(3));

        Iterable<PropertyState> properties = transientState.getProperties();
        for (PropertyState property : properties) {
            Scalar value = expectedProperties.remove(property.getName());
            assertNotNull(value);
            assertFalse(property.isArray());
            assertEquals(value, property.getScalar());
        }

        assertTrue(expectedProperties.isEmpty());

        assertEquals(3, transientState.getPropertyCount());
    }

    @Test
    public void addNode() {
        KernelBranch branch = new KernelBranch(state);
        TransientNodeState transientState = branch.getNode("/");

        assertFalse(transientState.hasNode("new"));
        TransientNodeState newNode = transientState.addNode("new");
        assertNotNull(newNode);
        assertEquals("new", newNode.getName());
        assertTrue(transientState.hasNode("new"));

        NodeState newState = branch.mergeInto(microkernel, state);
        assertNotNull(newState.getChildNode("new"));
    }

    @Test
    public void addExistingNode() {
        KernelBranch branch = new KernelBranch(state);
        TransientNodeState transientState = branch.getNode("/");

        assertFalse(transientState.hasNode("new"));
        transientState.addNode("new");
        NodeState newState = branch.mergeInto(microkernel, state);

        branch = new KernelBranch(newState);
        transientState = branch.getNode("/");
        assertTrue(transientState.hasNode("new"));
        TransientNodeState newNode = transientState.addNode("new");
        assertNotNull(newNode);
        assertEquals("new", newNode.getName());
    }

    @Test
    public void removeNode() {
        KernelBranch branch = new KernelBranch(state);
        TransientNodeState transientState = branch.getNode("/");

        assertTrue(transientState.hasNode("x"));
        transientState.removeNode("x");
        assertFalse(transientState.hasNode("x"));

        NodeState newState = branch.mergeInto(microkernel, state);
        assertNull(newState.getChildNode("x"));
    }

    @Test
    public void setProperty() {
        KernelBranch branch = new KernelBranch(state);
        TransientNodeState transientState = branch.getNode("/");

        assertFalse(transientState.hasProperty("new"));
        Scalar value = ScalarImpl.stringScalar("value");
        transientState.setProperty("new", value);
        PropertyState property = transientState.getProperty("new");
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
        TransientNodeState transientState = branch.getNode("/");

        assertTrue(transientState.hasProperty("a"));
        transientState.removeProperty("a");
        assertFalse(transientState.hasProperty("a"));

        NodeState newState = branch.mergeInto(microkernel, state);
        assertNull(newState.getProperty("a"));
    }

    @Test
    public void move() {
        KernelBranch branch = new KernelBranch(state);
        TransientNodeState transientState = branch.getNode("/");
        TransientNodeState y = transientState.getChildNode("y");

        assertTrue(transientState.hasNode("x"));
        branch.move("x", "y/xx");
        assertFalse(transientState.hasNode("x"));
        assertTrue(y.hasNode("xx"));

        NodeState newState = branch.mergeInto(microkernel, state);
        assertNull(newState.getChildNode("x"));
        assertNotNull(newState.getChildNode("y"));
        assertNotNull(newState.getChildNode("y").getChildNode("xx"));
    }

    @Test
    public void rename() {
        KernelBranch branch = new KernelBranch(state);
        TransientNodeState transientState = branch.getNode("/");

        assertTrue(transientState.hasNode("x"));
        branch.move("x", "xx");
        assertFalse(transientState.hasNode("x"));
        assertTrue(transientState.hasNode("xx"));

        NodeState newState = branch.mergeInto(microkernel, state);
        assertNull(newState.getChildNode("x"));
        assertNotNull(newState.getChildNode("xx"));
    }

    @Test
    public void copy() {
        KernelBranch branch = new KernelBranch(state);
        TransientNodeState transientState = branch.getNode("/");
        TransientNodeState y = transientState.getChildNode("y");

        assertTrue(transientState.hasNode("x"));
        branch.copy("x", "y/xx");
        assertTrue(transientState.hasNode("x"));
        assertTrue(y.hasNode("xx"));

        NodeState newState = branch.mergeInto(microkernel, state);
        assertNotNull(newState.getChildNode("x"));
        assertNotNull(newState.getChildNode("y"));
        assertNotNull(newState.getChildNode("y").getChildNode("xx"));
    }

    @Test
    public void deepCopy() {
        KernelBranch branch = new KernelBranch(state);
        TransientNodeState transientState = branch.getNode("/");
        TransientNodeState y = transientState.getChildNode("y");

        branch.getNode("x").addNode("x1");
        branch.copy("x", "y/xx");
        assertTrue(y.hasNode("xx"));
        assertTrue(y.getChildNode("xx").hasNode("x1"));

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
    public void getChildNodeCount() {
        KernelBranch branch = new KernelBranch(state);
        TransientNodeState transientState = branch.getNode("/");
        assertEquals(3, transientState.getChildNodeCount());

        transientState.removeNode("x");
        assertEquals(2, transientState.getChildNodeCount());

        transientState.addNode("a");
        assertEquals(3, transientState.getChildNodeCount());

        transientState.addNode("x");
        assertEquals(4, transientState.getChildNodeCount());
    }

    @Test
    public void getPropertyCount() {
        KernelBranch branch = new KernelBranch(state);
        TransientNodeState transientState = branch.getNode("/");
        assertEquals(3, transientState.getPropertyCount());

        Scalar value = ScalarImpl.stringScalar("foo");
        transientState.setProperty("a", value);
        assertEquals(3, transientState.getPropertyCount());

        transientState.removeProperty("a");
        assertEquals(2, transientState.getPropertyCount());

        transientState.setProperty("x", value);
        assertEquals(3, transientState.getPropertyCount());

        transientState.setProperty("a", value);
        assertEquals(4, transientState.getPropertyCount());
    }

    @Test
    public void largeChildNodeList() {
        KernelBranch branch = new KernelBranch(state);
        TransientNodeState transientState = branch.getNode("/");

        transientState.addNode("large");
        transientState = transientState.getChildNode("large");
        for (int c = 0; c < 10000; c++) {
            transientState.addNode("n" + c);
        }

        KernelNodeState newState = branch.mergeInto(microkernel, state);
        branch = new KernelBranch(newState);
        transientState = branch.getNode("/");
        transientState = transientState.getChildNode("large");

        int c = 0;
        for (TransientNodeState q : transientState.getChildNodes()) {
            assertEquals("n" + c++, q.getName());
        }

    }

}
