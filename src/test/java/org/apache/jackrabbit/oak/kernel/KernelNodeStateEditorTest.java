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

import org.apache.jackrabbit.mk.model.NodeState;
import org.apache.jackrabbit.mk.model.PropertyState;
import org.apache.jackrabbit.mk.model.Scalar;
import org.apache.jackrabbit.mk.model.Scalar.Type;
import org.apache.jackrabbit.mk.model.ScalarImpl;
import org.apache.jackrabbit.mk.simple.SimpleKernelImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class KernelNodeStateEditorTest {
    private SimpleKernelImpl microkernel;
    private KernelNodeState state;

    @Before
    public void setUp() {
        microkernel = new SimpleKernelImpl("mem:KernelNodeStateTest");
        String jsop =
                "+\"test\":{\"a\":1,\"b\":2,\"c\":3,"
                        + "\"x\":{},\"y\":{},\"z\":{}}";
        String revision = microkernel.commit(
                "/", jsop, microkernel.getHeadRevision(), "test data");
        state = new KernelNodeState(microkernel, "/test", revision);
    }

    @Test
    public void getNode() {
        KernelNodeStateEditor editor = new KernelNodeStateEditor(state);
        TransientNodeState transientState = editor.getTransientState();
        TransientNodeState childState = transientState.getChildNode("any");
        assertNull(childState);

        childState = transientState.getChildNode("x");
        assertNotNull(childState);
    }

    @Test
    public void getProperty() {
        KernelNodeStateEditor editor = new KernelNodeStateEditor(state);
        TransientNodeState transientState = editor.getTransientState();
        PropertyState propertyState = transientState.getProperty("any");
        assertNull(propertyState);
        
        propertyState = transientState.getProperty("a");
        assertNotNull(propertyState);
        assertFalse(propertyState.isArray());
        assertEquals(Type.LONG, propertyState.getScalar().getType());
        assertEquals(1, propertyState.getScalar().getLong());
    }
    
    @Test
    public void getNodes() {
        KernelNodeStateEditor editor = new KernelNodeStateEditor(state);
        TransientNodeState transientState = editor.getTransientState();
        Iterable<TransientNodeState> nodes = transientState.getChildNodes(0, -1);

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
        KernelNodeStateEditor editor = new KernelNodeStateEditor(state);
        TransientNodeState transientState = editor.getTransientState();

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
        KernelNodeStateEditor editor = new KernelNodeStateEditor(state);
        TransientNodeState transientState = editor.getTransientState();

        assertFalse(transientState.hasNode("new"));
        editor.addNode("new");
        assertTrue(transientState.hasNode("new"));

        NodeState newState = editor.mergeInto(microkernel, state);
        assertNotNull(newState.getChildNode("new"));
    }

    @Test
    public void removeNode() {
        KernelNodeStateEditor editor = new KernelNodeStateEditor(state);
        TransientNodeState transientState = editor.getTransientState();

        assertTrue(transientState.hasNode("x"));
        editor.removeNode("x");
        assertFalse(transientState.hasNode("x"));

        NodeState newState = editor.mergeInto(microkernel, state);
        assertNull(newState.getChildNode("x"));
    }

    @Test
    public void setProperty() {
        KernelNodeStateEditor editor = new KernelNodeStateEditor(state);
        TransientNodeState transientState = editor.getTransientState();

        assertFalse(transientState.hasProperty("new"));
        Scalar value = ScalarImpl.stringScalar("value");
        editor.setProperty(new KernelPropertyState("new", value));
        PropertyState property = transientState.getProperty("new");
        assertNotNull(property);
        assertEquals("new", property.getName());
        assertEquals(value, property.getScalar());

        NodeState newState = editor.mergeInto(microkernel, state);
        property = newState.getProperty("new");
        assertNotNull(property);
        assertEquals("new", property.getName());
        assertEquals(value, property.getScalar());
    }

    @Test
    public void removeProperty() {
        KernelNodeStateEditor editor = new KernelNodeStateEditor(state);
        TransientNodeState transientState = editor.getTransientState();

        assertTrue(transientState.hasProperty("a"));
        editor.removeProperty("a");
        assertFalse(transientState.hasProperty("a"));

        NodeState newState = editor.mergeInto(microkernel, state);
        Assert.assertNull(newState.getProperty("a"));
    }

    @Test
    public void move() {
        KernelNodeStateEditor editor = new KernelNodeStateEditor(state);
        TransientNodeState transientState = editor.getTransientState();
        TransientNodeState y = transientState.getChildNode("y");

        assertTrue(transientState.hasNode("x"));
        editor.move("x", "y/xx");
        assertFalse(transientState.hasNode("x"));
        assertTrue(y.hasNode("xx"));

        NodeState newState = editor.mergeInto(microkernel, state);
        assertNull(newState.getChildNode("x"));
        assertNotNull(newState.getChildNode("y"));
        assertNotNull(newState.getChildNode("y").getChildNode("xx"));
    }

    @Test
    public void copy() {
        KernelNodeStateEditor editor = new KernelNodeStateEditor(state);
        TransientNodeState transientState = editor.getTransientState();
        TransientNodeState y = transientState.getChildNode("y");

        assertTrue(transientState.hasNode("x"));
        editor.copy("x", "y/xx");
        assertTrue(transientState.hasNode("x"));
        assertTrue(y.hasNode("xx"));

        NodeState newState = editor.mergeInto(microkernel, state);
        assertNotNull(newState.getChildNode("x"));
        assertNotNull(newState.getChildNode("y"));
        assertNotNull(newState.getChildNode("y").getChildNode("xx"));
    }

    @Test
    public void deepCopy() {
        KernelNodeStateEditor editor = new KernelNodeStateEditor(state);
        TransientNodeState transientState = editor.getTransientState();
        TransientNodeState y = transientState.getChildNode("y");

        editor.edit("x").addNode("x1");
        editor.copy("x", "y/xx");
        assertTrue(y.hasNode("xx"));
        assertTrue(y.getChildNode("xx").hasNode("x1"));

        NodeState newState = editor.mergeInto(microkernel, state);
        assertNotNull(newState.getChildNode("x"));
        assertNotNull(newState.getChildNode("y"));
        assertNotNull(newState.getChildNode("y").getChildNode("xx"));
        assertNotNull(newState.getChildNode("y").getChildNode("xx").getChildNode("x1"));
    }

}
