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

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.apache.jackrabbit.oak.api.Type.STRING;

public class MemoryNodeBuilderTest {

    private static final NodeState BASE = new MemoryNodeState(
            ImmutableMap.<String, PropertyState>of(
                    "a", PropertyStates.longProperty("a", 1L),
                    "b", PropertyStates.longProperty("b", 2L),
                    "c", PropertyStates.longProperty("c", 3L)),
            ImmutableMap.of(
                    "x", MemoryNodeState.EMPTY_NODE,
                    "y", MemoryNodeState.EMPTY_NODE,
                    "z", MemoryNodeState.EMPTY_NODE));

    @Test
    public void testConnectOnAddProperty() {
        NodeBuilder root = new MemoryNodeBuilder(BASE);
        NodeBuilder childA = root.child("x");
        NodeBuilder childB = root.child("x");

        assertNull(childA.getProperty("test"));
        childB.setProperty("test", "foo");
        assertNotNull(childA.getProperty("test"));
    }

    @Test
    public void testConnectOnUpdateProperty() {
        NodeBuilder root = new MemoryNodeBuilder(BASE);
        NodeBuilder childA = root.child("x");
        NodeBuilder childB = root.child("x");

        childB.setProperty("test", "foo");
        childA.setProperty("test", "bar");
        assertEquals(
                "bar",
                childB.getProperty("test").getValue(STRING));
    }

    @Test
    public void testConnectOnRemoveProperty() {
        NodeBuilder root = new MemoryNodeBuilder(BASE);
        NodeBuilder childA = root.child("x");
        NodeBuilder childB = root.child("x");

        childB.setProperty("test", "foo");
        childA.removeProperty("test");
        assertNull(childB.getProperty("test"));
    }

    @Test
    public void testConnectOnAddNode() {
        NodeBuilder root = new MemoryNodeBuilder(BASE);
        NodeBuilder childA = root.child("x");
        NodeBuilder childB = root.child("x");

        assertFalse(childA.hasChildNode("test"));
        childB.child("test");
        assertTrue(childA.hasChildNode("test"));
    }

    @Test(expected = IllegalStateException.class)
    public void testConnectOnRemoveNode() {
        NodeBuilder root = new MemoryNodeBuilder(BASE);
        NodeBuilder child = root.child("x");

        root.removeNode("x");
        child.getChildNodeCount(); // should throw ISE
        fail();
    }

}
