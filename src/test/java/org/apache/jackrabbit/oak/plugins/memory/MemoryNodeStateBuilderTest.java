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
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateBuilder;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class MemoryNodeStateBuilderTest {

    private static final NodeState BASE = new MemoryNodeState(
            ImmutableMap.<String, PropertyState>of(
                    "a", SinglePropertyState.create("a", 1),
                    "b", SinglePropertyState.create("b", 2),
                    "c", SinglePropertyState.create("c", 3)),
            ImmutableMap.of(
                    "x", MemoryNodeState.EMPTY_NODE,
                    "y", MemoryNodeState.EMPTY_NODE,
                    "z", MemoryNodeState.EMPTY_NODE));

    @Test
    public void testConnectOnAddProperty() {
        NodeStateBuilder root = new MemoryNodeStateBuilder(BASE);
        NodeStateBuilder childA = root.getChildBuilder("x");
        NodeStateBuilder childB = root.getChildBuilder("x");

        assertNull(childA.getProperty("test"));
        childB.setProperty("test", new StringValue("foo"));
        assertNotNull(childA.getProperty("test"));
    }

    @Test
    public void testConnectOnUpdateProperty() {
        NodeStateBuilder root = new MemoryNodeStateBuilder(BASE);
        NodeStateBuilder childA = root.getChildBuilder("x");
        NodeStateBuilder childB = root.getChildBuilder("x");

        childB.setProperty("test", new StringValue("foo"));
        childA.setProperty("test", new StringValue("bar"));
        assertEquals(
                "bar",
                childB.getProperty("test").getValue().getString());
    }

    @Test
    public void testConnectOnRemoveProperty() {
        NodeStateBuilder root = new MemoryNodeStateBuilder(BASE);
        NodeStateBuilder childA = root.getChildBuilder("x");
        NodeStateBuilder childB = root.getChildBuilder("x");

        childB.setProperty("test", new StringValue("foo"));
        childA.removeProperty("test");
        assertNull(childB.getProperty("test"));
    }

    @Test
    public void testConnectOnAddNode() {
        NodeStateBuilder root = new MemoryNodeStateBuilder(BASE);
        NodeStateBuilder childA = root.getChildBuilder("x");
        NodeStateBuilder childB = root.getChildBuilder("x");

        assertFalse(childA.hasChildNode("test"));
        childB.setNode("test", MemoryNodeState.EMPTY_NODE);
        assertTrue(childA.hasChildNode("test"));
    }

    @Test(expected = IllegalStateException.class)
    public void testConnectOnRemoveNode() {
        NodeStateBuilder root = new MemoryNodeStateBuilder(BASE);
        NodeStateBuilder child = root.getChildBuilder("x");

        root.removeNode("x");
        child.getChildNodeCount(); // should throw ISE
        fail();
    }

}
