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

import java.util.Collections;

import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Ignore;
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
                    "a", LongPropertyState.createLongProperty("a", 1L),
                    "b", LongPropertyState.createLongProperty("b", 2L),
                    "c", LongPropertyState.createLongProperty("c", 3L)),
            ImmutableMap.of(
                    "x", new MemoryNodeState(
                        Collections.<String, PropertyState>emptyMap(),
                        Collections.singletonMap("q", MemoryNodeState.EMPTY_NODE)),
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
        assertEquals("bar", childA.getProperty("test").getValue(STRING));
        assertEquals("bar", childB.getProperty("test").getValue(STRING));
    }

    @Test
    public void testConnectOnRemoveProperty() {
        NodeBuilder root = new MemoryNodeBuilder(BASE);
        NodeBuilder childA = root.child("x");
        NodeBuilder childB = root.child("x");

        childB.setProperty("test", "foo");

        childA.removeProperty("test");
        assertNull(childA.getProperty("test"));
        assertNull(childB.getProperty("test"));

        childA.setProperty("test", "bar");
        assertEquals("bar", childA.getProperty("test").getValue(STRING));
        assertEquals("bar", childB.getProperty("test").getValue(STRING));
    }

    @Test
    public void testConnectOnAddNode() {
        NodeBuilder root = new MemoryNodeBuilder(BASE);
        NodeBuilder childA = root.child("x");
        NodeBuilder childB = root.child("x");

        assertFalse(childA.hasChildNode("test"));
        assertFalse(childB.hasChildNode("test"));

        childB.child("test");
        assertTrue(childA.hasChildNode("test"));
        assertTrue(childB.hasChildNode("test"));
    }

    @Test
    public void testConnectOnRemoveNode() {
        NodeBuilder root = new MemoryNodeBuilder(BASE);
        NodeBuilder child = root.child("x");

        root.removeNode("x");
        try {
            child.getChildNodeCount();
            fail();
        } catch (IllegalStateException e) {
            // expected
        }

        root.child("x");
        assertEquals(0, child.getChildNodeCount()); // reconnect!
    }

    @Test
    public void testAddRemovedNodeAgain() {
        NodeBuilder root = new MemoryNodeBuilder(BASE);

        root.removeNode("x");
        NodeBuilder x = root.child("x");

        x.child("q");
        assertTrue(x.hasChildNode("q"));
    }

    @Test
    public void testReset() {
        NodeBuilder root = new MemoryNodeBuilder(BASE);
        NodeBuilder child = root.child("x");
        child.child("new");

        assertTrue(child.hasChildNode("new"));
        assertTrue(root.child("x").hasChildNode("new"));

        root.reset(BASE);
        assertFalse(child.hasChildNode("new"));
        assertFalse(root.child("x").hasChildNode("new"));
    }

    @Test
    public void testReset2() {
        NodeBuilder root = new MemoryNodeBuilder(BASE);
        NodeBuilder x = root.child("x");
        NodeBuilder y = x.child("y");

        root.reset(BASE);
        assertTrue(root.hasChildNode("x"));
        assertFalse(x.hasChildNode("y"));
    }

    @Test
    public void testUnmodifiedEqualsBase() {
        NodeBuilder root = new MemoryNodeBuilder(BASE);
        NodeBuilder x = root.child("x");
        assertEquals(x.getBaseState(), x.getNodeState());
    }

}
