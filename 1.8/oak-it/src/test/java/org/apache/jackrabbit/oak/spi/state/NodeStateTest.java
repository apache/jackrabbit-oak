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

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static org.apache.jackrabbit.oak.api.Type.LONG;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.oak.OakBaseTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class NodeStateTest extends OakBaseTest {
    private NodeState state;

    private long initialCount;

    public NodeStateTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Before
    public void setUp() throws CommitFailedException {
        NodeState root = store.getRoot();
        initialCount = root.getPropertyCount();

        NodeBuilder builder = store.getRoot().builder();
        builder.setProperty("a", 1);
        builder.setProperty("b", 2);
        builder.setProperty("c", 3);
        builder.child("x");
        builder.child("y");
        builder.child("z");

        state = store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        state = store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    @After
    public void tearDown() {
        state = null;
    }

    @Test
    public void testGetPropertyCount() {
        assertEquals(3 + initialCount, state.getPropertyCount());
    }

    @Test
    public void testGetProperty() {
        assertEquals("a", state.getProperty("a").getName());
        assertEquals(1, (long) state.getProperty("a").getValue(LONG));
        assertEquals("b", state.getProperty("b").getName());
        assertEquals(2, (long) state.getProperty("b").getValue(LONG));
        assertEquals("c", state.getProperty("c").getName());
        assertEquals(3, (long) state.getProperty("c").getValue(LONG));
        assertNull(state.getProperty("x"));
        assertFalse(state.hasProperty("x"));
    }

    @Test
    public void testGetProperties() {
        List<String> names = new ArrayList<String>();
        List<Long> values = new ArrayList<Long>();
        for (PropertyState property : state.getProperties()) {
            if (property.getName().startsWith(":")) {
                continue;
            }
            names.add(property.getName());
            values.add(property.getValue(LONG));
        }
        Collections.sort(names);
        Collections.sort(values);
        assertEquals(Arrays.asList("a", "b", "c"), names);
        assertEquals(Arrays.asList(1L, 2L, 3L), values);
    }

    @Test
    public void testGetChildNodeCount() {
        assertEquals(3, state.getChildNodeCount(4));
    }

    @Test
    public void testGetChildNode() {
        assertTrue(state.getChildNode("x").exists());
        assertTrue(state.getChildNode("y").exists());
        assertTrue(state.getChildNode("z").exists());
        assertFalse(state.getChildNode("a").exists());
    }

    @Test
    public void testGetChildNodeEntries() {
        List<String> names = new ArrayList<String>();
        for (ChildNodeEntry entry : state.getChildNodeEntries()) {
            names.add(entry.getName());
        }
        Collections.sort(names);
        assertEquals(Arrays.asList("x", "y", "z"), names);
    }

}
