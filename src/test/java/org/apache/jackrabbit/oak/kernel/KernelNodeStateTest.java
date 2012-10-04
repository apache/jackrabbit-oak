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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.core.AbstractCoreTest;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static org.apache.jackrabbit.oak.api.Type.LONG;

public class KernelNodeStateTest extends AbstractCoreTest {

    @Override
    protected NodeState createInitialState(MicroKernel microKernel) {
        String jsop =
                "+\"test\":{\"a\":1,\"b\":2,\"c\":3,"
                + "\"x\":{},\"y\":{},\"z\":{}}";
        microKernel.commit("/", jsop, null, "test data");
        return store.getRoot().getChildNode("test");
    }

    @Test
    public void testGetPropertyCount() {
        assertEquals(3, state.getPropertyCount());
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
    }

    @Test
    public void testGetProperties() {
        List<String> names = new ArrayList<String>();
        List<Long> values = new ArrayList<Long>();
        for (PropertyState property : state.getProperties()) {
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
        assertEquals(3, state.getChildNodeCount());
    }

    @Test
    public void testGetChildNode() {
        assertNotNull(state.getChildNode("x"));
        assertNotNull(state.getChildNode("y"));
        assertNotNull(state.getChildNode("z"));
        assertNull(state.getChildNode("a"));
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
