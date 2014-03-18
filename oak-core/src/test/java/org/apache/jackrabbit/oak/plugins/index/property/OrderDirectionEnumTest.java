/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.property;

import org.junit.Test;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.OrderDirection;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.assertNull;

/**
 * tests the Enumeration for the index direction
 */
public class OrderDirectionEnumTest {
    @Test
    public void fromIndexMeta() {
        assertEquals(OrderDirection.ASC, OrderDirection.fromIndexMeta(null));

        NodeState indexMeta = EmptyNodeState.EMPTY_NODE;
        assertEquals(OrderDirection.ASC, OrderDirection.fromIndexMeta(indexMeta));

        indexMeta = EmptyNodeState.EMPTY_NODE.builder()
            .setProperty(OrderedIndex.DIRECTION, OrderDirection.ASC.getDirection()).getNodeState();
        assertEquals(OrderDirection.ASC, OrderDirection.fromIndexMeta(indexMeta));

        indexMeta = EmptyNodeState.EMPTY_NODE.builder()
            .setProperty(OrderedIndex.DIRECTION, OrderDirection.DESC.getDirection()).getNodeState();
        assertEquals(OrderDirection.DESC, OrderDirection.fromIndexMeta(indexMeta));
    }

    @Test
    public void isDescending(){
        NodeState indexMeta = null;
        assertFalse(OrderDirection.isDescending(indexMeta));

        indexMeta = EmptyNodeState.EMPTY_NODE.builder()
            .setProperty(OrderedIndex.DIRECTION, OrderDirection.ASC.getDirection()).getNodeState();
        assertFalse(OrderDirection.isDescending(indexMeta));

        indexMeta = EmptyNodeState.EMPTY_NODE.builder()
            .setProperty(OrderedIndex.DIRECTION, OrderDirection.DESC.getDirection()).getNodeState();
        assertTrue(OrderDirection.isDescending(indexMeta));
    }

    @Test
    public void isAscending(){
        NodeState indexMeta = null;
        assertTrue(OrderDirection.isAscending(indexMeta));

        indexMeta = EmptyNodeState.EMPTY_NODE.builder()
            .setProperty(OrderedIndex.DIRECTION, OrderDirection.ASC.getDirection()).getNodeState();
        assertTrue(OrderDirection.isAscending(indexMeta));

        indexMeta = EmptyNodeState.EMPTY_NODE.builder()
            .setProperty(OrderedIndex.DIRECTION, OrderDirection.DESC.getDirection()).getNodeState();
        assertFalse(OrderDirection.isAscending(indexMeta));
    }

    @Test
    public void orderedDirectionFromString() {
        assertNull("A non-existing order direction should result in null",
            OrderDirection.fromString("foobar"));
        assertEquals(OrderDirection.ASC, OrderDirection.fromString("ascending"));
        assertFalse(OrderDirection.ASC.equals(OrderDirection.fromString("descending")));
        assertEquals(OrderDirection.DESC, OrderDirection.fromString("descending"));
        assertFalse(OrderDirection.DESC.equals(OrderDirection.fromString("ascending")));
    }

}
