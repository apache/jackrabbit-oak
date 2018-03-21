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

package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import com.google.common.collect.Iterators;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileBufferLinkedList.NodeIterator;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FlatFileBufferLinkedListTest {
    private FlatFileBufferLinkedList list = null;
    private static final NodeStateEntry TEST_NODE_STATE_ENTRY = new NodeStateEntry(EMPTY_NODE, "/");

    @Before
    public void setup() {
        list = new FlatFileBufferLinkedList();
    }

    @Test
    public void add() {
        try {
            list.add(null);
            fail("Adding null must throw IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            //ignore
        }

        list.add(TEST_NODE_STATE_ENTRY);
    }

    @Test
    public void remove() {
        try {
            list.remove();
            fail("Must fail to remove from an empty list");
        } catch (IllegalStateException ise) {
            //ignore
        }

        list.add(TEST_NODE_STATE_ENTRY);
        assertEquals("Should get item on removal", TEST_NODE_STATE_ENTRY, list.remove());
    }

    @Test
    public void iterator() {
        assertEquals("empty list must be 0-sized", 0, Iterators.size(list.iterator()));

        list.add(TEST_NODE_STATE_ENTRY);
        assertEquals("single entry list must be 1-sized", 1, Iterators.size(list.iterator()));
        assertEquals("single entry list must be 1-sized on separate iterators",
                1, Iterators.size(list.iterator()));

        list.add(TEST_NODE_STATE_ENTRY);
        assertEquals("2 entries in list must be 2-sized", 2, Iterators.size(list.iterator()));

        assertEquals("2 entries in list must be 2-sized on separate iterators",
                2, Iterators.size(list.iterator()));

        NodeIterator iter2 = list.iterator();
        NodeIterator iter1 = list.iterator();
        iter2.next();
        assertEquals("2 entries in list must be 1-sized after consuming an item",
                1, Iterators.size(iter2));
        assertEquals("2 entries in list must be 2-sized even if some other iterator consumed an item",
                2, Iterators.size(iter1));

        list.add(TEST_NODE_STATE_ENTRY);
        iter1 = list.iterator();
        iter2 = list.iterator();

        iter1.next();//move iter to point at node being removed below
        iter2.next();iter2.next();// move iter beyond node being removed - this should remain valid

        list.remove();
        try {
            iter1.next();
            fail("Iterator state once removed from list can't be traversed");
        } catch (IllegalStateException ise) {
            //ignore
        }
        assertEquals(TEST_NODE_STATE_ENTRY, iter2.next());//this should work
        assertEquals("2 entries in list must be 1-sized after removal of an iterm",
                2, Iterators.size(list.iterator()));
    }

    @Test
    public void size() {
        assertEquals("empty list must be 0-sized", 0, list.size());

        list.add(TEST_NODE_STATE_ENTRY);
        assertEquals("single entry list must be 1-sized", 1, list.size());
        assertEquals("single entry list must be 1-sized on separate iterators", 1, list.size());

        list.add(TEST_NODE_STATE_ENTRY);
        assertEquals("2 entries in list must be 2-sized", 2, list.size());

        assertEquals("2 entries in list must be 2-sized on separate iterators", 2, list.size());

        list.remove();
        assertEquals("2 entries in list must be 1-sized after removing an item", 1, list.size());
    }

    @Test
    public void isEmpty() {
        assertTrue("Empty list should be empty", list.isEmpty());

        list.add(TEST_NODE_STATE_ENTRY);
        assertFalse("Non-empty list should be non-empty", list.isEmpty());

        list.remove();
        assertTrue("Empty list due to removal should be empty", list.isEmpty());
    }

    @Test
    public void memUsage() {
        assertEquals("Empty list must be estimate 0", 0, list.estimatedMemoryUsage());

        list.add(new NodeStateEntry(EMPTY_NODE, "/", 20));
        assertEquals(20, list.estimatedMemoryUsage());

        list.add(new NodeStateEntry(EMPTY_NODE, "/", 30));
        assertEquals(50, list.estimatedMemoryUsage());

        list.remove();
        assertEquals(30, list.estimatedMemoryUsage());
    }

    @Test
    public void memLimit() {
        list = new FlatFileBufferLinkedList(10);
        NodeStateEntry e10Bytes = new NodeStateEntry(EMPTY_NODE, "/", 10);
        NodeStateEntry e1Byte = new NodeStateEntry(EMPTY_NODE, "/", 1);

        list.add(e10Bytes); //this should succeed

        list.remove();
        list.add(e1Byte);
        try {
            list.add(e10Bytes);
        } catch (IllegalStateException ise) {
            //ignore
        }

        assertEquals("Addition beyond mem limit shouldn't get added", 1, list.size());
        assertEquals("Addition beyond mem limit shouldn't show up in usage", 1, list.estimatedMemoryUsage());
    }

    @Test
    public void basics() {
        list.add(TEST_NODE_STATE_ENTRY);
        assertEquals("Adding an item should change size", 1, list.size());
        assertTrue("Adding an item should be available", list.iterator().hasNext());
    }
}