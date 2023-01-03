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

package org.apache.jackrabbit.oak.index.indexer.document.flatfile.linkedList;

import com.google.common.collect.Iterators;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry.NodeStateEntryBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;

import java.io.IOException;
import java.util.Iterator;

public class FlatFileBufferLinkedListTest {

    protected NodeStateEntryList list;

    @Before
    public void setup() throws IOException {
        list = new FlatFileBufferLinkedList();
    }

    @Test
    public void add() {
        try {
            list.add(null);
            Assert.fail("Adding null must throw IllegalArgumentException");
        } catch (IllegalArgumentException iae) {
            //ignore
        }

        list.add(testNode("/"));
    }

    @Test
    public void remove() {
        try {
            list.remove();
            Assert.fail("Must fail to remove from an empty list");
        } catch (IllegalStateException ise) {
            //ignore
        }

        list.add(testNode("/"));
        Assert.assertEquals("Should get item on removal", testNode("/"), list.remove());
    }

    @Test
    public void iterator() {
        Assert.assertEquals("empty list must be 0-sized", 0, Iterators.size(list.iterator()));

        list.add(testNode("/"));
        Assert.assertEquals("single entry list must be 1-sized", 1, Iterators.size(list.iterator()));
        Assert.assertEquals("single entry list must be 1-sized on separate iterators",
                1, Iterators.size(list.iterator()));

        list.add(testNode("/a"));
        Assert.assertEquals("2 entries in list must be 2-sized", 2, Iterators.size(list.iterator()));

        Assert.assertEquals("2 entries in list must be 2-sized on separate iterators",
                2, Iterators.size(list.iterator()));

        Iterator<NodeStateEntry> iter2 = list.iterator();
        Iterator<NodeStateEntry> iter1 = list.iterator();
        Assert.assertEquals("/", iter2.next().toString());
        Assert.assertEquals("2 entries in list must be 1-sized after consuming an item",
                1, Iterators.size(iter2));
        Assert.assertEquals("2 entries in list must be 2-sized even if some other iterator consumed an item",
                2, Iterators.size(iter1));

        list.add(testNode("/b"));
        iter1 = list.iterator();
        iter2 = list.iterator();

        //move iter to point at node being removed below
        Assert.assertEquals("/", iter1.next().toString());
        // move iter beyond node being removed - this should remain valid
        Assert.assertEquals("/", iter2.next().toString());
        Assert.assertEquals("/a", iter2.next().toString());

        Assert.assertEquals("/", list.remove().toString());
        Assert.assertEquals("/a", list.remove().toString());
        try {
            iter1.next();
            Assert.fail("Iterator state once removed from list can't be traversed");
        } catch (IllegalStateException ise) {
            //ignore
        }
        //this should work
        Assert.assertEquals(testNode("/b"), iter2.next());
        Assert.assertEquals("2 entries in list must be 1-sized after removal of an iterm",
                1, Iterators.size(list.iterator()));
    }

    @Test
    public void size() {
        Assert.assertEquals("empty list must be 0-sized", 0, list.size());

        list.add(testNode("/"));
        Assert.assertEquals("single entry list must be 1-sized", 1, list.size());
        Assert.assertEquals("single entry list must be 1-sized on separate iterators", 1, list.size());

        list.add(testNode("/"));
        Assert.assertEquals("2 entries in list must be 2-sized", 2, list.size());

        Assert.assertEquals("2 entries in list must be 2-sized on separate iterators", 2, list.size());

        list.remove();
        Assert.assertEquals("2 entries in list must be 1-sized after removing an item", 1, list.size());
    }

    @Test
    public void isEmpty() {
        Assert.assertTrue("Empty list should be empty", list.isEmpty());

        list.add(testNode("/"));
        Assert.assertFalse("Non-empty list should be non-empty", list.isEmpty());

        list.remove();
        Assert.assertTrue("Empty list due to removal should be empty", list.isEmpty());
    }

    @Test
    public void memUsage() {
        Assert.assertEquals("Empty list must be estimate 0", 0, list.estimatedMemoryUsage());

        list.add(new NodeStateEntryBuilder(EMPTY_NODE, "/").withMemUsage(20).build());
        Assert.assertEquals(20, list.estimatedMemoryUsage());

        list.add(new NodeStateEntryBuilder(EMPTY_NODE, "/").withMemUsage(30).build());
        Assert.assertEquals(50, list.estimatedMemoryUsage());

        list.remove();
        Assert.assertEquals(30, list.estimatedMemoryUsage());
    }

    @Test
    public void memLimit() {
        list = new FlatFileBufferLinkedList(10);
        NodeStateEntry e10Bytes = new NodeStateEntryBuilder(EMPTY_NODE, "/").withMemUsage(10).build();
        NodeStateEntry e1Byte = new NodeStateEntryBuilder(EMPTY_NODE, "/").withMemUsage(1).build();

        list.add(e10Bytes); //this should succeed

        list.remove();
        list.add(e1Byte);
        try {
            list.add(e10Bytes);
        } catch (IllegalStateException ise) {
            //ignore
        }

        Assert.assertEquals("Addition beyond mem limit shouldn't get added", 1, list.size());
        Assert.assertEquals("Addition beyond mem limit shouldn't show up in usage", 1, list.estimatedMemoryUsage());
    }

    @Test
    public void basics() {
        list.add(testNode("/"));
        Assert.assertEquals("Adding an item should change size", 1, list.size());
        Assert.assertTrue("Adding an item should be available", list.iterator().hasNext());
    }

    private NodeStateEntry testNode(String n) {
        return new NodeStateEntryBuilder(EMPTY_NODE, n).build();
    }
}
