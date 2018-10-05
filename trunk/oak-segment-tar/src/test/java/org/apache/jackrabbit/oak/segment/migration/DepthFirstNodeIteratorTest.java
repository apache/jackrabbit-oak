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

package org.apache.jackrabbit.oak.segment.migration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.blob.migration.DepthFirstNodeIterator;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

public class DepthFirstNodeIteratorTest {

    private NodeStore store;

    @Before
    public void setup() throws CommitFailedException, IOException {
        store = SegmentNodeStoreBuilders.builder(new MemoryStore()).build();
        NodeBuilder rootBuilder = this.store.getRoot().builder();
        NodeBuilder countries = rootBuilder.child("countries");
        countries.child("uk").child("cities").child("london").child("districts").child("frognal");
        countries.child("germany");
        countries.child("france").child("cities").child("paris");
        store.merge(rootBuilder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    // The order of the returned nodes is not defined, that's why we have to
    // create 3 subtrees.
    @Test
    public void testIterate() {
        Map<String, String[]> subtrees = new HashMap<String, String[]>();
        subtrees.put("uk", new String[] { "cities", "london", "districts", "frognal" });
        subtrees.put("germany", new String[] {});
        subtrees.put("france", new String[] { "cities", "paris" });

        DepthFirstNodeIterator iterator = new DepthFirstNodeIterator(store.getRoot());
        assertTrue(iterator.hasNext());
        assertEquals("countries", iterator.next().getName());

        for (int i = 0; i < 3; i++) {
            assertTrue(iterator.hasNext());
            String country = iterator.next().getName();
            for (String node : subtrees.remove(country)) {
                assertTrue(iterator.hasNext());
                assertEquals(node, iterator.next().getName());
            }
        }
        assertFalse(iterator.hasNext());
        assertTrue(subtrees.isEmpty());
    }

    @Test
    public void testGetPath() {
        Map<String, String> nameToPath = new HashMap<String, String>();
        nameToPath.put("countries", "/countries");
        nameToPath.put("uk", "/countries/uk");
        nameToPath.put("frognal", "/countries/uk/cities/london/districts/frognal");
        nameToPath.put("paris", "/countries/france/cities/paris");

        DepthFirstNodeIterator iterator = new DepthFirstNodeIterator(store.getRoot());
        while (iterator.hasNext()) {
            String expectedPath = nameToPath.remove(iterator.next().getName());
            if (expectedPath == null) {
                continue;
            }
            assertEquals(expectedPath, iterator.getPath());
        }
        assertTrue(nameToPath.isEmpty());
    }

    @Test
    public void testSwitchRoot() {
        Map<String, String[]> subtrees = new HashMap<String, String[]>();
        subtrees.put("uk", new String[] { "cities", "london", "districts", "frognal" });
        subtrees.put("germany", new String[] {});
        subtrees.put("france", new String[] { "cities", "paris" });

        DepthFirstNodeIterator iterator = new DepthFirstNodeIterator(store.getRoot());
        assertTrue(iterator.hasNext());
        assertEquals("countries", iterator.next().getName());

        for (int i = 0; i < 3; i++) {
            assertTrue(iterator.hasNext());
            String country = iterator.next().getName();
            for (String node : subtrees.remove(country)) {
                assertTrue(iterator.hasNext());
                assertEquals(node, iterator.next().getName());
                iterator = iterator.switchRoot(store.getRoot());
            }
        }
        assertFalse(iterator.hasNext());
        assertTrue(subtrees.isEmpty());
    }

}
