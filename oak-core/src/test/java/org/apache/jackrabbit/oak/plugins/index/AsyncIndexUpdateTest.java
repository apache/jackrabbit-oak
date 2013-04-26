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
package org.apache.jackrabbit.oak.plugins.index;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ASYNC_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.jackrabbit.oak.plugins.index.AsyncIndexUpdate.IndexTask;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexLookup;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class AsyncIndexUpdateTest {

    // TODO test index config deletes

    private static Set<String> find(PropertyIndexLookup lookup, String name,
            String value) {
        return Sets.newHashSet(lookup.query(null, name,
                PropertyValues.newString(value)));
    }

    private static NodeState checkPathExists(NodeState state, String... verify) {
        NodeState c = state;
        for (String p : verify) {
            c = c.getChildNode(p);
            assertTrue(c.exists());
        }
        return c;
    }

    /**
     * Async Index Test
     * <ul>
     * <li>Add an index definition</li>
     * <li>Add some content</li>
     * <li>Search & verify</li>
     * </ul>
     * 
     */
    @Test
    public void testAsync() throws Exception {
        NodeStore store = new MemoryNodeStore();
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(0);
        IndexEditorProvider provider = new PropertyIndexEditorProvider();

        NodeStoreBranch branch = store.branch();
        NodeState root = branch.getHead();
        NodeBuilder builder = root.builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, true);
        builder.child("testRoot").setProperty("foo", "abc");

        // merge it back in
        branch.setRoot(builder.getNodeState());
        branch.merge(EmptyHook.INSTANCE);

        AsyncIndexUpdate async = new AsyncIndexUpdate(store, executor, provider);
        runIndexing(async, 1);
        root = store.getRoot();

        // first check that the index content nodes exist
        checkPathExists(root, INDEX_DEFINITIONS_NAME, "rootIndex",
                INDEX_CONTENT_NODE_NAME);

        PropertyIndexLookup lookup = new PropertyIndexLookup(root);
        assertEquals(ImmutableSet.of("testRoot"), find(lookup, "foo", "abc"));
    }

    private static void runIndexing(AsyncIndexUpdate async, int expectedActive) {
        async.run();
        Map<String, IndexTask> active = async.active;
        assertEquals(expectedActive, active.size());
        for (IndexTask task : active.values()) {
            task.run();
        }
    }

    /**
     * Async Index Test with 2 index defs at the same location
     * <ul>
     * <li>Add an index definition</li>
     * <li>Add some content</li>
     * <li>Search & verify</li>
     * </ul>
     * 
     */
    @Test
    public void testAsyncDouble() throws Exception {
        NodeStore store = new MemoryNodeStore();
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(0);
        IndexEditorProvider provider = new PropertyIndexEditorProvider();

        NodeStoreBranch branch = store.branch();
        NodeState root = branch.getHead();
        NodeBuilder builder = root.builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, true);
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndexSecond", true, false, ImmutableSet.of("bar"), null)
                .setProperty(ASYNC_PROPERTY_NAME, true);

        builder.child("testRoot").setProperty("foo", "abc")
                .setProperty("bar", "def");
        builder.child("testSecond").setProperty("bar", "ghi");

        // merge it back in
        branch.setRoot(builder.getNodeState());
        branch.merge(EmptyHook.INSTANCE);

        AsyncIndexUpdate async = new AsyncIndexUpdate(store, executor, provider);
        runIndexing(async, 1);
        root = store.getRoot();

        // first check that the index content nodes exist
        checkPathExists(root, INDEX_DEFINITIONS_NAME, "rootIndex",
                INDEX_CONTENT_NODE_NAME);
        checkPathExists(root, INDEX_DEFINITIONS_NAME, "rootIndexSecond",
                INDEX_CONTENT_NODE_NAME);

        PropertyIndexLookup lookup = new PropertyIndexLookup(root);
        assertEquals(ImmutableSet.of("testRoot"), find(lookup, "foo", "abc"));
        assertEquals(ImmutableSet.of(), find(lookup, "foo", "def"));
        assertEquals(ImmutableSet.of(), find(lookup, "foo", "ghi"));

        assertEquals(ImmutableSet.of(), find(lookup, "bar", "abc"));
        assertEquals(ImmutableSet.of("testRoot"), find(lookup, "bar", "def"));
        assertEquals(ImmutableSet.of("testSecond"), find(lookup, "bar", "ghi"));

    }

    /**
     * Async Index Test with 2 index defs at different tree locations
     * <ul>
     * <li>Add an index definition</li>
     * <li>Add some content</li>
     * <li>Search & verify</li>
     * </ul>
     * 
     */
    @Test
    public void testAsyncDoubleSubtree() throws Exception {
        NodeStore store = new MemoryNodeStore();
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(0);
        IndexEditorProvider provider = new PropertyIndexEditorProvider();

        NodeStoreBranch branch = store.branch();
        NodeState root = branch.getHead();
        NodeBuilder builder = root.builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, true);
        createIndexDefinition(
                builder.child("newchild").child("other")
                        .child(INDEX_DEFINITIONS_NAME), "subIndex", true,
                false, ImmutableSet.of("foo"), null).setProperty(
                ASYNC_PROPERTY_NAME, true);

        builder.child("testRoot").setProperty("foo", "abc");
        builder.child("newchild").child("other").child("testChild")
                .setProperty("foo", "xyz");

        // merge it back in
        branch.setRoot(builder.getNodeState());
        branch.merge(EmptyHook.INSTANCE);

        AsyncIndexUpdate async = new AsyncIndexUpdate(store, executor, provider);
        runIndexing(async, 1);
        root = store.getRoot();

        // first check that the index content nodes exist
        checkPathExists(root, INDEX_DEFINITIONS_NAME, "rootIndex",
                INDEX_CONTENT_NODE_NAME);
        checkPathExists(root, "newchild", "other", INDEX_DEFINITIONS_NAME,
                "subIndex", INDEX_CONTENT_NODE_NAME);

        PropertyIndexLookup lookup = new PropertyIndexLookup(root);
        assertEquals(ImmutableSet.of("testRoot"), find(lookup, "foo", "abc"));

        PropertyIndexLookup lookupChild = new PropertyIndexLookup(root
                .getChildNode("newchild").getChildNode("other"));
        assertEquals(ImmutableSet.of("testChild"),
                find(lookupChild, "foo", "xyz"));
        assertEquals(ImmutableSet.of(), find(lookupChild, "foo", "abc"));
    }

}
