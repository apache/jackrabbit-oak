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
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexLookup;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

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
        IndexEditorProvider provider = new PropertyIndexEditorProvider();

        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        builder.child("testRoot").setProperty("foo", "abc");

        // merge it back in
        store.merge(builder, EmptyHook.INSTANCE, null);

        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider);
        async.run();
        NodeState root = store.getRoot();

        // first check that the index content nodes exist
        checkPathExists(root, INDEX_DEFINITIONS_NAME, "rootIndex",
                INDEX_CONTENT_NODE_NAME);
        assertFalse(root.getChildNode(INDEX_DEFINITIONS_NAME).hasChildNode(
                ":conflict"));

        PropertyIndexLookup lookup = new PropertyIndexLookup(root);
        assertEquals(ImmutableSet.of("testRoot"), find(lookup, "foo", "abc"));
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
        IndexEditorProvider provider = new PropertyIndexEditorProvider();

        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndexSecond", true, false, ImmutableSet.of("bar"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");

        builder.child("testRoot").setProperty("foo", "abc")
                .setProperty("bar", "def");
        builder.child("testSecond").setProperty("bar", "ghi");

        // merge it back in
        store.merge(builder, EmptyHook.INSTANCE, null);

        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider);
        async.run();
        NodeState root = store.getRoot();

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
        IndexEditorProvider provider = new PropertyIndexEditorProvider();

        NodeBuilder builder = store.getRoot().builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");
        createIndexDefinition(
                builder.child("newchild").child("other")
                        .child(INDEX_DEFINITIONS_NAME), "subIndex", true,
                false, ImmutableSet.of("foo"), null)
                .setProperty(ASYNC_PROPERTY_NAME, "async");

        builder.child("testRoot").setProperty("foo", "abc");
        builder.child("newchild").child("other").child("testChild")
                .setProperty("foo", "xyz");

        // merge it back in
        store.merge(builder, EmptyHook.INSTANCE, null);

        AsyncIndexUpdate async = new AsyncIndexUpdate("async", store, provider);
        async.run();
        NodeState root = store.getRoot();

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
