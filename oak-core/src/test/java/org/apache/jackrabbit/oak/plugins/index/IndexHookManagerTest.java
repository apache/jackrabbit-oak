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

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.p2.Property2IndexHookProvider;
import org.apache.jackrabbit.oak.plugins.index.p2.Property2IndexLookup;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class IndexHookManagerTest {

    /**
     * Simple Test
     * <ul>
     * <li>Add an index definition</li>
     * <li>Add some content</li>
     * <li>Search & verify</li>
     * </ul>
     * 
     */
    @Test
    public void test() throws Exception {
        NodeState root = EMPTY_NODE;

        NodeBuilder builder = root.builder();

        createIndexDefinition(builder.child("oak:index"), "rootIndex", true,
                false, ImmutableSet.of("foo"), null);
        createIndexDefinition(
                builder.child("newchild").child("other").child("oak:index"),
                "subIndex", true, false, ImmutableSet.of("foo"), null);

        NodeState before = builder.getNodeState();

        // Add nodes
        builder.child("testRoot").setProperty("foo", "abc");
        builder.child("newchild").child("other").child("testChild")
                .setProperty("foo", "xyz");

        NodeState after = builder.getNodeState();

        IndexHookManager im = IndexHookManager
                .of(new Property2IndexHookProvider());
        EditorHook hook = new EditorHook(im);
        NodeState indexed = hook.processCommit(before, after);

        // first check that the index content nodes exist
        checkPathExists(indexed, "oak:index", "rootIndex", ":index");
        checkPathExists(indexed, "newchild", "other", "oak:index", "subIndex",
                ":index");

        Property2IndexLookup lookup = new Property2IndexLookup(indexed);
        assertEquals(ImmutableSet.of("testRoot"), find(lookup, "foo", "abc"));

        Property2IndexLookup lookupChild = new Property2IndexLookup(indexed
                .getChildNode("newchild").getChildNode("other"));
        assertEquals(ImmutableSet.of("testChild"),
                find(lookupChild, "foo", "xyz"));
        assertEquals(ImmutableSet.of(), find(lookupChild, "foo", "abc"));

    }

    private static Set<String> find(Property2IndexLookup lookup, String name,
            String value) {
        return Sets.newHashSet(lookup.query(null, name,
                PropertyValues.newString(value)));
    }

    /**
     * Reindex Test
     * <ul>
     * <li>Add some content</li>
     * <li>Add an index definition with the reindex flag set</li>
     * <li>Search & verify</li>
     * </ul>
     */
    @Test
    public void testReindex() throws Exception {
        NodeState root = EMPTY_NODE;

        NodeBuilder builder = root.builder();

        builder.child("testRoot").setProperty("foo", "abc");
        NodeState before = builder.getNodeState();
        createIndexDefinition(builder.child("oak:index"), "rootIndex", true,
                false, ImmutableSet.of("foo"), null);

        NodeState after = builder.getNodeState();

        IndexHookManager im = IndexHookManager
                .of(new Property2IndexHookProvider());
        EditorHook hook = new EditorHook(im);
        NodeState indexed = hook.processCommit(before, after);

        // first check that the index content nodes exist
        NodeState ns = checkPathExists(indexed, "oak:index", "rootIndex");
        checkPathExists(ns, ":index");
        PropertyState ps = ns.getProperty(REINDEX_PROPERTY_NAME);
        assertNotNull(ps);
        assertFalse(ps.getValue(Type.BOOLEAN).booleanValue());

        // next, lookup
        Property2IndexLookup lookup = new Property2IndexLookup(indexed);
        assertEquals(ImmutableSet.of("testRoot"), find(lookup, "foo", "abc"));
    }

    /**
     * Reindex Test
     * <ul>
     * <li>Add some content</li>
     * <li>Add an index definition with no reindex flag</li>
     * <li>Search & verify</li>
     * </ul>
     */
    @Test
    public void testReindex2() throws Exception {
        NodeState root = EMPTY_NODE;

        NodeBuilder builder = root.builder();

        builder.child("testRoot").setProperty("foo", "abc");
        NodeState before = builder.getNodeState();

        createIndexDefinition(builder.child("oak:index"), "rootIndex", true,
                false, ImmutableSet.of("foo"), null).removeProperty("reindex");

        NodeState after = builder.getNodeState();

        IndexHookManager im = IndexHookManager
                .of(new Property2IndexHookProvider());
        EditorHook hook = new EditorHook(im);
        NodeState indexed = hook.processCommit(before, after);

        // first check that the index content nodes exist
        NodeState ns = checkPathExists(indexed, "oak:index", "rootIndex");
        checkPathExists(ns, ":index");
        PropertyState ps = ns.getProperty(REINDEX_PROPERTY_NAME);
        assertNotNull(ps);
        assertFalse(ps.getValue(Type.BOOLEAN).booleanValue());

        // next, lookup
        Property2IndexLookup lookup = new Property2IndexLookup(indexed);
        assertEquals(ImmutableSet.of("testRoot"), find(lookup, "foo", "abc"));
    }

    /**
     * Reindex Test
     * <ul>
     * <li>Add some content & an index definition</li>
     * <li>Update the index def by setting the reindex flag to true</li>
     * <li>Search & verify</li>
     * </ul>
     */
    @Test
    public void testReindex3() throws Exception {
        NodeState root = EMPTY_NODE;

        NodeBuilder builder = root.builder();
        builder.child("testRoot").setProperty("foo", "abc");

        createIndexDefinition(builder.child("oak:index"), "rootIndex", true,
                false, ImmutableSet.of("foo"), null).removeProperty("reindex");

        NodeState before = builder.getNodeState();
        builder.child("oak:index").child("rootIndex")
                .setProperty(REINDEX_PROPERTY_NAME, true);
        NodeState after = builder.getNodeState();

        IndexHookManager im = IndexHookManager
                .of(new Property2IndexHookProvider());
        EditorHook hook = new EditorHook(im);
        NodeState indexed = hook.processCommit(before, after);

        // first check that the index content nodes exist
        NodeState ns = checkPathExists(indexed, "oak:index", "rootIndex");
        checkPathExists(ns, ":index");
        PropertyState ps = ns.getProperty(REINDEX_PROPERTY_NAME);
        assertNotNull(ps);
        assertFalse(ps.getValue(Type.BOOLEAN).booleanValue());

        // next, lookup
        Property2IndexLookup lookup = new Property2IndexLookup(indexed);
        assertEquals(ImmutableSet.of("testRoot"), find(lookup, "foo", "abc"));
    }

    @Test
    public void testIndexDefinitions() throws Exception {
        NodeState root = EMPTY_NODE;

        NodeBuilder builder = root.builder();
        createIndexDefinition(builder.child("oak:index"), "existing", true,
                false, ImmutableSet.of("foo"), null);

        NodeState before = builder.getNodeState();
        // Add index definition
        createIndexDefinition(builder.child("oak:index"), "foo", true, false,
                ImmutableSet.of("foo"), null);
        createIndexDefinition(
                builder.child("test").child("other").child("oak:index"),
                "index2", true, false, ImmutableSet.of("foo"), null);
        NodeState after = builder.getNodeState();

        IndexHookManager im = IndexHookManager
                .of(new Property2IndexHookProvider());
        EditorHook hook = new EditorHook(im);
        NodeState indexed = hook.processCommit(before, after);

        // check that the index content nodes exist
        checkPathExists(indexed, "oak:index", "existing", ":index");
        checkPathExists(indexed, "test", "other", "oak:index", "index2",
                ":index");
    }

    private static NodeState checkPathExists(NodeState state, String... verify) {
        NodeState c = state;
        for (String p : verify) {
            c = c.getChildNode(p);
            assertTrue(c.exists());
        }
        return c;
    }

}
