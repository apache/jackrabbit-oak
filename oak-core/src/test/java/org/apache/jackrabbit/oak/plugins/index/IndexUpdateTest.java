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

import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexLookup;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public class IndexUpdateTest {

    private static final EditorHook HOOK = new EditorHook(
            new IndexUpdateProvider(new PropertyIndexEditorProvider()));

    private NodeState root = INITIAL_CONTENT;

    private NodeBuilder builder = root.builder();

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
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null);
        createIndexDefinition(
                builder.child("newchild").child("other")
                        .child(INDEX_DEFINITIONS_NAME), "subIndex", true,
                false, ImmutableSet.of("foo"), null);

        NodeState before = builder.getNodeState();

        // Add nodes
        builder.child("testRoot").setProperty("foo", "abc");
        builder.child("newchild").child("other").child("testChild")
                .setProperty("foo", "xyz");

        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        // first check that the index content nodes exist
        checkPathExists(indexed, INDEX_DEFINITIONS_NAME, "rootIndex",
                INDEX_CONTENT_NODE_NAME);
        checkPathExists(indexed, "newchild", "other", INDEX_DEFINITIONS_NAME,
                "subIndex", INDEX_CONTENT_NODE_NAME);

        PropertyIndexLookup lookup = new PropertyIndexLookup(indexed);
        assertEquals(ImmutableSet.of("testRoot"), find(lookup, "foo", "abc"));

        PropertyIndexLookup lookupChild = new PropertyIndexLookup(indexed
                .getChildNode("newchild").getChildNode("other"));
        assertEquals(ImmutableSet.of("testChild"),
                find(lookupChild, "foo", "xyz"));
        assertEquals(ImmutableSet.of(), find(lookupChild, "foo", "abc"));

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
        builder.child("testRoot").setProperty("foo", "abc");
        NodeState before = builder.getNodeState();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null);

        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        // first check that the index content nodes exist
        NodeState ns = checkPathExists(indexed, INDEX_DEFINITIONS_NAME,
                "rootIndex");
        checkPathExists(ns, INDEX_CONTENT_NODE_NAME);
        PropertyState ps = ns.getProperty(REINDEX_PROPERTY_NAME);
        assertNotNull(ps);
        assertFalse(ps.getValue(Type.BOOLEAN));

        // next, lookup
        PropertyIndexLookup lookup = new PropertyIndexLookup(indexed);
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
    public void testReindex2() throws Exception {
        builder.child("testRoot").setProperty("foo", "abc");

        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "rootIndex", true, false, ImmutableSet.of("foo"), null)
                .removeProperty("reindex");

        NodeState before = builder.getNodeState();
        builder.child(INDEX_DEFINITIONS_NAME).child("rootIndex")
                .setProperty(REINDEX_PROPERTY_NAME, true);
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        // first check that the index content nodes exist
        NodeState ns = checkPathExists(indexed, INDEX_DEFINITIONS_NAME,
                "rootIndex");
        checkPathExists(ns, INDEX_CONTENT_NODE_NAME);
        PropertyState ps = ns.getProperty(REINDEX_PROPERTY_NAME);
        assertNotNull(ps);
        assertFalse(ps.getValue(Type.BOOLEAN));

        // next, lookup
        PropertyIndexLookup lookup = new PropertyIndexLookup(indexed);
        assertEquals(ImmutableSet.of("testRoot"), find(lookup, "foo", "abc"));
    }

    @Test
    public void testIndexDefinitions() throws Exception {
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "existing", true, false, ImmutableSet.of("foo"), null);

        NodeState before = builder.getNodeState();
        NodeBuilder other = builder.child("test").child("other");
        // Add index definition
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "foo",
                true, false, ImmutableSet.of("foo"), null);
        createIndexDefinition(
                other.child(INDEX_DEFINITIONS_NAME), "index2", true, false,
                ImmutableSet.of("foo"), null);
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        // check that the index content nodes exist
        checkPathExists(indexed, INDEX_DEFINITIONS_NAME, "existing",
                INDEX_CONTENT_NODE_NAME);
        checkPathExists(indexed, "test", "other", INDEX_DEFINITIONS_NAME,
                "index2", INDEX_CONTENT_NODE_NAME);
    }

    private Set<String> find(PropertyIndexLookup lookup, String name,
            String value) {
        NodeState system = root.getChildNode(JCR_SYSTEM);
        NodeState types = system.getChildNode(JCR_NODE_TYPES);
        NodeState type = types.getChildNode(NT_BASE);
        SelectorImpl selector = new SelectorImpl(type, NT_BASE);
        Filter filter = new FilterImpl(selector, "SELECT * FROM [nt:base]");
        return Sets.newHashSet(lookup.query(filter, name,
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

}
