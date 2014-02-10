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
package org.apache.jackrabbit.oak.plugins.index.property;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.JcrConstants.NT_FILE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Set;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.query.index.TraversingIndex;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * Test the Property2 index mechanism.
 */
public class PropertyIndexTest {

    private static final int MANY = 100;

    private static final EditorHook HOOK = new EditorHook(
            new IndexUpdateProvider(new PropertyIndexEditorProvider()));
    
    @Test
    public void costEstimation() throws Exception {
        NodeState root = INITIAL_CONTENT;

        // Add index definition
        NodeBuilder builder = root.builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "foo",
                true, false, ImmutableSet.of("foo"), null);
        NodeState before = builder.getNodeState();

        // Add some content and process it through the property index hook
        for (int i = 0; i < MANY; i++) {
            builder.child("n" + i).setProperty("foo", "x" + i % 20);
        }
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        FilterImpl f = createFilter(indexed, NT_BASE);

        // Query the index
        PropertyIndexLookup lookup = new PropertyIndexLookup(indexed);
        double cost;
        
        cost = lookup.getCost(f, "foo", PropertyValues.newString("x1"));
        assertTrue("cost: " + cost, cost >= 6.5 && cost <= 7.5);
        
        cost = lookup.getCost(f, "foo", PropertyValues.newString(
                Arrays.asList("x1", "x2")));
        assertTrue("cost: " + cost, cost >= 11.5 && cost <= 12.5);
        
        cost = lookup.getCost(f, "foo", PropertyValues.newString(
                Arrays.asList("x1", "x2", "x3", "x4", "x5")));
        assertTrue("cost: " + cost, cost >= 26.5 && cost <= 27.5);

        cost = lookup.getCost(f, "foo", PropertyValues.newString(
                Arrays.asList("x1", "x2", "x3", "x4", "x5", "x6", "x7", "x8", "x9", "x0")));
        assertTrue("cost: " + cost, cost >= 51.5 && cost <= 52.5);

        cost = lookup.getCost(f, "foo", null);
        assertTrue("cost: " + cost, cost >= MANY);
    }

    @Test
    public void costMaxEstimation() throws Exception {
        NodeState root = EmptyNodeState.EMPTY_NODE;

        // Add index definition
        NodeBuilder builder = root.builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "foo",
                true, false, ImmutableSet.of("foo"), null);
        NodeState before = builder.getNodeState();

        // 100 nodes in the index:
        // with a single level /content cost is 121
        // adding a second level /content/data cost is133

        // 101 nodes in the index:
        // with a single level /content cost is 121
        // adding a second level /content/data cost is 133

        // 100 nodes, 12 levels deep, cost is 345
        // 101 nodes, 12 levels deep, cost is 345

        // threshold for estimation (PropertyIndexLookup.MAX_COST) is at 100
        int nodes = 101;
        int levels = 12;
        
        NodeBuilder data = builder;
        for (int i = 0; i < levels; i++) {
            data = data.child("l" + i);
        }
        for (int i = 0; i < nodes; i++) {
            NodeBuilder c = data.child("c_" + i);
            c.setProperty("foo", "azerty");
        }
        NodeState after = builder.getNodeState();
        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        FilterImpl f = createFilter(indexed, NT_BASE);

        PropertyIndexLookup lookup = new PropertyIndexLookup(indexed);
        double cost = lookup.getCost(f, "foo",
                PropertyValues.newString("azerty"));
        double traversal = new TraversingIndex().getCost(f, indexed);

        assertTrue("Estimated cost for " + nodes
                + " nodes should not be higher than traversal (" + cost + ")",
                cost < traversal);
    }

    @Test
    public void testPropertyLookup() throws Exception {
        NodeState root = INITIAL_CONTENT;

        // Add index definition
        NodeBuilder builder = root.builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "foo",
                true, false, ImmutableSet.of("foo"), null);
        NodeState before = builder.getNodeState();

        // Add some content and process it through the property index hook
        builder.child("a").setProperty("foo", "abc");
        builder.child("b").setProperty("foo", Arrays.asList("abc", "def"),
                Type.STRINGS);
        // plus lots of dummy content to highlight the benefit of indexing
        for (int i = 0; i < MANY; i++) {
            builder.child("n" + i).setProperty("foo", "xyz");
        }
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        FilterImpl f = createFilter(indexed, NT_BASE);

        // Query the index
        PropertyIndexLookup lookup = new PropertyIndexLookup(indexed);
        assertEquals(ImmutableSet.of("a", "b"), find(lookup, "foo", "abc", f));
        assertEquals(ImmutableSet.of("b"), find(lookup, "foo", "def", f));
        assertEquals(ImmutableSet.of(), find(lookup, "foo", "ghi", f));
        assertEquals(MANY, find(lookup, "foo", "xyz", f).size());
        assertEquals(MANY + 2, find(lookup, "foo", null, f).size());

        double cost;
        cost = lookup.getCost(f, "foo", PropertyValues.newString("xyz"));
        assertTrue("cost: " + cost, cost >= MANY);
        cost = lookup.getCost(f, "foo", null);
        assertTrue("cost: " + cost, cost >= MANY);
    }

    private static Set<String> find(PropertyIndexLookup lookup, String name,
            String value, Filter filter) {
        return Sets.newHashSet(lookup.query(filter, name, value == null ? null
                : PropertyValues.newString(value)));
    }

    @Test
    public void testCustomConfigPropertyLookup() throws Exception {
        NodeState root = INITIAL_CONTENT;

        // Add index definition
        NodeBuilder builder = root.builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "fooIndex", true, false, ImmutableSet.of("foo", "extrafoo"),
                null);
        NodeState before = builder.getNodeState();

        // Add some content and process it through the property index hook
        builder.child("a").setProperty("foo", "abc")
                .setProperty("extrafoo", "pqr");
        builder.child("b").setProperty("foo", Arrays.asList("abc", "def"),
                Type.STRINGS);
        // plus lots of dummy content to highlight the benefit of indexing
        for (int i = 0; i < MANY; i++) {
            builder.child("n" + i).setProperty("foo", "xyz");
        }
        NodeState after = builder.getNodeState();

        // Add an index
        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        FilterImpl f = createFilter(indexed, NT_BASE);

        // Query the index
        PropertyIndexLookup lookup = new PropertyIndexLookup(indexed);
        assertEquals(ImmutableSet.of("a", "b"), find(lookup, "foo", "abc", f));
        assertEquals(ImmutableSet.of("b"), find(lookup, "foo", "def", f));
        assertEquals(ImmutableSet.of(), find(lookup, "foo", "ghi", f));
        assertEquals(MANY, find(lookup, "foo", "xyz", f).size());
        assertEquals(ImmutableSet.of("a"), find(lookup, "extrafoo", "pqr", f));

        try {
            assertEquals(ImmutableSet.of(), find(lookup, "pqr", "foo", f));
            fail();
        } catch (IllegalArgumentException e) {
            // expected: no index for "pqr"
        }
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-666">OAK-666:
     *      Property2Index: node type is used when indexing, but ignored when
     *      querying</a>
     */
    @Test
    public void testCustomConfigNodeType() throws Exception {
        NodeState root = INITIAL_CONTENT;

        // Add index definitions
        NodeBuilder builder = root.builder();
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        createIndexDefinition(index, "fooIndex", true, false,
                ImmutableSet.of("foo", "extrafoo"),
                ImmutableSet.of(NT_UNSTRUCTURED));
        createIndexDefinition(index, "fooIndexFile", true, false,
                ImmutableSet.of("foo"), ImmutableSet.of(NT_FILE));
        NodeState before = builder.getNodeState();

        // Add some content and process it through the property index hook
        builder.child("a")
                .setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, Type.NAME)
                .setProperty("foo", "abc");
        builder.child("b")
                .setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, Type.NAME)
                .setProperty("foo", Arrays.asList("abc", "def"), Type.STRINGS);
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        FilterImpl f = createFilter(indexed, NT_UNSTRUCTURED);

        // Query the index
        PropertyIndexLookup lookup = new PropertyIndexLookup(indexed);
        assertEquals(ImmutableSet.of("a", "b"), find(lookup, "foo", "abc", f));
        assertEquals(ImmutableSet.of("b"), find(lookup, "foo", "def", f));
        assertEquals(ImmutableSet.of(), find(lookup, "foo", "ghi", f));

        try {
            assertEquals(ImmutableSet.of(), find(lookup, "pqr", "foo", f));
            fail();
        } catch (IllegalArgumentException e) {
            // expected: no index for "pqr"
        }
    }

    private static FilterImpl createFilter(NodeState root, String nodeTypeName) {
        NodeState system = root.getChildNode(JCR_SYSTEM);
        NodeState types = system.getChildNode(JCR_NODE_TYPES);
        NodeState type = types.getChildNode(nodeTypeName);
        SelectorImpl selector = new SelectorImpl(type, nodeTypeName);
        return new FilterImpl(selector, "SELECT * FROM [" + nodeTypeName + "]");
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-666">OAK-666:
     *      Property2Index: node type is used when indexing, but ignored when
     *      querying</a>
     */
    @Test
    public void testCustomConfigNodeTypeFallback() throws Exception {
        NodeState root = EMPTY_NODE;

        // Add index definitions
        NodeBuilder builder = root.builder();
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        createIndexDefinition(
                index, "fooIndex", true, false,
                ImmutableSet.of("foo", "extrafoo"), null);
        createIndexDefinition(
                index, "fooIndexFile", true, false,
                ImmutableSet.of("foo"), ImmutableSet.of(NT_FILE));
        NodeState before = builder.getNodeState();

        // Add some content and process it through the property index hook
        builder.child("a")
                .setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, Type.NAME)
                .setProperty("foo", "abc");
        builder.child("b")
                .setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, Type.NAME)
                .setProperty("foo", Arrays.asList("abc", "def"), Type.STRINGS);
        NodeState after = builder.getNodeState();

        // Add an index
        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        FilterImpl f = createFilter(after, NT_UNSTRUCTURED);

        // Query the index
        PropertyIndexLookup lookup = new PropertyIndexLookup(indexed);
        assertEquals(ImmutableSet.of("a", "b"), find(lookup, "foo", "abc", f));
        assertEquals(ImmutableSet.of("b"), find(lookup, "foo", "def", f));
        assertEquals(ImmutableSet.of(), find(lookup, "foo", "ghi", f));

        try {
            assertEquals(ImmutableSet.of(), find(lookup, "pqr", "foo", f));
            fail();
        } catch (IllegalArgumentException e) {
            // expected: no index for "pqr"
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testUnique() throws Exception {
        NodeState root = EMPTY_NODE;

        // Add index definition
        NodeBuilder builder = root.builder();
        createIndexDefinition(
                builder.child(INDEX_DEFINITIONS_NAME),
                "fooIndex", true, true, ImmutableSet.of("foo"), null);
        NodeState before = builder.getNodeState();
        builder.child("a").setProperty("foo", "abc");
        builder.child("b").setProperty("foo", Arrays.asList("abc", "def"),
                Type.STRINGS);
        NodeState after = builder.getNodeState();

        HOOK.processCommit(before, after, CommitInfo.EMPTY); // should throw
    }

    @Test
    public void testUniqueByTypeOK() throws Exception {
        NodeState root = EMPTY_NODE;

        // Add index definition
        NodeBuilder builder = root.builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "fooIndex", true, true, ImmutableSet.of("foo"),
                ImmutableSet.of("typeFoo"));
        NodeState before = builder.getNodeState();
        builder.child("a").setProperty(JCR_PRIMARYTYPE, "typeFoo", Type.NAME)
                .setProperty("foo", "abc");
        builder.child("b").setProperty(JCR_PRIMARYTYPE, "typeBar", Type.NAME)
                .setProperty("foo", "abc");
        NodeState after = builder.getNodeState();

        HOOK.processCommit(before, after, CommitInfo.EMPTY); // should not throw
    }

    @Test(expected = CommitFailedException.class)
    public void testUniqueByTypeKO() throws Exception {
        NodeState root = EMPTY_NODE;

        // Add index definition
        NodeBuilder builder = root.builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "fooIndex", true, true, ImmutableSet.of("foo"),
                ImmutableSet.of("typeFoo"));
        NodeState before = builder.getNodeState();
        builder.child("a").setProperty(JCR_PRIMARYTYPE, "typeFoo", Type.NAME)
                .setProperty("foo", "abc");
        builder.child("b").setProperty(JCR_PRIMARYTYPE, "typeFoo", Type.NAME)
                .setProperty("foo", "abc");
        NodeState after = builder.getNodeState();

        HOOK.processCommit(before, after, CommitInfo.EMPTY); // should throw
    }

    @Test
    public void testUniqueByTypeDelete() throws Exception {
        NodeState root = EMPTY_NODE;

        // Add index definition
        NodeBuilder builder = root.builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME),
                "fooIndex", true, true, ImmutableSet.of("foo"),
                ImmutableSet.of("typeFoo"));
        builder.child("a").setProperty(JCR_PRIMARYTYPE, "typeFoo", Type.NAME)
                .setProperty("foo", "abc");
        builder.child("b").setProperty(JCR_PRIMARYTYPE, "typeBar", Type.NAME)
                .setProperty("foo", "abc");
        NodeState before = builder.getNodeState();
        builder.getChildNode("b").remove();
        NodeState after = builder.getNodeState();

        HOOK.processCommit(before, after, CommitInfo.EMPTY); // should not throw
    }

}
