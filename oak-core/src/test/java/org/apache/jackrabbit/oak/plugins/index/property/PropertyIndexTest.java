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

import static com.google.common.collect.ImmutableSet.of;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.JcrConstants.NT_FILE;
import static org.apache.jackrabbit.JcrConstants.NT_UNSTRUCTURED;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditor.COUNT_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.apache.jackrabbit.oak.spi.filter.PathFilter.PROP_EXCLUDED_PATHS;
import static org.apache.jackrabbit.oak.spi.filter.PathFilter.PROP_INCLUDED_PATHS;
import static org.apache.jackrabbit.oak.spi.state.NodeStateUtils.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.ContentMirrorStoreStrategy;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.query.NodeStateNodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.QueryEngineSettings;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfo;
import org.apache.jackrabbit.oak.query.ast.NodeTypeInfoProvider;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.query.index.TraversingIndex;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeHook;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.read.ListAppender;
import ch.qos.logback.core.spi.FilterReply;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
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
        NodeBuilder index = createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "foo",
                true, false, ImmutableSet.of("foo"), null);
        // disable the estimation
        index.setProperty("entryCount", -1);        
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

    /**
     * This is essentially same test as {@link #costEstimation()} with one difference that it uses
     * path constraint in query and creates similar trees under 2 branches {@code path1} and {@code path2}.
     * The cost estimation is then verified to be same as that in {@code costEstimation} for query under {@code path1}
     * @throws Exception
     */
    @Test
    public void pathBasedCostEstimation() throws Exception {
        NodeState root = INITIAL_CONTENT;

        // Add index definition
        NodeBuilder builder = root.builder();
        NodeBuilder index = createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "foo",
                true, false, ImmutableSet.of("foo"), null);
        // disable the estimation
        index.setProperty("entryCount", -1);
        builder.setProperty(COUNT_PROPERTY_NAME, (long) MANY * 2, Type.LONG);
        NodeState before = builder.getNodeState();

        NodeBuilder path1 = builder.child("path1");
        NodeBuilder path2 = builder.child("path2");
        // Add some content and process it through the property index hook
        for (int i = 0; i < MANY; i++) {
            path1.child("n" + i).setProperty("foo", "x" + i % 20);
            path2.child("n" + i).setProperty("foo", "x" + i % 20);
        }
        path1.setProperty(COUNT_PROPERTY_NAME, (long) MANY, Type.LONG);
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        FilterImpl f = createFilter(indexed, NT_BASE);
        f.restrictPath("/path1", Filter.PathRestriction.ALL_CHILDREN);

        // Query the index
        PropertyIndexLookup lookup = new PropertyIndexLookup(indexed);
        double cost;

        cost = lookup.getCost(f, "foo", PropertyValues.newString("x1"));
        assertTrue("cost: " + cost, cost >= 10 && cost <= 14);

        cost = lookup.getCost(f, "foo", PropertyValues.newString(
                Arrays.asList("x1", "x2")));
        assertTrue("cost: " + cost, cost >= 20 && cost <= 24);

        cost = lookup.getCost(f, "foo", PropertyValues.newString(
                Arrays.asList("x1", "x2", "x3", "x4", "x5")));
        assertTrue("cost: " + cost, cost >= 50 && cost <= 54);

        cost = lookup.getCost(f, "foo", PropertyValues.newString(
                Arrays.asList("x1", "x2", "x3", "x4", "x5", "x6", "x7", "x8", "x9", "x0")));
        assertTrue("cost: " + cost, cost >= 120 && cost <= 124);

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
        // add more nodes (to make traversal more expensive)
        for (int i = 0; i < 10000; i++) {
            data.child("cx_" + i);
        }
        NodeState after = builder.getNodeState();
        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        FilterImpl f = createFilter(indexed, NT_BASE);

        PropertyIndexLookup lookup = new PropertyIndexLookup(indexed);
        double cost = lookup.getCost(f, "foo",
                PropertyValues.newString("azerty"));
        double traversal = new TraversingIndex().getCost(f, indexed);

        assertTrue("Estimated cost for " + nodes
                + " nodes should not be higher than traversal (" + cost + " < " + traversal + ")",
                cost < traversal);
    }

    @Test
    public void testPropertyLookup() throws Exception {
        NodeState root = INITIAL_CONTENT;

        // Add index definition
        NodeBuilder builder = root.builder();
        NodeBuilder index = createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "foo",
                true, false, ImmutableSet.of("foo"), null);
        index.setProperty("entryCount", -1);
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

    @Test
    public void testPathAwarePropertyLookup() throws Exception {
        NodeState root = INITIAL_CONTENT;

        // Add index definition
        NodeBuilder builder = root.builder();
        createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "foo",
                true, false, ImmutableSet.of("foo"), null);
        NodeState before = builder.getNodeState();

        // Add some content and process it through the property index hook
        builder.child("a").setProperty("foo", "abc");
        builder.child("b").setProperty("foo", "abc");

        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        FilterImpl f = createFilter(indexed, NT_BASE);
        f.restrictPath("/a", Filter.PathRestriction.ALL_CHILDREN);

        // Query the index
        PropertyIndexLookup lookup = new PropertyIndexLookup(indexed);
        assertEquals(ImmutableSet.of("a"), find(lookup, "foo", "abc", f));
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
        NodeTypeInfoProvider nodeTypes = new NodeStateNodeTypeInfoProvider(root);
        NodeTypeInfo type = nodeTypes.getNodeTypeInfo(nodeTypeName);        
        SelectorImpl selector = new SelectorImpl(type, nodeTypeName);
        return new FilterImpl(selector, "SELECT * FROM [" + nodeTypeName + "]", new QueryEngineSettings());
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
    
    @Test
    public void valuePattern() throws Exception {
        NodeState root = EMPTY_NODE;

        // Add index definitions
        NodeBuilder builder = root.builder();
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder indexDef = createIndexDefinition(
                index, "fooIndex", true, false,
                ImmutableSet.of("foo"), null);
        indexDef.setProperty(IndexConstants.VALUE_PATTERN, "(a.*|b)");
        NodeState before = builder.getNodeState();

        // Add some content and process it through the property index hook
        builder.child("a")
                .setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, Type.NAME)
                .setProperty("foo", "a");
        builder.child("a1")
                .setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, Type.NAME)
                .setProperty("foo", "a1");
        builder.child("b")
                .setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, Type.NAME)
                .setProperty("foo", "b");
        builder.child("c")
                .setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, Type.NAME)
                .setProperty("foo", "c");
        NodeState after = builder.getNodeState();

        // Add an index
        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        FilterImpl f = createFilter(after, NT_UNSTRUCTURED);

        // Query the index
        PropertyIndexLookup lookup = new PropertyIndexLookup(indexed);
        PropertyIndex pIndex = new PropertyIndex(Mounts.defaultMountInfoProvider());
        assertEquals(ImmutableSet.of("a"), find(lookup, "foo", "a", f));
        assertEquals(ImmutableSet.of("a1"), find(lookup, "foo", "a1", f));
        assertEquals(ImmutableSet.of("b"), find(lookup, "foo", "b", f));

        // expected: no index for "is not null"
        assertTrue(pIndex.getCost(f, indexed) == Double.POSITIVE_INFINITY);
        
        ArrayList<PropertyValue> list = new ArrayList<PropertyValue>();
        list.add(PropertyValues.newString("c"));
        f.restrictPropertyAsList("foo", list);
        // expected: no index for value c
        assertTrue(pIndex.getCost(f, indexed) == Double.POSITIVE_INFINITY);

        f = createFilter(after, NT_UNSTRUCTURED);
        list = new ArrayList<PropertyValue>();
        list.add(PropertyValues.newString("a"));
        f.restrictPropertyAsList("foo", list);
        // expected: no index for value a
        assertTrue(pIndex.getCost(f, indexed) < Double.POSITIVE_INFINITY);

    }    
    
    @Test
    public void valuePatternExclude() throws Exception {
        NodeState root = EMPTY_NODE;
        // Add index definitions
        NodeBuilder builder = root.builder();
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder indexDef = createIndexDefinition(
                index, "fooIndex", true, false,
                ImmutableSet.of("foo"), null);
        indexDef.setProperty(IndexConstants.VALUE_EXCLUDED_PREFIXES, "test");
        valuePatternExclude0(builder);
    }

    @Test
    public void valuePatternExclude2() throws Exception {
        NodeState root = EMPTY_NODE;
        // Add index definitions
        NodeBuilder builder = root.builder();
        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
        NodeBuilder indexDef = createIndexDefinition(
                index, "fooIndex", true, false,
                ImmutableSet.of("foo"), null);
        PropertyState ps = PropertyStates.createProperty(
                IndexConstants.VALUE_EXCLUDED_PREFIXES,
                Arrays.asList("test"),
                Type.STRINGS);
        indexDef.setProperty(ps);
        valuePatternExclude0(builder);
    }

    private void valuePatternExclude0(NodeBuilder builder) throws CommitFailedException {
        NodeState before = builder.getNodeState();
        
        // Add some content and process it through the property index hook
        builder.child("a")
                .setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, Type.NAME)
                .setProperty("foo", "a");
        builder.child("a1")
                .setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, Type.NAME)
                .setProperty("foo", "a1");
        builder.child("b")
                .setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, Type.NAME)
                .setProperty("foo", "b");
        builder.child("c")
                .setProperty(JCR_PRIMARYTYPE, NT_UNSTRUCTURED, Type.NAME)
                .setProperty("foo", "c");
        NodeState after = builder.getNodeState();

        // Add an index
        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        FilterImpl f = createFilter(after, NT_UNSTRUCTURED);

        // Query the index
        PropertyIndexLookup lookup = new PropertyIndexLookup(indexed);
        PropertyIndex pIndex = new PropertyIndex(Mounts.defaultMountInfoProvider());
        assertEquals(ImmutableSet.of("a"), find(lookup, "foo", "a", f));
        assertEquals(ImmutableSet.of("a1"), find(lookup, "foo", "a1", f));
        assertEquals(ImmutableSet.of("b"), find(lookup, "foo", "b", f));

        // expected: no index for "is not null", "= 'test'", "like 't%'"
        assertTrue(pIndex.getCost(f, indexed) == Double.POSITIVE_INFINITY);
        f.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("test"));
        assertTrue(pIndex.getCost(f, indexed) == Double.POSITIVE_INFINITY);
        f = createFilter(after, NT_UNSTRUCTURED);
        f.restrictProperty("foo", Operator.LIKE, PropertyValues.newString("t%"));
        assertTrue(pIndex.getCost(f, indexed) == Double.POSITIVE_INFINITY);
        f = createFilter(after, NT_UNSTRUCTURED);
        
        
        // expected: index for "like 'a%'"
        f.restrictProperty("foo", Operator.GREATER_OR_EQUAL, PropertyValues.newString("a"));
        f.restrictProperty("foo", Operator.LESS_OR_EQUAL, PropertyValues.newString("a0"));
        assertTrue(pIndex.getCost(f, indexed) < Double.POSITIVE_INFINITY);
        f = createFilter(after, NT_UNSTRUCTURED);
        
        // expected: index for value c
        ArrayList<PropertyValue> list = new ArrayList<PropertyValue>();
        list.add(PropertyValues.newString("c"));
        f.restrictPropertyAsList("foo", list);
        assertTrue(pIndex.getCost(f, indexed) < Double.POSITIVE_INFINITY);

        // expected: index for value a
        f = createFilter(after, NT_UNSTRUCTURED);
        list = new ArrayList<PropertyValue>();
        list.add(PropertyValues.newString("a"));
        f.restrictPropertyAsList("foo", list);
        assertTrue(pIndex.getCost(f, indexed) < Double.POSITIVE_INFINITY);
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

        // should throw
        HOOK.processCommit(before, after, CommitInfo.EMPTY); 
    }
    
    @Test
    public void testUpdateUnique() throws Exception {
        NodeState root = EMPTY_NODE;

        NodeBuilder builder = root.builder();
        createIndexDefinition(
                builder.child(INDEX_DEFINITIONS_NAME),
                "fooIndex", true, true, ImmutableSet.of("foo"), null);
        NodeState before = builder.getNodeState();
        builder.child("a").setProperty("foo", "abc");
        NodeState after = builder.getNodeState();
        NodeState done = HOOK.processCommit(before, after, CommitInfo.EMPTY); 

        // remove, and then re-add the same node
        builder = done.builder();
        builder.child("a").setProperty("foo", "abc");
        after = builder.getNodeState();
        
        // apply the changes to the state before adding the entries
        done = HOOK.processCommit(before, after, CommitInfo.EMPTY); 
        
        // re-apply the changes
        done = HOOK.processCommit(done, after, CommitInfo.EMPTY); 
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

    @Test
    public void traversalWarning() throws Exception {
        ListAppender appender = createAndRegisterAppender();

        int testDataSize = ContentMirrorStoreStrategy.TRAVERSING_WARN;
        NodeState indexed = createTestData(testDataSize);
        assertEquals(testDataSize, getResultSize(indexed, "foo", "bar"));
        assertFalse(appender.list.isEmpty());

        appender.list.clear();

        testDataSize = 100;
        indexed = createTestData(100);
        assertEquals(testDataSize, getResultSize(indexed, "foo", "bar"));
        assertTrue("Warning should not be logged for traversing " + testDataSize,
                appender.list.isEmpty());
        deregisterAppender(appender);
    }

    @Test
    public void testPathInclude() throws Exception {
        NodeState root = INITIAL_CONTENT;

        // Add index definition
        NodeBuilder builder = root.builder();
        NodeBuilder index = createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "foo",
                true, false, ImmutableSet.of("foo"), null);
        index.setProperty(createProperty(PROP_INCLUDED_PATHS, of("/test/a"), Type.STRINGS));
        NodeState before = builder.getNodeState();

        // Add some content and process it through the property index hook
        builder.child("test").child("a").setProperty("foo", "abc");
        builder.child("test").child("b").setProperty("foo", "abc");
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        FilterImpl f = createFilter(indexed, NT_BASE);

        // Query the index
        PropertyIndexLookup lookup = new PropertyIndexLookup(indexed);
        assertEquals(ImmutableSet.of("test/a"), find(lookup, "foo", "abc", f));
    }

    @Test
    public void testPathExclude() throws Exception {
        NodeState root = INITIAL_CONTENT;

        // Add index definition
        NodeBuilder builder = root.builder();
        NodeBuilder index = createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "foo",
                true, false, ImmutableSet.of("foo"), null);
        index.setProperty(createProperty(PROP_EXCLUDED_PATHS, of("/test/a"), Type.STRINGS));
        NodeState before = builder.getNodeState();

        // Add some content and process it through the property index hook
        builder.child("test").child("a").setProperty("foo", "abc");
        builder.child("test").child("b").setProperty("foo", "abc");
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        FilterImpl f = createFilter(indexed, NT_BASE);
        f.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("abc"));

        // Query the index
        PropertyIndexLookup lookup = new PropertyIndexLookup(indexed);
        assertEquals(ImmutableSet.of("test/b"), find(lookup, "foo", "abc", f));

        //no path restriction, opt out
        PropertyIndexPlan plan = new PropertyIndexPlan("plan", root, index.getNodeState(), f);
        assertTrue(Double.POSITIVE_INFINITY == plan.getCost());

        //path restriction is not an ancestor of excluded path, index may be used
        f.setPath("/test2");
        plan = new PropertyIndexPlan("plan", root, index.getNodeState(), f);
        assertTrue(Double.POSITIVE_INFINITY != plan.getCost());

        //path restriction is an ancestor of excluded path, opt out
        f.setPath("/test");
        plan = new PropertyIndexPlan("plan", root, index.getNodeState(), f);
        assertTrue(Double.POSITIVE_INFINITY == plan.getCost());
    }

    @Test
    public void testPathIncludeExclude() throws Exception {
        NodeState root = INITIAL_CONTENT;

        // Add index definition
        NodeBuilder builder = root.builder();
        NodeBuilder index = createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "foo",
                true, false, ImmutableSet.of("foo"), null);
        index.setProperty(createProperty(PROP_INCLUDED_PATHS, of("/test/a"), Type.STRINGS));
        index.setProperty(createProperty(PROP_EXCLUDED_PATHS, of("/test/a/b"), Type.STRINGS));
        NodeState before = builder.getNodeState();

        // Add some content and process it through the property index hook
        builder.child("test").child("a").setProperty("foo", "abc");
        builder.child("test").child("a").child("b").setProperty("foo", "abc");
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        FilterImpl f = createFilter(indexed, NT_BASE);
        f.restrictProperty("foo", Operator.EQUAL, PropertyValues.newString("abc"));

        // Query the index
        PropertyIndexLookup lookup = new PropertyIndexLookup(indexed);
        assertEquals(ImmutableSet.of("test/a"), find(lookup, "foo", "abc", f));

        //no path restriction, opt out
        PropertyIndexPlan plan = new PropertyIndexPlan("plan", root, index.getNodeState(), f);
        assertTrue(Double.POSITIVE_INFINITY == plan.getCost());

        //path restriction is not an ancestor of excluded path, index may be used
        f.setPath("/test/a/x");
        plan = new PropertyIndexPlan("plan", root, index.getNodeState(), f);
        assertTrue(Double.POSITIVE_INFINITY != plan.getCost());

        //path restriction is an ancestor of excluded path but no included path, opt out
        f.setPath("/test/a/b");
        plan = new PropertyIndexPlan("plan", root, index.getNodeState(), f);
        assertTrue(Double.POSITIVE_INFINITY == plan.getCost());

        //path restriction is an ancestor of excluded path, opt out
        f.setPath("/test/a");
        plan = new PropertyIndexPlan("plan", root, index.getNodeState(), f);
        assertTrue(Double.POSITIVE_INFINITY == plan.getCost());
}

    @Test
    public void testPathExcludeInclude() throws Exception{
        NodeState root = INITIAL_CONTENT;

        // Add index definition
        NodeBuilder builder = root.builder();
        NodeBuilder index = createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "foo",
                true, false, ImmutableSet.of("foo"), null);
        index.setProperty(createProperty(PROP_INCLUDED_PATHS, of("/test/a/b"), Type.STRINGS));
        index.setProperty(createProperty(PROP_EXCLUDED_PATHS, of("/test/a"), Type.STRINGS));
        NodeState before = builder.getNodeState();

        // Add some content and process it through the property index hook
        builder.child("test").child("a").setProperty("foo", "abc");
        builder.child("test").child("a").child("b").setProperty("foo", "abc");
        NodeState after = builder.getNodeState();

        try {
            HOOK.processCommit(before, after, CommitInfo.EMPTY);
            assertTrue(false);
        } catch (IllegalStateException expected) {}
    }

    @Test
    public void testPathMismatch() throws Exception {
        NodeState root = INITIAL_CONTENT;

        // Add index definition
        NodeBuilder builder = root.builder();
        NodeBuilder index = createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "foo",
                true, false, ImmutableSet.of("foo"), null);
        index.setProperty(createProperty(PROP_INCLUDED_PATHS, of("/test/a"), Type.STRINGS));
        index.setProperty(createProperty(PROP_EXCLUDED_PATHS, of("/test/a/b"), Type.STRINGS));
        NodeState before = builder.getNodeState();

        // Add some content and process it through the property index hook
        builder.child("test").child("a").setProperty("foo", "abc");
        builder.child("test").child("a").child("b").setProperty("foo", "abc");
        NodeState after = builder.getNodeState();

        NodeState indexed = HOOK.processCommit(before, after, CommitInfo.EMPTY);

        FilterImpl f = createFilter(indexed, NT_BASE);
        f.restrictPath("/test2", Filter.PathRestriction.ALL_CHILDREN);
        PropertyIndexPlan plan = new PropertyIndexPlan("plan", root, index.getNodeState(), f);
        assertTrue(Double.POSITIVE_INFINITY == plan.getCost());
    }

    @Test
    public void singleMount() throws Exception {
        NodeState root = INITIAL_CONTENT;

        // Add index definition
        NodeBuilder builder = root.builder();
        NodeBuilder index = createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "foo",
                true, false, ImmutableSet.of("foo"), null);
        index.setProperty("entryCount", -1);
        NodeState before = builder.getNodeState();

        // Add some content and process it through the property index hook
        builder.child("a").setProperty("foo", "abc");
        builder.child("b").child("x").setProperty("foo", "abc");
        builder.child("a").child("x").setProperty("foo", "abc");
        builder.child("m").child("n").setProperty("foo", "abc");
        builder.child("m").child("n").child("o").setProperty("foo", "abc");
        builder.child("m").setProperty("foo", "abc");

        NodeState after = builder.getNodeState();

        MountInfoProvider mip = Mounts.newBuilder()
                .mount("foo", "/a", "/m/n")
                .build();

        Mount fooMount = mip.getMountByName("foo");
        Mount defMount = mip.getDefaultMount();

        EditorHook hook = new EditorHook(
                new IndexUpdateProvider(new PropertyIndexEditorProvider().with(mip)));

        NodeState indexed = hook.processCommit(before, after, CommitInfo.EMPTY);

        FilterImpl f = createFilter(indexed, NT_BASE);

        // Query the index
        PropertyIndexLookup lookup = new PropertyIndexLookup(indexed,mip);
        assertEquals(ImmutableSet.of("a", "b/x", "a/x", "m", "m/n", "m/n/o"), find(lookup, "foo", "abc", f));
        assertEquals(ImmutableSet.of(), find(lookup, "foo", "ghi", f));

        assertTrue(getNode(indexed, "/oak:index/foo/:index").exists());

        //Separate node for mount
        assertTrue(getNode(indexed, "/oak:index/foo/"+ getNodeForMount(fooMount)).exists());

        //Index entries for paths in foo mount should go to :oak:foo-index
        assertTrue(getNode(indexed, pathInIndex(fooMount, "/oak:index/foo", "/a", "abc")).exists());
        assertTrue(getNode(indexed, pathInIndex(fooMount, "/oak:index/foo", "/a/x", "abc")).exists());
        assertTrue(getNode(indexed, pathInIndex(fooMount, "/oak:index/foo", "/m/n", "abc")).exists());
        assertTrue(getNode(indexed, pathInIndex(fooMount, "/oak:index/foo", "/m/n/o", "abc")).exists());
        assertFalse(getNode(indexed, pathInIndex(defMount, "/oak:index/foo", "/a", "abc")).exists());
        assertFalse(getNode(indexed, pathInIndex(defMount, "/oak:index/foo", "/a/x", "abc")).exists());
        assertFalse(getNode(indexed, pathInIndex(defMount, "/oak:index/foo", "/m/n", "abc")).exists());
        assertFalse(getNode(indexed, pathInIndex(defMount, "/oak:index/foo", "/m/n/o", "abc")).exists());

        //All other index entries should go to :index
        assertTrue(getNode(indexed, pathInIndex(defMount, "/oak:index/foo", "/b", "abc")).exists());
        assertTrue(getNode(indexed, pathInIndex(defMount, "/oak:index/foo", "/b/x", "abc")).exists());
        assertTrue(getNode(indexed, pathInIndex(defMount, "/oak:index/foo", "/m", "abc")).exists());
        assertFalse(getNode(indexed, pathInIndex(fooMount, "/oak:index/foo", "/b", "abc")).exists());
        assertFalse(getNode(indexed, pathInIndex(fooMount, "/oak:index/foo", "/b/x", "abc")).exists());

        //System.out.println(NodeStateUtils.toString(getNode(indexed, "/oak:index/foo")));

    }

    @Test
    public void mountWithCommitInWritableMount() throws Exception{
        NodeState root = INITIAL_CONTENT;

        // Add index definition
        NodeBuilder builder = root.builder();
        NodeBuilder index = createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "foo",
                true, false, ImmutableSet.of("foo"), null);
        index.setProperty("entryCount", -1);
        NodeState before = builder.getNodeState();

        // Add some content and process it through the property index hook
        builder.child("content").setProperty("foo", "abc");
        NodeState after = builder.getNodeState();

        MountInfoProvider mip = Mounts.newBuilder()
                .readOnlyMount("foo",  "/readOnly")
                .build();

        CompositeHook hook = new CompositeHook(
                new EditorHook(new IndexUpdateProvider(new PropertyIndexEditorProvider().with(mip))),
                new EditorHook(new ValidatorProvider(){
                    protected Validator getRootValidator(NodeState before, NodeState after, CommitInfo info) {
                        return new PrivateStoreValidator("/", mip);
                    }
                })
        );

        NodeState indexed = hook.processCommit(before, after, CommitInfo.EMPTY);

        Mount defMount = mip.getDefaultMount();
        assertTrue(getNode(indexed, pathInIndex(defMount, "/oak:index/foo", "/content", "abc")).exists());
    }

    @Test(expected = CommitFailedException.class)
    public void mountAndUniqueIndexes() throws Exception {
        NodeState root = INITIAL_CONTENT;

        // Add index definition
        NodeBuilder builder = root.builder();
        NodeBuilder index = createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "foo",
                true, true, ImmutableSet.of("foo"), null);
        index.setProperty("entryCount", -1);
        NodeState before = builder.getNodeState();

        MountInfoProvider mip = Mounts.newBuilder()
                .mount("foo", "/a")
                .build();

        builder.child("a").setProperty("foo", "abc");
        builder.child("b").setProperty("foo", Arrays.asList("abc", "def"),
                Type.STRINGS);
        NodeState after = builder.getNodeState();

        EditorHook hook = new EditorHook(
                new IndexUpdateProvider(new PropertyIndexEditorProvider().with(mip)));
        // should throw
        hook.processCommit(before, after, CommitInfo.EMPTY);
    }

    private static String pathInIndex(Mount mount,
                                      String indexPath, String indexedPath, String indexedValue){
        return indexPath + "/" + getNodeForMount(mount) + "/" + indexedValue + indexedPath;
    }

    private static String getNodeForMount(Mount mount) {
        return Multiplexers.getNodeForMount(mount, INDEX_CONTENT_NODE_NAME);
    }

    private int getResultSize(NodeState indexed, String name, String value){
        FilterImpl f = createFilter(indexed, NT_BASE);

        // Query the index
        PropertyIndexLookup lookup = new PropertyIndexLookup(indexed);
        Iterable<String> result = lookup.query(f, name, PropertyValues.newString(value));
        return Iterables.size(result);
    }

    private NodeState createTestData(int entryCount) throws CommitFailedException {
        NodeState root = INITIAL_CONTENT;

        // Add index definition
        NodeBuilder builder = root.builder();
        NodeBuilder index = createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "foo",
                true, false, ImmutableSet.of("foo"), null);
        // disable the estimation
        index.setProperty("entryCount", -1);
        NodeState before = builder.getNodeState();

        // Add some content and process it through the property index hook
        int depth = ContentMirrorStoreStrategy.TRAVERSING_WARN / entryCount + 10;
        for (int i = 0; i < entryCount; i++) {
            NodeBuilder parentNode = builder.child("n" + i);
            for (int j = 0; j < depth ; j++) {
                parentNode = parentNode.child("c" + j);
            }
            parentNode.setProperty("foo", "bar");
        }
        NodeState after = builder.getNodeState();

        return HOOK.processCommit(before, after, CommitInfo.EMPTY);
    }

    private ListAppender createAndRegisterAppender() {
        TraversingWarningFilter filter = new TraversingWarningFilter();
        filter.start();
        ListAppender appender = new ListAppender<ILoggingEvent>();
        appender.setContext(getContext());
        appender.setName("TestLogCollector");
        appender.addFilter(filter);
        appender.start();
        rootLogger().addAppender(appender);
        return appender;
    }

    private void deregisterAppender(Appender<ILoggingEvent> appender){
        rootLogger().detachAppender(appender);
    }

    private static LoggerContext getContext(){
        return (LoggerContext) LoggerFactory.getILoggerFactory();
    }

    private static ch.qos.logback.classic.Logger rootLogger() {
        return getContext().getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
    }

    private static class TraversingWarningFilter extends ch.qos.logback.core.filter.Filter<ILoggingEvent> {

        @Override
        public FilterReply decide(ILoggingEvent event) {
            if (event.getLevel().isGreaterOrEqual(Level.WARN)
                    && event.getMessage().contains("Traversed")) {
                return FilterReply.ACCEPT;
            } else {
                return FilterReply.DENY;
            }
        }
    }

    private class PrivateStoreValidator extends DefaultValidator {
        private final String path;
        private final MountInfoProvider mountInfoProvider;

        public PrivateStoreValidator(String path, MountInfoProvider mountInfoProvider) {
            this.path = path;
            this.mountInfoProvider = mountInfoProvider;
        }

        public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
            return checkPrivateStoreCommit(getCommitPath(name));
        }

        public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
            return checkPrivateStoreCommit(getCommitPath(name));
        }

        public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
            return checkPrivateStoreCommit(getCommitPath(name));
        }

        private Validator checkPrivateStoreCommit(String commitPath) throws CommitFailedException {
            Mount mountInfo = mountInfoProvider.getMountByPath(commitPath);
            if (mountInfo.isReadOnly()) {
                    throw new CommitFailedException(CommitFailedException.UNSUPPORTED, 0,
                            "Unsupported commit to a read-only store "+ commitPath);
            }

            return new PrivateStoreValidator(commitPath, mountInfoProvider);
        }

        private String getCommitPath(String changeNodeName) {
            return PathUtils.concat(path, changeNodeName);
        }
    }
}
