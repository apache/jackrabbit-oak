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

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.JcrConstants.NT_BASE;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Set;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditor;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexLookup;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.query.ast.SelectorImpl;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
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

    @Test
    public void testPropertyLookup() throws Exception {
        NodeState root = new InitialContent().initialize(EMPTY_NODE);

        // Add index definition
        NodeBuilder builder = root.builder();
        builder.child("oak:index")
                .child("foo")
                .setProperty("jcr:primaryType", "oak:queryIndexDefinition",
                        Type.NAME).setProperty("type", "p2")
                .setProperty("propertyNames", Arrays.asList("foo"), Type.NAMES);
        NodeState before = builder.getNodeState();

        // Add some content and process it through the property index hook
        builder = before.builder();
        builder.child("a").setProperty("foo", "abc");
        builder.child("b").setProperty("foo", Arrays.asList("abc", "def"),
                Type.STRINGS);
        // plus lots of dummy content to highlight the benefit of indexing
        for (int i = 0; i < MANY; i++) {
            builder.child("n" + i).setProperty("foo", "xyz");
        }
        NodeState after = builder.getNodeState();

        EditorDiff.process(new PropertyIndexEditor(builder), before, after);
        NodeState indexed = builder.getNodeState();

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
        NodeState root = new InitialContent().initialize(EMPTY_NODE);

        // Add index definition
        NodeBuilder builder = root.builder();
        builder.child("oak:index")
                .child("fooIndex")
                .setProperty("jcr:primaryType", "oak:queryIndexDefinition",
                        Type.NAME)
                .setProperty("type", "p2")
                .setProperty("propertyNames", Arrays.asList("foo", "extrafoo"),
                        Type.NAMES);
        NodeState before = builder.getNodeState();

        // Add some content and process it through the property index hook
        builder = before.builder();
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
        EditorDiff.process(new PropertyIndexEditor(builder), before, after);
        NodeState indexed = builder.getNodeState();

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
        NodeState root = new InitialContent().initialize(EMPTY_NODE);

        // Add index definitions
        NodeBuilder builder = root.builder();
        NodeBuilder index = builder.child("oak:index");
        index.child("fooIndex")
                .setProperty("jcr:primaryType", "oak:queryIndexDefinition",
                        Type.NAME)
                .setProperty("type", "p2")
                .setProperty("propertyNames", Arrays.asList("foo", "extrafoo"),
                        Type.NAMES)
                .setProperty("declaringNodeTypes",
                        Arrays.asList("nt:unstructured"), Type.NAMES);
        index.child("fooIndexFile")
                .setProperty("jcr:primaryType", "oak:queryIndexDefinition",
                        Type.NAME)
                .setProperty("type", "p2")
                .setProperty("propertyNames", Arrays.asList("foo"),
                        Type.NAMES)
                .setProperty("declaringNodeTypes", Arrays.asList("nt:file"),
                        Type.NAMES);
        NodeState before = builder.getNodeState();

        // Add some content and process it through the property index hook
        builder = before.builder();
        builder.child("a")
                .setProperty("jcr:primaryType", "nt:unstructured", Type.NAME)
                .setProperty("foo", "abc");
        builder.child("b")
                .setProperty("jcr:primaryType", "nt:unstructured", Type.NAME)
                .setProperty("foo", Arrays.asList("abc", "def"), Type.STRINGS);
        NodeState after = builder.getNodeState();

        EditorDiff.process(new PropertyIndexEditor(builder), before, after);
        NodeState indexed = builder.getNodeState();

        FilterImpl f = createFilter(indexed, "nt:unstructured");

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
        NodeBuilder index = builder.child("oak:index");
        index.child("fooIndex")
                .setProperty("jcr:primaryType", "oak:queryIndexDefinition",
                        Type.NAME)
                .setProperty("type", "p2")
                .setProperty("propertyNames", Arrays.asList("foo", "extrafoo"),
                        Type.NAMES);
        index.child("fooIndexFile")
                .setProperty("jcr:primaryType", "oak:queryIndexDefinition",
                        Type.NAME)
                .setProperty("type", "p2")
                .setProperty("propertyNames", Arrays.asList("foo"),
                        Type.NAMES)
                .setProperty("declaringNodeTypes", Arrays.asList("nt:file"),
                        Type.NAMES);
        NodeState before = builder.getNodeState();

        // Add some content and process it through the property index hook
        builder = before.builder();
        builder.child("a")
            .setProperty("jcr:primaryType", "nt:unstructured", Type.NAME)
            .setProperty("foo", "abc");
        builder.child("b")
            .setProperty("jcr:primaryType", "nt:unstructured", Type.NAME)
            .setProperty("foo", Arrays.asList("abc", "def"), Type.STRINGS);
        NodeState after = builder.getNodeState();

        // Add an index
        EditorDiff.process(new PropertyIndexEditor(builder), before, after);
        NodeState indexed = builder.getNodeState();

        FilterImpl f = createFilter(after, "nt:unstructured");

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
    public void testUnique() throws Exception {
        NodeState root = EMPTY_NODE;

        // Add index definition
        NodeBuilder builder = root.builder();
        builder.child("oak:index")
                .child("fooIndex")
                .setProperty("jcr:primaryType", "oak:queryIndexDefinition",
                        Type.NAME)
                .setProperty("type", "p2")
                .setProperty("unique", "true")
                .setProperty("propertyNames", Arrays.asList("foo"),
                        Type.NAMES);

        NodeState before = builder.getNodeState();
        builder = before.builder();
        builder.child("a").setProperty("foo", "abc");
        builder.child("b").setProperty("foo", Arrays.asList("abc", "def"),
                Type.STRINGS);
        NodeState after = builder.getNodeState();

        CommitFailedException expected =
                EditorDiff.process(new PropertyIndexEditor(builder), before, after);
        assertNotNull("Unique constraint should be respected", expected);
    }

    @Test
    public void testUniqueByTypeOK() throws Exception {
        NodeState root = EMPTY_NODE;

        // Add index definition
        NodeBuilder builder = root.builder();
        builder.child("oak:index")
                .child("fooIndex")
                .setProperty("jcr:primaryType", "oak:queryIndexDefinition",
                        Type.NAME)
                .setProperty("type", "p2")
                .setProperty("unique", "true")
                .setProperty("propertyNames", Arrays.asList("foo"),
                        Type.NAMES)
                .setProperty(PropertyIndexEditor.declaringNodeTypes,
                        Arrays.asList("typeFoo"), Type.NAMES);
        NodeState before = builder.getNodeState();
        builder = before.builder();
        builder.child("a").setProperty("jcr:primaryType", "typeFoo", Type.NAME)
                .setProperty("foo", "abc");
        builder.child("b").setProperty("jcr:primaryType", "typeBar", Type.NAME)
                .setProperty("foo", "abc");
        NodeState after = builder.getNodeState();

        CommitFailedException unexpected = EditorDiff.process(
                new PropertyIndexEditor(builder), before, after);
        assertNull(unexpected);
    }

    @Test
    public void testUniqueByTypeKO() throws Exception {
        NodeState root = EMPTY_NODE;

        // Add index definition
        NodeBuilder builder = root.builder();
        builder.child("oak:index")
                .child("fooIndex")
                .setProperty("jcr:primaryType", "oak:queryIndexDefinition",
                        Type.NAME)
                .setProperty("type", "p2")
                .setProperty("unique", "true")
                .setProperty("propertyNames", Arrays.asList("foo"),
                        Type.NAMES)
                .setProperty(PropertyIndexEditor.declaringNodeTypes,
                        Arrays.asList("typeFoo"), Type.NAMES);
        NodeState before = builder.getNodeState();
        builder = before.builder();
        builder.child("a").setProperty("jcr:primaryType", "typeFoo", Type.NAME)
                .setProperty("foo", "abc");
        builder.child("b").setProperty("jcr:primaryType", "typeFoo", Type.NAME)
                .setProperty("foo", "abc");
        NodeState after = builder.getNodeState();

        CommitFailedException expected = EditorDiff.process(
                new PropertyIndexEditor(builder), before, after);
        assertNotNull("Unique constraint should be respected", expected);
    }

    @Test
    public void testUniqueByTypeDelete() throws Exception {
        NodeState root = EMPTY_NODE;

        // Add index definition
        NodeBuilder builder = root.builder();
        builder.child("oak:index")
                .child("fooIndex")
                .setProperty("jcr:primaryType", "oak:queryIndexDefinition",
                        Type.NAME)
                .setProperty("type", "p2")
                .setProperty("unique", "true")
                .setProperty("propertyNames", Arrays.asList("foo"),
                        Type.NAMES)
                .setProperty(PropertyIndexEditor.declaringNodeTypes,
                        Arrays.asList("typeFoo"), Type.NAMES);
        builder.child("a").setProperty("jcr:primaryType", "typeFoo", Type.NAME)
                .setProperty("foo", "abc");
        builder.child("b").setProperty("jcr:primaryType", "typeBar", Type.NAME)
                .setProperty("foo", "abc");
        NodeState before = builder.getNodeState();
        builder = before.builder();
        builder.removeChildNode("b");
        NodeState after = builder.getNodeState();

        CommitFailedException unexpected = EditorDiff.process(
                new PropertyIndexEditor(builder), before, after);
        assertNull(unexpected);
    }

}
