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
package org.apache.jackrabbit.oak.plugins.index.p2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Set;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexHook;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
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
public class Property2IndexTest {

    private static final int MANY = 100;

    @Test
    public void testPropertyLookup() throws Exception {
        NodeState root = MemoryNodeState.EMPTY_NODE;

        // Add index definition
        NodeBuilder builder = root.builder();
        builder.child("oak:index").child("foo")
                .setProperty("jcr:primaryType", "oak:queryIndexDefinition", Type.NAME)
                .setProperty("type", "p2")
                .setProperty("propertyNames", "foo");
        NodeState before = builder.getNodeState();

        // Add some content and process it through the property index hook
        builder = before.builder();
        builder.child("a").setProperty("foo", "abc");
        builder.child("b").setProperty("foo", Arrays.asList("abc", "def"), Type.STRINGS);
        // plus lots of dummy content to highlight the benefit of indexing
        for (int i = 0; i < MANY; i++) {
            builder.child("n" + i).setProperty("foo", "xyz");
        }
        NodeState after = builder.getNodeState();

        // Add an index
        IndexHook p = new Property2IndexDiff(builder);
        after.compareAgainstBaseState(before, p);
        p.apply();
        p.close();

        // Query the index
        Property2IndexLookup lookup = new Property2IndexLookup(builder.getNodeState());
        assertEquals(ImmutableSet.of("a", "b"), find(lookup, "foo", "abc"));
        assertEquals(ImmutableSet.of("b"), find(lookup, "foo", "def"));
        assertEquals(ImmutableSet.of(), find(lookup, "foo", "ghi"));
        assertEquals(MANY, find(lookup, "foo", "xyz").size());
        assertEquals(MANY + 2, find(lookup, "foo", null).size());

        double cost;
        cost = lookup.getCost(null, "foo", PropertyValues.newString("xyz"));
        assertTrue("cost: " + cost, cost >= MANY);
        cost = lookup.getCost(null, "foo", null);
        assertTrue("cost: " + cost, cost >= MANY);
    }

    private static Set<String> find(Property2IndexLookup lookup, String name, String value, Filter filter) {
        return Sets.newHashSet(lookup.query(filter, name, value == null ? null : PropertyValues.newString(value)));
    }

    private static Set<String> find(Property2IndexLookup lookup, String name, String value) {
        return find(lookup, name, value, null);
    }

    @Test
    public void testCustomConfigPropertyLookup() throws Exception {
        NodeState root = MemoryNodeState.EMPTY_NODE;

        // Add index definition
        NodeBuilder builder = root.builder();
        builder.child("oak:index").child("fooIndex")
                .setProperty("jcr:primaryType", "oak:queryIndexDefinition", Type.NAME)
                .setProperty("type", "p2")
                .setProperty("propertyNames", Arrays.asList("foo", "extrafoo"), Type.STRINGS);
        NodeState before = builder.getNodeState();

        // Add some content and process it through the property index hook
        builder = before.builder();
        builder.child("a").setProperty("foo", "abc").setProperty("extrafoo", "pqr");
        builder.child("b").setProperty("foo", Arrays.asList("abc", "def"), Type.STRINGS);
        // plus lots of dummy content to highlight the benefit of indexing
        for (int i = 0; i < MANY; i++) {
            builder.child("n" + i).setProperty("foo", "xyz");
        }
        NodeState after = builder.getNodeState();

        // Add an index
        IndexHook p = new Property2IndexDiff(builder);
        after.compareAgainstBaseState(before, p);
        p.apply();
        p.close();

        // Query the index
        Property2IndexLookup lookup = new Property2IndexLookup(builder.getNodeState());
        assertEquals(ImmutableSet.of("a", "b"), find(lookup, "foo", "abc"));
        assertEquals(ImmutableSet.of("b"), find(lookup, "foo", "def"));
        assertEquals(ImmutableSet.of(), find(lookup, "foo", "ghi"));
        assertEquals(MANY, find(lookup, "foo", "xyz").size());
        assertEquals(ImmutableSet.of("a"), find(lookup, "extrafoo", "pqr"));
        
        try {
            assertEquals(ImmutableSet.of(), find(lookup, "pqr", "foo"));
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
        NodeState root = MemoryNodeState.EMPTY_NODE;

        // Add index definitions
        NodeBuilder builder = root.builder();
        NodeBuilder index = builder.child("oak:index");
        index.child("fooIndex")
                .setProperty("jcr:primaryType", "oak:queryIndexDefinition",
                        Type.NAME)
                .setProperty("type", "p2")
                .setProperty("propertyNames", Arrays.asList("foo", "extrafoo"),
                        Type.STRINGS)
                .setProperty("declaringNodeTypes",
                        Arrays.asList("nt:unstructured"), Type.STRINGS);
        index.child("fooIndexFile")
                .setProperty("jcr:primaryType", "oak:queryIndexDefinition",
                        Type.NAME)
                .setProperty("type", "p2")
                .setProperty("propertyNames", Arrays.asList("foo"),
                        Type.STRINGS)
                .setProperty("declaringNodeTypes", Arrays.asList("nt:file"),
                        Type.STRINGS);
        NodeState before = builder.getNodeState();

        // Add some content and process it through the property index hook
        builder = before.builder();
        builder.child("a").setProperty("jcr:primaryType", "nt:unstructured")
                .setProperty("foo", "abc");
        builder.child("b").setProperty("jcr:primaryType", "nt:unstructured")
                .setProperty("foo", Arrays.asList("abc", "def"), Type.STRINGS);
        NodeState after = builder.getNodeState();

        // Add an index
        IndexHook p = new Property2IndexDiff(builder);
        after.compareAgainstBaseState(before, p);
        p.apply();
        p.close();

        NodeState indexedState = builder.getNodeState();

        FilterImpl f = new FilterImpl(null, null);
        f.setNodeType("nt:unstructured");

        // Query the index
        Property2IndexLookup lookup = new Property2IndexLookup(indexedState);
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

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-666">OAK-666:
     *      Property2Index: node type is used when indexing, but ignored when
     *      querying</a>
     */
    @Test
    public void testCustomConfigNodeTypeFallback() throws Exception {
        NodeState root = MemoryNodeState.EMPTY_NODE;

        // Add index definitions
        NodeBuilder builder = root.builder();
        NodeBuilder index = builder.child("oak:index");
        index.child("fooIndex")
                .setProperty("jcr:primaryType", "oak:queryIndexDefinition",
                        Type.NAME)
                .setProperty("type", "p2")
                .setProperty("propertyNames", Arrays.asList("foo", "extrafoo"),
                        Type.STRINGS);
        index.child("fooIndexFile")
                .setProperty("jcr:primaryType", "oak:queryIndexDefinition",
                        Type.NAME)
                .setProperty("type", "p2")
                .setProperty("propertyNames", Arrays.asList("foo"),
                        Type.STRINGS)
                .setProperty("declaringNodeTypes", Arrays.asList("nt:file"),
                        Type.STRINGS);
        NodeState before = builder.getNodeState();

        // Add some content and process it through the property index hook
        builder = before.builder();
        builder.child("a").setProperty("jcr:primaryType", "nt:unstructured")
                .setProperty("foo", "abc");
        builder.child("b").setProperty("jcr:primaryType", "nt:unstructured")
                .setProperty("foo", Arrays.asList("abc", "def"), Type.STRINGS);
        NodeState after = builder.getNodeState();

        // Add an index
        IndexHook p = new Property2IndexDiff(builder);
        after.compareAgainstBaseState(before, p);
        p.apply();
        p.close();

        NodeState indexedState = builder.getNodeState();

        FilterImpl f = new FilterImpl(null, null);
        f.setNodeType("nt:unstructured");

        // Query the index
        Property2IndexLookup lookup = new Property2IndexLookup(indexedState);
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

        NodeState root = MemoryNodeState.EMPTY_NODE;

        // Add index definition
        NodeBuilder builder = root.builder();
        builder.child("oak:index")
                .child("fooIndex")
                .setProperty("jcr:primaryType", "oak:queryIndexDefinition",
                        Type.NAME)
                .setProperty("type", "p2")
                .setProperty("unique", "true")
                .setProperty("propertyNames", Arrays.asList("foo"),
                        Type.STRINGS);

        NodeState before = builder.getNodeState();
        builder = before.builder();
        builder.child("a").setProperty("foo", "abc");
        builder.child("b").setProperty("foo", Arrays.asList("abc", "def"),
                Type.STRINGS);
        NodeState after = builder.getNodeState();

        IndexHook p = new Property2IndexDiff(builder);
        after.compareAgainstBaseState(before, p);
        try {
            p.apply();
            fail("Unique constraint should be respected");
        } catch (CommitFailedException e) {
            // expected
        } finally {
            p.close();
        }
    }

    @Test
    public void testUniqueByTypeOK() throws Exception {

        NodeState root = MemoryNodeState.EMPTY_NODE;

        // Add index definition
        NodeBuilder builder = root.builder();
        builder.child("oak:index").child("fooIndex")
                .setProperty("jcr:primaryType", "oak:queryIndexDefinition", Type.NAME)
                .setProperty("type", "p2")
                .setProperty("unique", "true")
                .setProperty("propertyNames", Arrays.asList("foo"), Type.STRINGS)
                .setProperty(Property2IndexDiff.declaringNodeTypes, Arrays.asList("typeFoo"), Type.STRINGS);
        NodeState before = builder.getNodeState();
        builder = before.builder();
        builder.child("a")
                .setProperty("jcr:primaryType", "typeFoo", Type.NAME)
                .setProperty("foo", "abc");
        builder.child("b")
                .setProperty("jcr:primaryType", "typeBar", Type.NAME)
                .setProperty("foo", "abc");
        NodeState after = builder.getNodeState();

        IndexHook p = new Property2IndexDiff(builder);
        after.compareAgainstBaseState(before, p);
        p.apply();
        p.close();
    }

    @Test
    public void testUniqueByTypeKO() throws Exception {

        NodeState root = MemoryNodeState.EMPTY_NODE;

        // Add index definition
        NodeBuilder builder = root.builder();
        builder.child("oak:index").child("fooIndex")
                .setProperty("jcr:primaryType", "oak:queryIndexDefinition", Type.NAME)
                .setProperty("type", "p2")
                .setProperty("unique", "true")
                .setProperty("propertyNames", Arrays.asList("foo"), Type.STRINGS)
                .setProperty(Property2IndexDiff.declaringNodeTypes, Arrays.asList("typeFoo"), Type.STRINGS);
        NodeState before = builder.getNodeState();
        builder = before.builder();
        builder.child("a")
                .setProperty("jcr:primaryType", "typeFoo", Type.NAME)
                .setProperty("foo", "abc");
        builder.child("b")
                .setProperty("jcr:primaryType", "typeFoo", Type.NAME)
                .setProperty("foo", "abc");
        NodeState after = builder.getNodeState();

        IndexHook p = new Property2IndexDiff(builder);
        after.compareAgainstBaseState(before, p);
        try {
            p.apply();
            fail("Unique constraint should be respected");
        } catch (CommitFailedException e) {
            // expected
        } finally {
            p.close();
        }
    }

    @Test
    public void testUniqueByTypeDelete() throws Exception {

        NodeState root = MemoryNodeState.EMPTY_NODE;

        // Add index definition
        NodeBuilder builder = root.builder();
        builder.child("oak:index").child("fooIndex")
                .setProperty("jcr:primaryType", "oak:queryIndexDefinition", Type.NAME)
                .setProperty("type", "p2")
                .setProperty("unique", "true")
                .setProperty("propertyNames", Arrays.asList("foo"), Type.STRINGS)
                .setProperty(Property2IndexDiff.declaringNodeTypes, Arrays.asList("typeFoo"), Type.STRINGS);
        builder.child("a")
                .setProperty("jcr:primaryType", "typeFoo", Type.NAME)
                .setProperty("foo", "abc");
        builder.child("b")
                .setProperty("jcr:primaryType", "typeBar", Type.NAME)
                .setProperty("foo", "abc");
        NodeState before = builder.getNodeState();
        builder = before.builder();
        builder.removeNode("b");
        NodeState after = builder.getNodeState();

        IndexHook p = new Property2IndexDiff(builder);
        after.compareAgainstBaseState(before, p);
        p.apply();
        p.close();
    }

}
