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
        cost = lookup.getCost("foo", PropertyValues.newString("xyz"));
        assertTrue("cost: " + cost, cost >= MANY);
        cost = lookup.getCost("foo", null);
        assertTrue("cost: " + cost, cost >= MANY);
    }

    private static Set<String> find(Property2IndexLookup lookup, String name, String value) {
        return Sets.newHashSet(lookup.query(null, name, value == null ? null : PropertyValues.newString(value)));
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
