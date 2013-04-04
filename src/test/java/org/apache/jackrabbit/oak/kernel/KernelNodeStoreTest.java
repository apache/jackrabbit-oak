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
package org.apache.jackrabbit.oak.kernel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.MultiStringPropertyState;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.EmptyNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertFalse;
import static org.apache.jackrabbit.oak.api.Type.LONG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class KernelNodeStoreTest {

    private KernelNodeStore store;

    private NodeState root;

    @Before
    public void setUp() {
        MicroKernel kernel = new MicroKernelImpl();
        String jsop =
                "+\"test\":{\"a\":1,\"b\":2,\"c\":3,"
                + "\"x\":{},\"y\":{},\"z\":{}}";
        kernel .commit("/", jsop, null, "test data");
        store = new KernelNodeStore(kernel);
        root = store.getRoot();
    }

    @Test
    public void getRoot() {
        assertEquals(root, store.getRoot());
        assertEquals(root.getChildNode("test"), store.getRoot().getChildNode("test"));
        assertEquals(root.getChildNode("test").getChildNode("x"),
                store.getRoot().getChildNode("test").getChildNode("x"));
        assertEquals(root.getChildNode("test").getChildNode("any"),
                store.getRoot().getChildNode("test").getChildNode("any"));
        assertEquals(root.getChildNode("test").getProperty("a"),
                store.getRoot().getChildNode("test").getProperty("a"));
        assertEquals(root.getChildNode("test").getProperty("any"),
                store.getRoot().getChildNode("test").getProperty("any"));
    }

    @Test
    public void branch() throws CommitFailedException {
        NodeStoreBranch branch = store.branch();

        NodeBuilder rootBuilder = branch.getHead().builder();
        NodeBuilder testBuilder = rootBuilder.child("test");
        NodeBuilder newNodeBuilder = testBuilder.child("newNode");

        testBuilder.removeNode("x");

        newNodeBuilder.setProperty("n", 42);

        // Assert changes are present in the builder
        NodeState testState = rootBuilder.getNodeState().getChildNode("test");
        assertTrue(testState.getChildNode("newNode").exists());
        assertFalse(testState.getChildNode("x").exists());
        assertEquals(42, (long) testState.getChildNode("newNode").getProperty("n").getValue(LONG));

        // Assert changes are not yet present in the branch
        testState = branch.getHead().getChildNode("test");
        assertFalse(testState.getChildNode("newNode").exists());
        assertTrue(testState.getChildNode("x").exists());

        branch.setRoot(rootBuilder.getNodeState());

        // Assert changes are present in the branch
        testState = branch.getHead().getChildNode("test");
        assertTrue(testState.getChildNode("newNode").exists());
        assertFalse(testState.getChildNode("x").exists());
        assertEquals(42, (long) testState.getChildNode("newNode").getProperty("n").getValue(LONG));

        // Assert changes are not yet present in the trunk
        testState = store.getRoot().getChildNode("test");
        assertFalse(testState.getChildNode("newNode").exists());
        assertTrue(testState.getChildNode("x").exists());

        branch.merge(EmptyHook.INSTANCE);

        // Assert changes are present in the trunk
        testState = store.getRoot().getChildNode("test");
        assertTrue(testState.getChildNode("newNode").exists());
        assertFalse(testState.getChildNode("x").exists());
        assertEquals(42, (long) testState.getChildNode("newNode").getProperty("n").getValue(LONG));
    }

    @Test
    public void afterCommitHook() throws CommitFailedException {
        final NodeState[] states = new NodeState[2]; // { before, after }
        store.setObserver(new Observer() {
            @Override
            public void contentChanged(NodeState before, NodeState after) {
                states[0] = before;
                states[1] = after;
            }
        });

        NodeState root = store.getRoot();
        NodeBuilder rootBuilder= root.builder();
        NodeBuilder testBuilder = rootBuilder.child("test");
        NodeBuilder newNodeBuilder = testBuilder.child("newNode");

        newNodeBuilder.setProperty("n", 42);

        testBuilder.removeNode("a");

        NodeState newRoot = rootBuilder.getNodeState();

        NodeStoreBranch branch = store.branch();
        branch.setRoot(newRoot);
        branch.merge(EmptyHook.INSTANCE);
        store.getRoot(); // triggers the observer

        NodeState before = states[0];
        NodeState after = states[1];
        assertNotNull(before);
        assertNotNull(after);

        assertFalse(before.getChildNode("test").getChildNode("newNode").exists());
        assertTrue(after.getChildNode("test").getChildNode("newNode").exists());
        assertFalse(after.getChildNode("test").getChildNode("a").exists());
        assertEquals(42, (long) after.getChildNode("test").getChildNode("newNode").getProperty("n").getValue(LONG));
        assertEquals(newRoot, after);
    }

    @Test
    public void beforeCommitHook() throws CommitFailedException {
        NodeState root = store.getRoot();
        NodeBuilder rootBuilder = root.builder();
        NodeBuilder testBuilder = rootBuilder.child("test");
        NodeBuilder newNodeBuilder = testBuilder.child("newNode");

        newNodeBuilder.setProperty("n", 42);

        testBuilder.removeNode("a");

        NodeState newRoot = rootBuilder.getNodeState();

        NodeStoreBranch branch = store.branch();
        branch.setRoot(newRoot);
        branch.merge(new CommitHook() {
            @Override
            public NodeState processCommit(NodeState before, NodeState after) {
                NodeBuilder rootBuilder = after.builder();
                NodeBuilder testBuilder = rootBuilder.child("test");
                testBuilder.child("fromHook");
                return rootBuilder.getNodeState();
            }
        });

        NodeState test = store.getRoot().getChildNode("test");
        assertTrue(test.getChildNode("newNode").exists());
        assertTrue(test.getChildNode("fromHook").exists());
        assertFalse(test.getChildNode("a").exists());
        assertEquals(42, (long) test.getChildNode("newNode").getProperty("n").getValue(LONG));
        assertEquals(test, store.getRoot().getChildNode("test"));
    }

    @Test
    public void manyChildNodes() throws CommitFailedException {
        NodeStoreBranch branch = store.branch();
        NodeBuilder root = branch.getHead().builder();
        NodeBuilder parent = root.child("parent");
        for (int i = 0; i <= KernelNodeState.MAX_CHILD_NODE_NAMES; i++) {
            parent.child("child-" + i);
        }
        branch.setRoot(root.getNodeState());
        branch.merge(EmptyHook.INSTANCE);

        NodeState base = store.getRoot();
        branch = store.branch();
        root = branch.getHead().builder();
        parent = root.child("parent");
        parent.child("child-new");
        branch.setRoot(root.getNodeState());
        branch.merge(EmptyHook.INSTANCE);

        Diff diff = new Diff();
        store.getRoot().compareAgainstBaseState(base, diff);

        assertEquals(0, diff.removed.size());
        assertEquals(1, diff.added.size());
        assertEquals("child-new", diff.added.get(0));

        base = store.getRoot();
        branch = store.branch();
        branch.move("/parent/child-new", "/parent/child-moved");
        branch.merge(EmptyHook.INSTANCE);

        diff = new Diff();
        store.getRoot().compareAgainstBaseState(base, diff);

        assertEquals(1, diff.removed.size());
        assertEquals("child-new", diff.removed.get(0));
        assertEquals(1, diff.added.size());
        assertEquals("child-moved", diff.added.get(0));

        base = store.getRoot();
        branch = store.branch();
        root = branch.getHead().builder();
        parent = root.child("parent");
        parent.child("child-moved").setProperty("foo", "value");
        parent.child("child-moved").setProperty(
                new MultiStringPropertyState("bar", Arrays.asList("value")));
        branch.setRoot(root.getNodeState());
        branch.merge(EmptyHook.INSTANCE);

        diff = new Diff();
        store.getRoot().compareAgainstBaseState(base, diff);

        assertEquals(0, diff.removed.size());
        assertEquals(0, diff.added.size());
        assertEquals(2, diff.addedProperties.size());
        assertTrue(diff.addedProperties.contains("foo"));
        assertTrue(diff.addedProperties.contains("bar"));

        base = store.getRoot();
        branch = store.branch();
        root = branch.getHead().builder();
        parent = root.child("parent");
        parent.setProperty("foo", "value");
        parent.setProperty(new MultiStringPropertyState(
                "bar", Arrays.asList("value")));
        branch.setRoot(root.getNodeState());
        branch.merge(EmptyHook.INSTANCE);

        diff = new Diff();
        store.getRoot().compareAgainstBaseState(base, diff);

        assertEquals(0, diff.removed.size());
        assertEquals(0, diff.added.size());
        assertEquals(2, diff.addedProperties.size());
        assertTrue(diff.addedProperties.contains("foo"));
        assertTrue(diff.addedProperties.contains("bar"));

        base = store.getRoot();
        branch = store.branch();
        root = branch.getHead().builder();
        parent = root.child("parent");
        parent.removeNode("child-moved");
        branch.setRoot(root.getNodeState());
        branch.merge(EmptyHook.INSTANCE);

        diff = new Diff();
        store.getRoot().compareAgainstBaseState(base, diff);

        assertEquals(1, diff.removed.size());
        assertEquals(0, diff.added.size());
        assertEquals("child-moved", diff.removed.get(0));

    }

    private static class Diff extends EmptyNodeStateDiff {

        List<String> addedProperties = new ArrayList<String>();
        List<String> added = new ArrayList<String>();
        List<String> removed = new ArrayList<String>();

        @Override
        public void childNodeAdded(String name, NodeState after) {
            added.add(name);
        }

        @Override
        public void childNodeDeleted(String name, NodeState before) {
            removed.add(name);
        }

        @Override
        public void childNodeChanged(String name,
                                     NodeState before,
                                     NodeState after) {
            after.compareAgainstBaseState(before, this);
        }

        @Override
        public void propertyAdded(PropertyState after) {
            addedProperties.add(after.getName());
        }
    }

}
