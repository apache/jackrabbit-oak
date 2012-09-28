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

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.core.MicroKernelImpl;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStoreBranch;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

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

        NodeBuilder rootBuilder = branch.getRoot().getBuilder();
        NodeBuilder testBuilder = rootBuilder.getChildBuilder("test");
        NodeBuilder newNodeBuilder = testBuilder.getChildBuilder("newNode");

        testBuilder.removeNode("x");

        CoreValue fortyTwo = store.getValueFactory().createValue(42);
        newNodeBuilder.setProperty("n", fortyTwo);

        // Assert changes are present in the builder
        NodeState testState = rootBuilder.getNodeState().getChildNode("test");
        assertNotNull(testState.getChildNode("newNode"));
        assertNull(testState.getChildNode("x"));
        assertEquals(fortyTwo, testState.getChildNode("newNode").getProperty("n").getValue());

        // Assert changes are not yet present in the branch
        testState = branch.getRoot().getChildNode("test");
        assertNull(testState.getChildNode("newNode"));
        assertNotNull(testState.getChildNode("x"));

        branch.setRoot(rootBuilder.getNodeState());

        // Assert changes are present in the branch
        testState = branch.getRoot().getChildNode("test");
        assertNotNull(testState.getChildNode("newNode"));
        assertNull(testState.getChildNode("x"));
        assertEquals(fortyTwo, testState.getChildNode("newNode").getProperty("n").getValue());

        // Assert changes are not yet present in the trunk
        testState = store.getRoot().getChildNode("test");
        assertNull(testState.getChildNode("newNode"));
        assertNotNull(testState.getChildNode("x"));

        branch.merge();

        // Assert changes are present in the trunk
        testState = store.getRoot().getChildNode("test");
        assertNotNull(testState.getChildNode("newNode"));
        assertNull(testState.getChildNode("x"));
        assertEquals(fortyTwo, testState.getChildNode("newNode").getProperty("n").getValue());
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
        NodeBuilder rootBuilder= root.getBuilder();
        NodeBuilder testBuilder = rootBuilder.getChildBuilder("test");
        NodeBuilder newNodeBuilder = testBuilder.getChildBuilder("newNode");

        CoreValue fortyTwo = store.getValueFactory().createValue(42);
        newNodeBuilder.setProperty("n", fortyTwo);

        testBuilder.removeNode("a");

        NodeState newRoot = rootBuilder.getNodeState();

        NodeStoreBranch branch = store.branch();
        branch.setRoot(newRoot);
        branch.merge();
        store.getRoot(); // triggers the observer

        NodeState before = states[0];
        NodeState after = states[1];
        assertNotNull(before);
        assertNotNull(after);

        assertNull(before.getChildNode("test").getChildNode("newNode"));
        assertNotNull(after.getChildNode("test").getChildNode("newNode"));
        assertNull(after.getChildNode("test").getChildNode("a"));
        assertEquals(fortyTwo, after.getChildNode("test").getChildNode("newNode").getProperty("n").getValue());
        assertEquals(newRoot, after);
    }

    @Test
    public void beforeCommitHook() throws CommitFailedException {
        store.setHook(new CommitHook() {
            @Override
            public NodeState processCommit(NodeState before, NodeState after) {
                NodeBuilder rootBuilder = after.getBuilder();
                NodeBuilder testBuilder = rootBuilder.getChildBuilder("test");
                testBuilder.getChildBuilder("fromHook");
                return rootBuilder.getNodeState();
            }
        });

        NodeState root = store.getRoot();
        NodeBuilder rootBuilder = root.getBuilder();
        NodeBuilder testBuilder = rootBuilder.getChildBuilder("test");
        NodeBuilder newNodeBuilder = testBuilder.getChildBuilder("newNode");

        final CoreValue fortyTwo = store.getValueFactory().createValue(42);
        newNodeBuilder.setProperty("n", fortyTwo);

        testBuilder.removeNode("a");

        NodeState newRoot = rootBuilder.getNodeState();

        NodeStoreBranch branch = store.branch();
        branch.setRoot(newRoot);
        branch.merge();

        NodeState test = store.getRoot().getChildNode("test");
        assertNotNull(test.getChildNode("newNode"));
        assertNotNull(test.getChildNode("fromHook"));
        assertNull(test.getChildNode("a"));
        assertEquals(fortyTwo, test.getChildNode("newNode").getProperty("n").getValue());
        assertEquals(test, store.getRoot().getChildNode("test"));
    }

}
