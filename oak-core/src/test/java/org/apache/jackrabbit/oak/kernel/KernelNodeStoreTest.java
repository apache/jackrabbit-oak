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
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.core.AbstractOakTest;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.EmptyCommitHook;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class KernelNodeStoreTest extends AbstractOakTest {
    private final CommitHookDelegate commitHookDelegate = new CommitHookDelegate();

    private NodeState root;

    @Override
    protected NodeState createInitialState(MicroKernel microKernel) {
        String jsop =
                "+\"test\":{\"a\":1,\"b\":2,\"c\":3,"
                        + "\"x\":{},\"y\":{},\"z\":{}}";
        microKernel.commit("/", jsop, null, "test data");
        root = store.getRoot();
        return root;
    }

    @Override
    protected CommitHook createCommitHook() {
        return commitHookDelegate;
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
    public void setRoot() throws CommitFailedException {
        NodeState test = store.getRoot().getChildNode("test");
        NodeStateBuilder testBuilder = store.getBuilder(test);

        NodeStateBuilder newNodeBuilder = testBuilder.addNode("newNode");
        testBuilder.removeNode("a");
        CoreValue fortyTwo = store.getValueFactory().createValue(42);
        newNodeBuilder.setProperty("n", fortyTwo);
        NodeState testState = testBuilder.getNodeState();

        store.setRoot(testBuilder.getNodeState());

        assertNotNull(testState.getChildNode("newNode"));
        assertNull(testState.getChildNode("a"));
        assertEquals(fortyTwo, testState.getChildNode("newNode").getProperty("n").getValue());
        assertEquals(testState, store.getRoot().getChildNode("test"));
    }

    @Test
    public void afterCommitHook() throws CommitFailedException {
        NodeState test = store.getRoot().getChildNode("test");
        NodeStateBuilder testBuilder = store.getBuilder(test);

        NodeStateBuilder newNodeBuilder = testBuilder.addNode("newNode");
        testBuilder.removeNode("a");
        final CoreValue fortyTwo = store.getValueFactory().createValue(42);
        newNodeBuilder.setProperty("n", fortyTwo);
        final NodeState testState = testBuilder.getNodeState();

        commitWithHook(testBuilder.getNodeState(), new EmptyCommitHook() {
            @Override
            public void afterCommit(NodeStore store, NodeState before, NodeState after) {
                assertNull(before.getChildNode("newNode"));
                assertNotNull(after.getChildNode("newNode"));
                assertNull(after.getChildNode("a"));
                assertEquals(fortyTwo, after.getChildNode("newNode").getProperty("n").getValue());
                assertEquals(testState, after);
            }
        });
    }

    @Test
    public void beforeCommitHook() throws CommitFailedException {
        NodeState test = store.getRoot().getChildNode("test");
        NodeStateBuilder testBuilder = store.getBuilder(test);

        NodeStateBuilder newNodeBuilder = testBuilder.addNode("newNode");
        testBuilder.removeNode("a");
        final CoreValue fortyTwo = store.getValueFactory().createValue(42);
        newNodeBuilder.setProperty("n", fortyTwo);

        commitWithHook(testBuilder.getNodeState(), new EmptyCommitHook() {
            @Override
            public NodeState beforeCommit(NodeStore store, NodeState before, NodeState after) {
                NodeStateBuilder afterBuilder = store.getBuilder(after);
                afterBuilder.addNode("fromHook");
                return afterBuilder.getNodeState();
            }
        });

        test = store.getRoot().getChildNode("test");
        assertNotNull(test.getChildNode("newNode"));
        assertNotNull(test.getChildNode("fromHook"));
        assertNull(test.getChildNode("a"));
        assertEquals(fortyTwo, test.getChildNode("newNode").getProperty("n").getValue());
        assertEquals(test, store.getRoot().getChildNode("test"));

    }

    //------------------------------------------------------------< private >---

    private void commitWithHook(NodeState nodeState, CommitHook commitHook)
            throws CommitFailedException {

        commitHookDelegate.set(commitHook);
        try {
            store.setRoot(nodeState);
        }
        finally {
            commitHookDelegate.set(new EmptyCommitHook());
        }
    }

    private static class CommitHookDelegate implements CommitHook {
        private CommitHook delegate = new EmptyCommitHook();

        public void set(CommitHook commitHook) {
            delegate = commitHook;
        }

        @Override
        public NodeState beforeCommit(NodeStore store, NodeState before, NodeState after)
                throws CommitFailedException {

            return delegate.beforeCommit(store, before, after);
        }

        @Override
        public void afterCommit(NodeStore store, NodeState before, NodeState after) {
            delegate.afterCommit(store, before, after);
        }
    }

}
