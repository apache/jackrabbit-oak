/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for OAK-3903
 */
public class CommitRootUpdateTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @Test
    public void exceptionOnUpdate() throws Exception {
        final AtomicBoolean throwAfterUpdate = new AtomicBoolean(false);
        MemoryDocumentStore store = new MemoryDocumentStore() {
            @Override
            public <T extends Document> T findAndUpdate(Collection<T> collection,
                                                        UpdateOp update) {
                T doc = super.findAndUpdate(collection, update);
                if (isFinalCommitRootUpdate(update) &&
                        throwAfterUpdate.compareAndSet(true, false)) {
                    throw new RuntimeException("communication failure");
                }
                return doc;
            }

            private boolean isFinalCommitRootUpdate(UpdateOp update) {
                boolean finalUpdate = true;
                for (Map.Entry<Key, Operation> op : update.getChanges().entrySet()) {
                    String name = op.getKey().getName();
                    if (NodeDocument.isRevisionsEntry(name)
                            || NodeDocument.MODIFIED_IN_SECS.equals(name)) {
                        continue;
                    }
                    finalUpdate = false;
                    break;
                }
                return finalUpdate;
            }
        };

        DocumentNodeStore ns = builderProvider.newBuilder()
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();
        NodeBuilder b = ns.getRoot().builder();
        b.child("foo");
        b.child("bar");
        merge(ns, b);

        throwAfterUpdate.set(true);
        boolean success = false;
        Commit c = ns.newCommit(ns.getHeadRevision(), null);
        try {
            c.addNode(new DocumentNodeState(ns, "/foo/node", c.getBaseRevision()));
            c.addNode(new DocumentNodeState(ns, "/bar/node", c.getBaseRevision()));
            c.apply();
            success = true;
        } finally {
            if (success) {
                ns.done(c, false, CommitInfo.EMPTY);
            } else {
                ns.canceled(c);
            }
        }

        NodeState root = ns.getRoot();
        assertTrue(root.getChildNode("foo").getChildNode("node").exists());
        assertTrue(root.getChildNode("bar").getChildNode("node").exists());
        assertFalse(throwAfterUpdate.get());
    }

    @Test
    public void exceptionOnSingleUpdate() throws Exception {
        final AtomicBoolean throwAfterUpdate = new AtomicBoolean(false);
        MemoryDocumentStore store = new MemoryDocumentStore(true) {
            @Override
            public <T extends Document> T findAndUpdate(Collection<T> collection,
                                                        UpdateOp update) {
                T doc = super.findAndUpdate(collection, update);
                if (isCommitRootUpdate(update) &&
                        throwAfterUpdate.compareAndSet(true, false)) {
                    throw new DocumentStoreException("communication failure");
                }
                return doc;
            }

            private boolean isCommitRootUpdate(UpdateOp update) {
                boolean isCommitRootUpdate = false;
                for (Map.Entry<Key, Operation> op : update.getChanges().entrySet()) {
                    String name = op.getKey().getName();
                    if (NodeDocument.isRevisionsEntry(name)) {
                        isCommitRootUpdate = true;
                        break;
                    }
                }
                return isCommitRootUpdate;
            }
        };

        DocumentNodeStore ns = builderProvider.newBuilder()
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();
        NodeBuilder b = ns.getRoot().builder();
        b.child("foo");
        merge(ns, b);

        throwAfterUpdate.set(true);
        boolean success = false;
        Commit c = ns.newCommit(ns.getHeadRevision(), null);
        try {
            c.updateProperty("/foo", "p", "v");
            c.apply();
            success = true;
        } finally {
            if (success) {
                ns.done(c, false, CommitInfo.EMPTY);
            } else {
                ns.canceled(c);
            }
        }

        NodeState root = ns.getRoot();
        assertTrue(root.getChildNode("foo").hasProperty("p"));
        assertFalse(throwAfterUpdate.get());
    }

    private NodeState merge(NodeStore store, NodeBuilder builder)
            throws Exception {
        return store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }
}
