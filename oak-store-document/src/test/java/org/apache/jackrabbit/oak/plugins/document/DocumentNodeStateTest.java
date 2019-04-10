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

import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.TestUtils.asDocumentState;
import static org.junit.Assert.assertEquals;

public class DocumentNodeStateTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @Test
    public void getMemory() {
        DocumentNodeStore store = builderProvider.newBuilder().getNodeStore();
        RevisionVector rv = new RevisionVector(Revision.newRevision(1));
        DocumentNodeState state = new DocumentNodeState(store, Path.fromString("/foo"), rv);
        assertEquals(198, state.getMemory());
    }

    @Test
    public void propertyCount() throws Exception{
        DocumentNodeStore store = builderProvider.newBuilder().getNodeStore();
        NodeBuilder builder = store.getRoot().builder();
        builder.child("a").setProperty("x", 1);
        builder.child("a").setProperty("y", 1);
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeState ns = store.getRoot().getChildNode("a");
        assertEquals(2, ns.getPropertyCount());
    }

    @Test
    public void asBranchRootState() {
        DocumentNodeStore store = builderProvider.newBuilder().getNodeStore();
        DocumentNodeStoreBranch branch = store.createBranch(store.getRoot());
        NodeBuilder builder = branch.getBase().builder();
        builder.child("a");
        branch.setRoot(builder.getNodeState());
        branch.persist();
        asDocumentState(branch.getHead()).asBranchRootState(branch);
    }

    @Test(expected = IllegalStateException.class)
    public void asBranchRootStateNonBranch() {
        DocumentNodeStore store = builderProvider.newBuilder().getNodeStore();
        DocumentNodeStoreBranch branch = store.createBranch(store.getRoot());
        store.getRoot().asBranchRootState(branch);
    }

    @Test(expected = IllegalStateException.class)
    public void asBranchRootStateNonRootState() throws Exception {
        DocumentNodeStore store = builderProvider.newBuilder().getNodeStore();
        NodeBuilder builder = store.getRoot().builder();
        builder.child("a");
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        DocumentNodeStoreBranch branch = store.createBranch(store.getRoot());
        asDocumentState(store.getRoot().getChildNode("a")).asBranchRootState(branch);
    }
}
