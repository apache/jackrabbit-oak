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

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.document.Branch.BranchCommit;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.DefaultNodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.asDocumentState;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.persistToBranch;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getRootDocument;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BranchTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    @Test
    public void getModifiedPathsUntil() {
        UnmergedBranches branches = new UnmergedBranches();

        RevisionVector base = new RevisionVector(Revision.newRevision(1));
        Revision c1 = Revision.newRevision(1).asBranchRevision();
        Branch b = branches.create(base, c1, null);

        BranchCommit bc1 = b.getCommit(c1);
        bc1.track(Path.fromString("/foo"));

        Revision c2 = Revision.newRevision(1).asBranchRevision();
        b.addCommit(c2);
        BranchCommit bc2 = b.getCommit(c2);
        bc2.track(Path.fromString("/bar"));

        Revision c3 = Revision.newRevision(1).asBranchRevision();
        b.rebase(c3, new RevisionVector(Revision.newRevision(1)));

        Revision c4 = Revision.newRevision(1).asBranchRevision();
        b.addCommit(c4);
        BranchCommit bc4 = b.getCommit(c4);
        bc4.track(Path.fromString("/baz"));

        Revision c5 = Revision.newRevision(1).asBranchRevision();

        try {
            b.getModifiedPathsUntil(Revision.newRevision(1));
            fail("Must fail with IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }

        assertModifiedPaths(b.getModifiedPathsUntil(c1), "/foo");
        assertModifiedPaths(b.getModifiedPathsUntil(c2), "/foo", "/bar");
        assertModifiedPaths(b.getModifiedPathsUntil(c3), "/foo", "/bar");
        assertModifiedPaths(b.getModifiedPathsUntil(c4), "/foo", "/bar", "/baz");
        assertModifiedPaths(b.getModifiedPathsUntil(c5));
    }

    @Test
    public void rootBranchCommitChildTest() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setClusterId(1)
                .setDocumentStore(store).build();

        NodeBuilder builder = ns.getRoot().builder();
        builder.child("a");
        persistToBranch(builder);

        merge(ns, builder);

        Sweep2TestHelper.testPre18UpgradeSimulations(ns, builderProvider);
    }

    @Test
    public void childBranchCommitChildTest() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setClusterId(1)
                .setDocumentStore(store).build();

        NodeBuilder builder = ns.getRoot().builder();
        builder.child("a");
        merge(ns, builder);

        builder = ns.getRoot().builder();
        builder.child("a").child("b");
        persistToBranch(builder);

        merge(ns, builder);

        Sweep2TestHelper.testPre18UpgradeSimulations(ns, builderProvider);
    }

    @Test
    public void manyBranchCommitsDepthTest() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setClusterId(1)
                .setDocumentStore(store).build();

        NodeBuilder builder = ns.getRoot().builder();
        builder.child("a");
        persistToBranch(builder);

        builder.getChildNode("a").child("b");
        persistToBranch(builder);

        builder.getChildNode("a").getChildNode("b").child("c");
        persistToBranch(builder);

        builder.getChildNode("a").getChildNode("b").getChildNode("c").child("d");
        persistToBranch(builder);

        builder.getChildNode("a").getChildNode("b").getChildNode("c").getChildNode("d").child("e");
        persistToBranch(builder);

        merge(ns, builder);

        Sweep2TestHelper.testPre18UpgradeSimulations(ns, builderProvider);
    }

    @Test
    public void manyBranchCommitsWidthTest() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setClusterId(1)
                .setDocumentStore(store).build();

        NodeBuilder builder = ns.getRoot().builder();
        builder.child("a");
        persistToBranch(builder);

        builder.child("b");
        persistToBranch(builder);

        builder.child("c");
        persistToBranch(builder);

        builder.child("d");
        persistToBranch(builder);

        builder.child("e");
        persistToBranch(builder);

        merge(ns, builder);

        Sweep2TestHelper.testPre18UpgradeSimulations(ns, builderProvider);
    }

    @Test
    public void mixedPre18BranchTest() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setClusterId(1)
                .setDocumentStore(store).build();
        // step 1 : create an initial structure /a/b/c
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("a").child("b").child("c").setProperty("commit", "1");
        merge(ns, builder);

        // step 2 : create a branch commit incl. with a child under an existing node, ie /a/b/c/d..
        builder = ns.getRoot().builder();
        builder.child("a").setProperty("commit", "2");
        builder.child("a").child("b").child("c").child("d").child("e").setProperty("commit", "2");
        persistToBranch(builder);
        merge(ns, builder);

        // step 3 : verify that "_bc" are set correctly : on all except /a/b (where nothing was changed)
        assertNotNull(store.find(NODES, Utils.getIdFromPath("/")).get("_bc"));
        assertNotNull(store.find(NODES, Utils.getIdFromPath("/a")).get("_bc"));
        assertNull(store.find(NODES, Utils.getIdFromPath("/a/b")).get("_bc"));
        assertNotNull(store.find(NODES, Utils.getIdFromPath("/a/b/c")).get("_bc"));
        assertNotNull(store.find(NODES, Utils.getIdFromPath("/a/b/c/d")).get("_bc"));
        assertNotNull(store.find(NODES, Utils.getIdFromPath("/a/b/c/d/e")).get("_bc"));

        // step 4 : verify the "_bc" are set correctly by backgroundSweep()/forceBackgroundSweep()

        // step 4b: /a/b/c did not get a "_bc" because it only contained "_commitRoot" but no other changes, and
        //          https://github.com/apache/jackrabbit-oak/blob/d35346d4d446908c7019e931cb54d88824c1a637/oak-store-document/src/main/java/org/apache/jackrabbit/oak/plugins/document/NodeDocumentSweeper.java#L179
        //          only went through document that had changes in properties in PROPERTY_OR_DELETED

        // step 4c: / also failed with a similar reason as the above, except the root
        //          only has changes in "_revisions" (not "_commitRoot")
        Sweep2TestHelper.testPre18UpgradeSimulations(ns, builderProvider);
    }

    @Test
    public void orphanedBranchTest() {
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setDocumentStore(store).build();
        NodeBuilder builder = ns.getRoot().builder();
        builder.setProperty("p", "v");
        persistToBranch(builder);
        ns.dispose();

        // start again
        ns = builderProvider.newBuilder()
                .setDocumentStore(store).build();
        assertFalse(ns.getRoot().hasProperty("p"));

        Sweep2TestHelper.testPre18UpgradeSimulations(ns, builderProvider);
    }

    @Test
    public void compareBranchStates() {
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setAsyncDelay(0).build();
        ns.runBackgroundOperations();
        DocumentStore store = ns.getDocumentStore();

        NodeBuilder builder = ns.getRoot().builder();
        builder.setProperty("p", "a");
        persistToBranch(builder);
        NodeState s1 = builder.getNodeState();
        builder.setProperty("p", "b");
        persistToBranch(builder);
        NodeState s2 = builder.getNodeState();

        Set<String> changes = new HashSet<>();
        NodeStateDiff diff = new DefaultNodeStateDiff() {
            @Override
            public boolean propertyChanged(PropertyState before,
                                           PropertyState after) {
                changes.add(before.getName());
                return super.propertyChanged(before, after);
            }
        };
        s2.compareAgainstBaseState(s1, diff);
        assertThat(changes, contains("p"));

        NodeDocument root = getRootDocument(store);
        RevisionVector br = asDocumentState(s1).getRootRevision();
        assertTrue(br.isBranch());
        DocumentNodeState state = root.getNodeAtRevision(ns, br, null);
        assertNotNull(state);
        assertEquals("a", state.getString("p"));

        Sweep2TestHelper.testPre18UpgradeSimulations(ns, builderProvider);
    }

    private void assertModifiedPaths(Iterable<Path> actual, String... expected) {
        Set<Path> expectedSet = Sets.newHashSet();
        for (String p : expected) {
            expectedSet.add(Path.fromString(p));
        }
        assertEquals(expectedSet, Sets.newHashSet(actual));
    }
}
