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

import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.persistToBranch;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.cache.Cache;

import junitx.util.PrivateAccessor;

/**
 * Tests around branch commits and the sweep2 introduced with OAK-9176
 */
public class Sweep2Test {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    /**
     * Test case 1 : direct upgrade from pre-1.8 to post-OAK-9176
     */
    @Test
    public void testSweep2UnecessaryPre18Direct() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setAsyncDelay(0)
                .setClusterId(1)
                .setDocumentStore(store).build();
        assertFalse(isSweep2Necessary(ns));
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("n");
        merge(ns, builder);
        assertFalse(isSweep2Necessary(ns));
        Sweep2TestHelper.revertToPre18State(store);
        assertFalse(isSweep2Necessary(ns));
    }

    /**
     * Test case 3 : a pre-1.8 repo was previously upgraded to 1.8, now comes OAK-9176
     */
    @Test
    public void testSweep2UnnecessaryPre18IndirectNoBcs() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setAsyncDelay(0)
                .setClusterId(1)
                .setDocumentStore(store).build();
        assertFalse(isSweep2Necessary(ns));
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("n");
        merge(ns, builder);
        assertFalse(isSweep2Necessary(ns));
        DocumentNodeStore ns2 = Sweep2TestHelper.applyPre18Aging(store, builderProvider, 2);
        Sweep2TestHelper.removeSweep2Status(store);
        assertFalse(isSweep2Necessary(ns2));
    }

    /**
     * Another test for case 3 : a pre-1.8 repo was previously upgraded to 1.8, now comes OAK-9176
     * (This time with branch commits that are fine)
     * @throws Exception
     */
    @Test
    public void testSweep2NecessaryPre18IndirectWithBcs() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setAsyncDelay(0)
                .setClusterId(1)
                .setDocumentStore(store).build();
        assertFalse(isSweep2Necessary(ns));
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("a").child("b").child("c");
        persistToBranch(builder);
        merge(ns, builder);
        assertFalse(isSweep2Necessary(ns));
        DocumentNodeStore ns2 = Sweep2TestHelper.applyPre18Aging(store, builderProvider, 2);
        Sweep2TestHelper.removeSweep2Status(store);
        assertTrue(isSweep2Necessary(ns2));
    }

    @Test
    public void testSweep2() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setAsyncDelay(0)
                .setClusterId(1)
                .setDocumentStore(store).build();
        assertFalse(isSweep2Necessary(ns));
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("a").child("b").child("c");
        persistToBranch(builder);
        merge(ns, builder);

        DocumentNodeStore ns2 = Sweep2TestHelper.applyPre18Aging(store, builderProvider, 2, 0);
        Sweep2TestHelper.removeSweep2Status(store);

        builder = ns.getRoot().builder();
        builder.child("d");
        persistToBranch(builder);
        assertFalse(DocumentNodeStoreSweepIT.isClean(ns2, "/d"));
        assertTrue(DocumentNodeStoreSweepIT.isClean(ns2, "/a"));
        assertTrue(ns2.backgroundSweep2(0));
        assertFalse(DocumentNodeStoreSweepIT.isClean(ns2, "/d"));
        assertTrue(DocumentNodeStoreSweepIT.isClean(ns2, "/a"));
    }

    @Test
    public void testSweep2Uncommitted() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        FailingDocumentStore fStore = new FailingDocumentStore(store, 42);
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setAsyncDelay(0)
                .setClusterId(1)
                .setDocumentStore(fStore).build();
        assertFalse(isSweep2Necessary(ns));

        DocumentNodeStoreSweepIT.createUncommittedChanges(ns, fStore);
        DocumentNodeStore ns2 = Sweep2TestHelper.applyPre18Aging(store, builderProvider, 2, 0);
        Sweep2TestHelper.removeSweep2Status(store);

        // node-1 is not committed
        assertFalse(DocumentNodeStoreSweepIT.isClean(ns2, "/node-1"));
        assertTrue(ns2.backgroundSweep2(0));
        // doing a backgroundSweep2 should not change anything on node-1 though,
        // so it should still not be clean
        assertFalse(DocumentNodeStoreSweepIT.isClean(ns2, "/node-1"));
    }

    /**
     * Another test for case 3 : a pre-1.8 repo was previously upgraded to 1.8, now comes OAK-9176
     * (This time with branch commits that are fine)
     * @throws Exception
     */
    @Test
    public void testSweep2NecessaryPre18IndirectWithBcsAndSplitDocs() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setAsyncDelay(0)
                .setClusterId(1)
                .setDocumentStore(store).build();
        assertFalse(isSweep2Necessary(ns));
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("a").child("b").child("c");
        persistToBranch(builder);
        merge(ns, builder);
        String data = new String(new byte[16 * 1024]);
        for(int i = 0; i < 50; i++) {
            builder = ns.getRoot().builder();
            builder.setProperty("p-" + i, data);
            merge(ns, builder);
        }
        assertEquals(4, store.query(Collection.NODES, NodeDocument.MIN_ID_VALUE, NodeDocument.MAX_ID_VALUE, Integer.MAX_VALUE).size());
        ns.runBackgroundUpdateOperations();
        // make sure we have a split doc
        assertEquals(5, store.query(Collection.NODES, NodeDocument.MIN_ID_VALUE, NodeDocument.MAX_ID_VALUE, Integer.MAX_VALUE).size());
        assertFalse(isSweep2Necessary(ns));
        DocumentNodeStore ns2 = Sweep2TestHelper.applyPre18Aging(store, builderProvider, 2);
        Sweep2TestHelper.removeSweep2Status(store);
        assertTrue(isSweep2Necessary(ns2));
    }

    @Test
    public void testSweep2Lock() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setAsyncDelay(0)
                .setClusterId(1)
                .setDocumentStore(store).build();
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("n");
        merge(ns, builder);
        Sweep2TestHelper.revertToPre18State(store);
        assertFalse(isSweep2Necessary(ns));
        long lock = acquireSweep2Lock(ns);
        // successfully locked => 1
        assertEquals(1, lock);
        assertTrue(forceReleaseSweep2LockAndMarkSwept(store, 1));
        lock = acquireSweep2Lock(ns);
        // could not lock, lock not needed => -1
        assertEquals(-1, lock);
    }

    @Test
    public void largeBranchCommitsWidthTest() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setAsyncDelay(0)
                .setClusterId(1)
                .setDocumentStore(store).build();

        NodeBuilder builder = ns.getRoot().builder();
        builder.child("a");
        merge(ns, builder);

        for(int i = 0; i < 1000; i ++) {
            if (i % 100 == 0) {
                System.out.println("AT i = " + i);
            }
            builder = ns.getRoot().builder();
            builder.getChildNode("a").child("b" + i);
            persistToBranch(builder);
            merge(ns, builder);
        }

        Sweep2TestHelper.testPre18UpgradeSimulations(ns, builderProvider);
    }

    @SuppressWarnings("unchecked")
    private Cache<Revision, String> forceGetCommitValueResolverCache(DocumentNodeStore ns) throws NoSuchFieldException {
        final CachingCommitValueResolver commitValueResolver = (CachingCommitValueResolver) PrivateAccessor.getField(ns, "commitValueResolver");
        assertNotNull("could not get the commitValueResolver from DocumentNodeStore", commitValueResolver);
        final Cache<Revision, String> commitValueCache = (Cache<Revision, String>) PrivateAccessor.getField(commitValueResolver, "commitValueCache");
        assertNotNull("could not get the commitValueCache from CachingCommitValueResolver", commitValueCache);
        return commitValueCache;
    }

    @Test
    public void branchCommitLastRevTest() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setClusterId(1)
                .setAsyncDelay(0) // dont run the bg operations
                .setDocumentStore(store).build();

        @NotNull
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("a1").child("b0");
        merge(ns, builder);

        builder = ns.getRoot().builder();
        builder.child("a1").child("b").child("c").setProperty("p", "v");
        persistToBranch(builder);
        merge(ns, builder);

        builder = ns.getRoot().builder();
        builder.child("a2");
        merge(ns, builder);

        // let ns crash - only set it to disposed - the bg operations aren't running,
        // no update will be done after isDisposed.set(true)
        AtomicBoolean isDisposed = (AtomicBoolean) PrivateAccessor.getField(ns, "isDisposed");
        isDisposed.set(true);

        @SuppressWarnings("unused")
        MemoryDocumentStore orig = store.copy();

        ns = Sweep2TestHelper.applyPre18Aging(
                Sweep2TestHelper.getMemoryDocumentStore(ns), builderProvider, 2);
        store = Sweep2TestHelper.getMemoryDocumentStore(ns);
        UpdateOp update = new UpdateOp("1", false);
        update.set("leaseEnd", 0L);
        store.findAndUpdate(Collection.CLUSTER_NODES, update);
        ns.getLastRevRecoveryAgent().performRecoveryIfNeeded();

        // make sure to do a bgRead as otherwise clusterId 2 might not see what the
        // recovery (even though on clusterId 2 as well) has just recovered.
        ns.runBackgroundReadOperations();

        DocumentNodeState r1 = ns.getRoot();
        NodeState a1 = r1.getChildNode("a1");
        assertTrue(a1.exists());
        NodeState b = a1.getChildNode("b");
        assertTrue(b.exists());
        NodeState c = b.getChildNode("c");
        assertTrue(c.exists());
        assertTrue(c.hasProperty("p"));
    }

    /**
     * Reproduces a case of the following exception:
     * <pre>
     * org.apache.jackrabbit.oak.plugins.document.DocumentStoreException: Aborting getChildNodes() - DocumentNodeState is null
     * </pre>
     * @throws Exception
     */
    @Test
    @Ignore(value = "the test fails on purpose")
    public void abortingGetChildNodesDocumentNodeStateIsNullTest() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setAsyncDelay(0)
                .setClusterId(1)
                .setDocumentStore(store).build();
        // step 1 : create some structure where some nodes (parent) do not have a "_bc"
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("base").child("camp");
        merge(ns, builder);
        builder = ns.getRoot().builder();
        builder.child("base").child("camp").child("a").child("b").child("c").setProperty("p", "v");
        persistToBranch(builder);
        merge(ns, builder);
        ns.dispose();
        MemoryDocumentStore memStore = Sweep2TestHelper.getMemoryDocumentStore(ns);
        MemoryDocumentStore orig = memStore.copy();
        Sweep2TestHelper.revertToPre18State(memStore);
        // avoid doing a sweep2 as that would otherwise fix the test
        NodeDocumentSweeper.SWEEP_ONE_PREDICATE = Utils.PROPERTY_OR_DELETED;
        Sweep2StatusDocument.forceReleaseSweep2LockAndMarkSwept(memStore, 1);
        ns = builderProvider.newBuilder()
                .setAsyncDelay(0)
                .setClusterId(1)
                .setDocumentStore(memStore).build();
        // do another restart to flush the cache - since the above did another sweep..
        ns.dispose();
        ns = builderProvider.newBuilder()
                .setAsyncDelay(0)
                .setClusterId(1)
                .setDocumentStore(memStore).build();

        // initialization of the DocumentNodeStore did add a few entries to the
        // caches which we want full control over - so let's clear them all.
        // without this, the test is dependent on this initialization behaviour,
        // and that's not ideal (besides, flushing the cache should always be working fine)
        forceGetCommitValueResolverCache(ns).invalidateAll();
        ns.getNodeCache().invalidateAll();

        NodeDocument origRootDoc = orig.find(Collection.NODES, "0:/");
        Set<Revision> origRootBcs = origRootDoc.getLocalBranchCommits();

        // step 2 : get a DocumentNodeState of such a parent loaded with lastRev on a branch commit
        DocumentNodeState a = (DocumentNodeState) ns.getRoot().getChildNode("base").getChildNode("camp").getChildNode("a");
        RevisionVector lastRev = a.getLastRevision();
        Revision lastRev1 = lastRev.iterator().next();
        assertTrue("DocumentNodeState(/base/camp/a) not created with a lastRev matching a branchCommit: " + a, origRootBcs.contains(lastRev1));
        Cache<Revision, String> commitValueCache = forceGetCommitValueResolverCache(ns);
        String cachedCommitValue = commitValueCache.getIfPresent(lastRev1);
        assertNotNull(cachedCommitValue);
        assertTrue("c".equals(cachedCommitValue));

        // step 3 : get the children of that node
        // step 3.1 : first it will do DocumentNodeStore.getChildren(aBcLastRev) which uses
        //            DocumentNodeState.Children children = nodeChildrenCache
        // -> this step should find the child
        // --> this is when CachingCommitValueResolver contains a "c" for a lastRev
        assertTrue(ns.getRoot().getChildNode("base").getChildNode("camp").getChildNode("a").exists());
        assertTrue(ns.getRoot().getChildNode("base").getChildNode("camp").getChildNode("a").getChildNode("b").exists());
        NamePathRev key = new NamePathRev("", Path.fromString("/base/camp/a"), lastRev);
        assertNull(ns.getNodeChildrenCache().getIfPresent(key));
        ns.getRoot().getChildNode("base").getChildNode("camp").getChildNode("a").getChildNodeEntries().iterator().next();
        assertNotNull(ns.getNodeChildrenCache().getIfPresent(key));

        // step 3.2 : then the CachingCommitValueResolver's cache has an overflow
        commitValueCache.invalidate(lastRev1);
        // step 3.2b : but also remove /a/b from the nodeCache to ensure it has to be freshly loaded
        ns.getNodeCache().invalidate(new PathRev(Path.fromString("/base/camp/a/b"), lastRev));

        // step 3.3 : then it will do DocumentNodeStore.getNode(aBcLastRev) / readNode
        // -> this step should NOT find the child
        // --> this is automatic if the CachingCommitValueResolver's cache is empty
        //     and a fresh lookup is done on a document where the "_bc" is set
        assertTrue(ns.getRoot().getChildNode("base").getChildNode("camp").getChildNode("a").exists());
        assertNotNull(ns.getRoot().getChildNode("base").getChildNode("camp").getChildNode("a").getChildNodeEntries().iterator().next());
        assertTrue(ns.getRoot().getChildNode("base").getChildNode("camp").getChildNode("a").getChildNode("b").exists());
    }

    private static boolean isSweep2Necessary(DocumentNodeStore ns) {
        return Sweep2Helper.isSweep2Necessary(ns.getDocumentStore());
    }

    private static long acquireSweep2Lock(DocumentNodeStore ns) {
        return Sweep2StatusDocument.acquireSweep2Lock(ns, ns.getClusterId());
    }

    private static boolean forceReleaseSweep2LockAndMarkSwept(DocumentStore store, int clusterId) {
        return Sweep2StatusDocument.forceReleaseSweep2LockAndMarkSwept(store, clusterId);
    }
}
