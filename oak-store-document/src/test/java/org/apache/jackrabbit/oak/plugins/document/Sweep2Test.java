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
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.commit.Observer;
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

        DocumentNodeStore ns2 = Sweep2TestHelper.applyPre18Aging(store, withAsyncDelay(builderProvider, 0), 2);
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
        DocumentNodeStore ns2 = Sweep2TestHelper.applyPre18Aging(fStore, withAsyncDelay(builderProvider, 0), 2);
        Sweep2TestHelper.removeSweep2Status(fStore);

        // node-1 is not committed
        assertFalse(DocumentNodeStoreSweepIT.isClean(ns2, "/node-1"));
        assertTrue(ns2.backgroundSweep2(0));
        // doing a backgroundSweep2 should not change anything on node-1 though,
        // so it should still not be clean
        assertFalse(DocumentNodeStoreSweepIT.isClean(ns2, "/node-1"));
    }

    @Test
    public void testFailedJournalWrite() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        FailingDocumentStore fStore = new FailingDocumentStore(store, 42);
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setAsyncDelay(0)
                .setClusterId(1)
                .setDocumentStore(fStore).build();
        assertFalse(isSweep2Necessary(ns));
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("a").child("b").child("c");
        persistToBranch(builder);
        merge(ns, builder);
        assertEquals(0, Sweep2TestHelper.scanForMissingBranchCommits(ns).size());
        DocumentNodeStore ns2 = Sweep2TestHelper.applyPre18Aging(fStore, withAsyncDelay(builderProvider, 0), 2);
        assertEquals(4, Sweep2TestHelper.scanForMissingBranchCommits(ns).size());
        Sweep2TestHelper.removeSweep2Status(fStore);

        fStore.fail().on(Collection.JOURNAL).after(0).once();
        try {
            // directly invoking forceBackgroundSweep2 to have control over the include list
            ns2.forceBackgroundSweep2(Collections.emptyList());
            fail("Should have thrown a DocumentStoreException");
        } catch (DocumentStoreException e) {
            // expected
        }
        assertEquals(4, Sweep2TestHelper.scanForMissingBranchCommits(ns).size());
        // directly invoking forceBackgroundSweep2 to have control over the include list
        ns2.forceBackgroundSweep2(Collections.emptyList());
        assertEquals(0, Sweep2TestHelper.scanForMissingBranchCommits(ns).size());
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
        long lock = acquireOrUpdateSweep2Lock(ns, false);
        // successfully locked => 1
        assertEquals(1, lock);
        assertTrue(forceReleaseSweep2LockAndMarkSwept(store, 1));
        lock = acquireOrUpdateSweep2Lock(ns, false);
        // could not lock, lock not needed => -1
        assertEquals(-1, lock);
    }

    @Test
    public void testSweep2LockStates() throws Exception {
        // normal case without concurrency
        MemoryDocumentStore store = new MemoryDocumentStore();
        assertEquals(1, Sweep2StatusDocument.acquireOrUpdateSweep2Lock(store, 1, false));
        assertSweep2Status(store, true, false, false, null);
        assertTrue(Sweep2StatusDocument.forceReleaseSweep2LockAndMarkSwept(store, 1));
        assertSweep2Status(store, false, false, true, 1);

        // crash case while checking
        store = new MemoryDocumentStore();
        assertEquals(1, Sweep2StatusDocument.acquireOrUpdateSweep2Lock(store, 1, false));
        assertSweep2Status(store, true, false, false, null);
        assertEquals(2, Sweep2StatusDocument.acquireOrUpdateSweep2Lock(store, 2 /*1 crashed*/, false));
        assertSweep2Status(store, true, false, false, null);
        assertTrue(Sweep2StatusDocument.forceReleaseSweep2LockAndMarkSwept(store, 2));
        assertSweep2Status(store, false, false, true, 2);

        // crash case while sweeping
        store = new MemoryDocumentStore();
        assertEquals(1, Sweep2StatusDocument.acquireOrUpdateSweep2Lock(store, 1, false));
        assertSweep2Status(store, true, false, false, null);
        assertEquals(2, Sweep2StatusDocument.acquireOrUpdateSweep2Lock(store, 1, true));
        assertSweep2Status(store, false, true, false, null);
        assertEquals(3, Sweep2StatusDocument.acquireOrUpdateSweep2Lock(store, 2 /*1 crashed*/, false));
        assertSweep2Status(store, false, true, false, null);
        assertTrue(Sweep2StatusDocument.forceReleaseSweep2LockAndMarkSwept(store, 2));
        assertSweep2Status(store, false, false, true, 2);

        // concurrency
        store = new MemoryDocumentStore();
        assertEquals(1, Sweep2StatusDocument.acquireOrUpdateSweep2Lock(store, 1, false));
        assertSweep2Status(store, true, false, false, null);
        assertEquals(2, Sweep2StatusDocument.acquireOrUpdateSweep2Lock(store, 2, false));
        assertSweep2Status(store, true, false, false, null);
        assertEquals(3, Sweep2StatusDocument.acquireOrUpdateSweep2Lock(store, 1, true));
        assertSweep2Status(store, false, true, false, null);
        assertEquals(3, Sweep2StatusDocument.acquireOrUpdateSweep2Lock(store, 1, true));
        assertSweep2Status(store, false, true, false, null);
        assertEquals(4, Sweep2StatusDocument.acquireOrUpdateSweep2Lock(store, 3, false));
        assertSweep2Status(store, false, true, false, null);
        assertEquals(4, Sweep2StatusDocument.acquireOrUpdateSweep2Lock(store, 3, true));
        assertSweep2Status(store, false, true, false, null);
        assertEquals(5, Sweep2StatusDocument.acquireOrUpdateSweep2Lock(store, 4, false));
        assertSweep2Status(store, false, true, false, null);
        assertTrue(Sweep2StatusDocument.forceReleaseSweep2LockAndMarkSwept(store, 2));
        assertSweep2Status(store, false, false, true, 2);
        assertTrue(Sweep2StatusDocument.forceReleaseSweep2LockAndMarkSwept(store, 2));
        assertSweep2Status(store, false, false, true, 2);
        assertTrue(Sweep2StatusDocument.forceReleaseSweep2LockAndMarkSwept(store, 3));
        assertSweep2Status(store, false, false, true, 2 /* not 3 !*/);

        // unexpected cases
        store = new MemoryDocumentStore();
        // normally it wouldn't use "force" first, it would use the default==checking first
        // but the code should probably still behave nice - hence this test.
        assertEquals(1, Sweep2StatusDocument.acquireOrUpdateSweep2Lock(store, 1, true));
        assertSweep2Status(store, false, true, false, null);
        assertTrue(Sweep2StatusDocument.forceReleaseSweep2LockAndMarkSwept(store, 1));
        assertSweep2Status(store, false, false, true, 1);
    }

    private void assertSweep2Status(MemoryDocumentStore store,
            boolean checking, boolean sweeping, boolean swept, Integer sweptById) {
        Sweep2StatusDocument status = Sweep2StatusDocument.readFrom(store);
        assertEquals("checking status mismatch, expected " + checking, checking, status.isChecking());
        assertEquals("sweeping status mismatch, expected " + sweeping, sweeping, status.isSweeping());
        assertEquals("swept status mismatch, expected " + swept, swept, status.isSwept());
        assertEquals("swept by clusterId mismatch, expected " + sweptById, sweptById, status.getSweptById());
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

    /**
     * This method checks that sweep2 doesn't block the shutdown/dispose().
     * - phase A : In order to do so, it first has to make sure there's some data for sweep2 to process.
     * - phase B : Then bring that sweep2 into the right position by using semaphores and such
     * - phase C : trigger the dispose - which initially should be blocked by above semaphore
     * @throws Exception
     */
    @Test
    public void disposeTest() throws Exception {
        // phase A : make sure there's some data for sweep2 to process
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setClusterId(1)
                .setAsyncDelay(0) // dont run the bg operations
                .setDocumentStore(store).build();
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("a1").child("b").child("c").setProperty("p", "v");
        persistToBranch(builder);
        merge(ns, builder);
        ns.dispose();
        // aging, and keep instance 2 running thereafter - it holds a fake sweep2 lock
        DocumentNodeStore ns2 = Sweep2TestHelper.applyPre18Aging(store, withAsyncDelay(builderProvider, 0), 2);
        // add clusterId 1 to the _sweepRev to make sure it gets included in any subsequent sweep2
        UpdateOp addSweepRev1 = new UpdateOp(Utils.getIdFromPath("/"), false);
        addSweepRev1.setMapEntry("_sweepRev", new Revision(0, 0, 1), new Revision(0, 0, 1).toString());
        assertNotNull(store.findAndUpdate(Collection.NODES, addSweepRev1));
        Sweep2TestHelper.removeSweep2Status(store, false);
        assertTrue(Sweep2StatusDocument.acquireOrUpdateSweep2Lock(store, 2, true) > 0);
        assertTrue(Sweep2StatusDocument.readFrom(store).isSweeping());
        assertEquals(2, Sweep2StatusDocument.readFrom(store).getLockClusterId());
        // phase B : bring a sweep2 (on instance 3) right before it would be doing its business
        // start up an instance normally, which should trigger a sweep2
        DocumentNodeStore ns3 = builderProvider.newBuilder()
                .setAsyncDelay(1 /*0 would disable sweep2*/)
                .setClusterId(3)
                .setDocumentStore(store).build();
        // now clusterId 3's sweep2 bgthread will wait because instance 2 is sweeping
        // it will wait until the sweep2 status is either done or instance 2 crashes
        // let's make sure if instance 3 does do a sweep2 it is paused before it can finish
        final Semaphore pausing = new Semaphore(-1);
        final Semaphore doContinue = new Semaphore(2);
        // the below observer is in charge of blocking sweep2 until we're ready to let go
        ns3.addObserver(new Observer() {
            @Override
            public void contentChanged(@NotNull NodeState root, @NotNull CommitInfo info) {
                try {
                    pausing.release();
                    if (!doContinue.tryAcquire(5, TimeUnit.SECONDS)) {
                        fail("timeout acquiring the pause permit");
                    }
                } catch (InterruptedException e) {
                    fail("interrupted acquiring the pause permit");
                }            }
        });
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        // the blocker thread makes sure the above Observer blocks things
        final Thread blockerThread = new Thread(() -> {
            NodeBuilder b = ns3.getRoot().builder();
            b.child("anotherchild");
            try {
                ns3.merge(b, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            } catch (CommitFailedException e) {
                failure.set(e);
            }
        }, "commit-blocker");
        blockerThread.setDaemon(true);
        blockerThread.start();
        // wait for separate-commit thread to reach pausing
        assertTrue(pausing.tryAcquire(5, TimeUnit.SECONDS));
        // then let's crash instance 2 - this should release the lease
        // which should let instance 3 take notice and continue sweep2
        ns2.dispose();
        // wait until instance 3 has the sweep2 lock
        waitMax(5000, () -> {return Sweep2StatusDocument.readFrom(store).getLockClusterId() == 3;});
        assertTrue(Sweep2StatusDocument.readFrom(store).isSweeping());
        // the release thread releases the Observer, thus unblocking the commit
        // - the latter unblocks the sweep2 (which is what we wanted to achieve in the first place)
        final Thread releaseThread = new Thread(() -> {
                // make sure this happens 1sec after ns3.dispose
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // lets assume this doesn't happen
                }
                // let go of the observer-lock
                doContinue.release(1000);
            }, "observer-lock-releaser");
        releaseThread.setDaemon(true);
        // dispose 3 -> this is the actual test happening here: does dispose now
        // interrupt sweep2 as expected?
        final Thread disposeThread = new Thread(() -> {
                ns3.dispose();
        }, "disposer");
        disposeThread.setDaemon(true);
        disposeThread.start();
        // the disposeThread should now be blocked, let's check for that
        // there's not many other options than *actually* waiting a bit to verify that
        Thread.sleep(500);
        assertTrue(disposeThread.isAlive());
        // now release the observer
        releaseThread.start();
        // this should now have let the dispose() continue and terminate
        disposeThread.join(5000);
        assertFalse(disposeThread.isAlive());
        assertTrue(Sweep2StatusDocument.readFrom(store).isSweeping());
        assertEquals("got a failure: " + failure.get(), null, failure.get());
    }

    private void waitMax(int maxWaitMillis, Supplier<Boolean> r) throws InterruptedException {
        final long timeout = System.currentTimeMillis() + 5000;
        while(!r.get()) {
            if (System.currentTimeMillis() > timeout) {
                fail("instance 3 did not pick up the sweep2 lock within time");
            }
            Thread.sleep(10);
        }
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

    /**
     * This tests a repository which contains a mix of revisions from clusterNodeIds
     * that have not been swept and some that have - and then sweep2 comes along.
     */
    @Test
    public void testPartiallySwept() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        for(int clusterId = 1; clusterId <= 5; clusterId++) {
            // create some changes with this specific clusterId
            DocumentNodeStore ns = builderProvider.newBuilder()
                    .setAsyncDelay(0)
                    .setClusterId(clusterId)
                    .setDocumentStore(store).build();
            NodeBuilder builder = ns.getRoot().builder();
            builder.child("a" + clusterId).child("b" + clusterId).child("c" + clusterId);
            persistToBranch(builder);
            merge(ns, builder);
            ns.dispose();
        }
        // revert back the entire store
        Sweep2TestHelper.revertToPre18State(store);
        // no sweep2 necessary at this point as no sweep1 was done yet
        assertFalse(sweep2Done(store));
        Sweep2TestHelper.revertToPre18State(store);
        // then do a sweep1 for some of the clusterIds only
        for (DocumentNodeStore ns : Sweep2TestHelper.applyPre18Aging(store, builderProvider, 1, 2)) {
            ns.dispose();
        }
        store.remove(Collection.SETTINGS, Sweep2StatusDocument.SWEEP2_STATUS_ID);
        assertFalse(sweep2Done(store));
        // now start up an instance normally, which should trigger a sweep2
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setAsyncDelay(1 /*0 would disable sweep2*/)
                .setClusterId(3)
                .setDocumentStore(store).build();
        assertTrue(waitForSweep2Done(store, 2000));
        ns.dispose();
        assertTrue(sweep2Done(store));

        // the result of all of this should be that
        // * for clusterId 1 and 2 a sweep1 happened first, then a sweep2 should have been done
        // * for clusterId 3 a sweep2 happened directly (without a sweep1)
        // * for clusterId 4 and 5 no sweep1/2 happened - they still look "pre 1.8" and stay that way

        assertTrue("/a1 has no _bc", hasBcProperty(store, "/a1"));
        assertTrue("/a2 has no _bc", hasBcProperty(store, "/a2"));
        assertTrue("/a3 has no _bc", hasBcProperty(store, "/a3"));
        assertFalse("/a4 has _bc", hasBcProperty(store, "/a4"));
        assertFalse("/a5 has _bc", hasBcProperty(store, "/a5"));
    }

    private boolean hasBcProperty(MemoryDocumentStore store, String path) {
        NodeDocument doc = store.find(Collection.NODES, Utils.getIdFromPath(path));
        if (doc == null) {
            fail("could not find " + path);
        }
        Set<Revision> bc = doc.getLocalBranchCommits();
        return bc != null && !bc.isEmpty();
    }

    private boolean waitForSweep2Done(MemoryDocumentStore store, int maxWaitMillis) throws InterruptedException {
        final long timeout = System.currentTimeMillis() + maxWaitMillis;
        while(!sweep2Done(store) && System.currentTimeMillis() < timeout) {
            Thread.sleep(10);
        }
        return sweep2Done(store);
    }

    private boolean sweep2Done(MemoryDocumentStore store) {
        Sweep2StatusDocument sweep2Status = Sweep2StatusDocument.readFrom(store);
        if (sweep2Status == null) {
            return false;
        }
        return sweep2Status.isSwept();
    }

    private static boolean isSweep2Necessary(DocumentNodeStore ns) {
        return Sweep2Helper.isSweep2Necessary(ns.getDocumentStore());
    }

    private static long acquireOrUpdateSweep2Lock(DocumentNodeStore ns, boolean forceSweepingStatus) {
        return Sweep2StatusDocument.acquireOrUpdateSweep2Lock(ns.getDocumentStore(), ns.getClusterId(), forceSweepingStatus);
    }

    private static boolean forceReleaseSweep2LockAndMarkSwept(DocumentStore store, int clusterId) {
        return Sweep2StatusDocument.forceReleaseSweep2LockAndMarkSwept(store, clusterId);
    }

    private DocumentMKBuilderProvider withAsyncDelay(DocumentMKBuilderProvider builderProvider, int asyncDelay) {
        return new DocumentMKBuilderProvider() {
            @Override
            public Builder newBuilder() {
                return super.newBuilder().setAsyncDelay(asyncDelay);
            }
        };
    }
}
