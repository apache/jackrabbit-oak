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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.PausableDocumentStore.PauseCallback;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class CollisionRollbackTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private Clock clock;

    private FailingDocumentStore store;

    private DocumentNodeStore ns;

    private final DocumentStoreFixture fixture;

    public CollisionRollbackTest(DocumentStoreFixture fixture) {
        this.fixture = fixture;
    }

    @Parameterized.Parameters(name="{0}")
    public static java.util.Collection<Object[]> fixtures() throws IOException {
        List<Object[]> fixtures = new ArrayList<>();
        fixtures.add(new Object[] {new DocumentStoreFixture.MemoryFixture()});
        fixtures.add(new Object[] {new DocumentStoreFixture.ClusterlikeMemoryFixture()});
        fixtures.add(new Object[] {new DocumentStoreFixture.RDBFixture()});

        DocumentStoreFixture mongo = new DocumentStoreFixture.MongoFixture();
        if(mongo.isAvailable()){
            fixtures.add(new Object[] {mongo});
        }
        return fixtures;
    }

    @Before
    public void before() throws Exception {
        clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        ClusterNodeInfo.setClock(clock);
        store = new FailingDocumentStore(new MemoryDocumentStore());
        ns = createDocumentNodeStore(0);
    }

    @After
    public void after() throws Exception {
        if (fixture != null) {
            fixture.dispose();
        }
    }

    @AfterClass
    public static void resetClock() {
        Revision.resetClockToDefault();
        ClusterNodeInfo.resetClockToDefault();
    }

    /**
     * Test to show-case a race-condition between a collision and
     * MongoDocumentStore's nodesCache: when a document is read into
     * the nodesCache shortly before a collision is rolled back,
     * it runs risk of later making uncommitted changes visible.
     * <p/>
     * The test case works as follows:
     * <ul>
     * <li>consider clusterId 2 and 4 being active in a cluster</li>
     * <li>a target path /parent/foo (and sibling /parent/bar) having 
     * formerly been created</li>
     * <li>clusterId 2: now wants to delete /parent/foo and /parent/bar, and starts
     * doing so versus mongo by first setting _deleted:true on those two nodes
     * (using revision r123456789a-0-2)</li>
     * <li>before clusterId 2 continues, clusterId 4 comes with a conflicting update
     * on /parent/bar (using revision r123456789b-0-4). This update notices
     * the changes from 2 and leaves a corresponding collision marker (on 0:/
     * with _collisions.r123456789a-0-2=r123456789b-0-4)</li>
     * <li>before clusterId 4 proceeds, it happens to force a read from
     * 2:/parent/foo from mongo - this is achieved as a result of
     * another /parent/foo</li>
     * <li>the result of the above is clusterId 4 having a state of 2:/parent/foo
     * in its MongoDocumentStore nodesCache that contains uncommitted information.
     * In this test case, that uncommitted information is a deletion. But it could
     * be anything else really.</li>
     * <li>then things continue on both clusterId 2 and 4 (from the previous
     * test-pause)</li>
     * <li>then clusterId 2 does another, unrelated change on /parent/bar,
     * thereby resulting in a newer lastRev (on root and /parent) than the collision.
     * Also, it results in a sweepRev that is newer than the collision</li>
     * <li>when later, clusterId 4 reads /parent/foo, it still finds the
     * cached 2:/parent/foo document with the uncommitted data - plus it
     * now has a readRevision/lastRevision that is newer than that - plus
     * it will resolve that uncommitted data's revision (r123456789a-0-2)
     * as commitvalue="c", since it is older than the sweepRevision</li>
     * <li>and thus, clusterId 4 managed to read uncommitted, rolled back data
     * from an earlier collision</li>
     * </ul>
     */
    @Test
    public void cachingUncommittedBeforeCollisionRollback_withPause() throws Exception {
        doCachingUncommittedBeforeCollisionRollback(false, false);
    }

    /**
     * Same as {@link #cachingUncommittedBeforeCollisionRollback_withPause()},
     * except with a crash instead of a pause (of clusterId 2).
     * This variant should test whether recovery correctly causes cache
     * invalidation on clusterId 4 as well.
     */
    @Test
    public void cachingUncommittedBeforeCollisionRollback_crashBeforeRollback() throws Exception {
        doCachingUncommittedBeforeCollisionRollback(true, false);
    }

    /**
     * Same as {@link #cachingUncommittedBeforeCollisionRollback_crashBeforeRollback()},
     * except it crashes later, after the rollback was applied.
     */
    @Test
    public void cachingUncommittedBeforeCollisionRollback_crashAfterRollback() throws Exception {
        doCachingUncommittedBeforeCollisionRollback(true, true);
    }

    public void doCachingUncommittedBeforeCollisionRollback(
            final boolean crashInsteadOfContinue,
            final boolean crashAfterRollback) throws Exception {
        assumeTrue(fixture.hasSinglePersistence());
        // two nodes part of the game:
        // 2 : the main one that starts to do a subtree deletion
        // 4 : a peer one that gets in between the above and causes a collision.
        // as a result 4 manages to read 2's rolled back (uncommitted) state as
        // committed

        ns.dispose();

        final Semaphore breakpoint1 = new Semaphore(0);
        final Semaphore breakpoint2 = new Semaphore(0);

        PauseCallback breakOnceInThread = new PauseCallback() {

            @Override
            public PauseCallback handlePause(List<UpdateOp> remainingOps) {
                breakpoint1.release();
                try {
                    breakpoint2.tryAcquire(1, 5, TimeUnit.SECONDS);
                    if (crashInsteadOfContinue && !crashAfterRollback) {
                        throw new RuntimeException("crashInsteadOfContinue");
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return null;
            }

        };

        // RDBFixture only support 1 or 2, so the historically chosen 4 had to be (re)set to 1
        int clusterId4 = 1;
        DocumentStore store1 = fixture.createDocumentStore(clusterId4);
        final PausableDocumentStore pausableStore2 = new PausableDocumentStore(fixture.createDocumentStore(2));
        pausableStore2.pauseWith(breakOnceInThread).on(Collection.NODES)
                .on("2:/parent/foo").after(1).afterOp().eternally();
        FailingDocumentStore store2 = new FailingDocumentStore(pausableStore2);

        ns = builderProvider.newBuilder().setDocumentStore(store1)
                // use lenient mode because tests use a virtual clock
                .setLeaseCheckMode(LeaseCheckMode.LENIENT).setClusterId(clusterId4).clock(clock)
                // cancelInvalidation on this nodestore doesn't have any effect in the test
                .setCancelInvalidationFeature(createFeature(true))
                .setAsyncDelay(0).getNodeStore();
        DocumentNodeStore ns4 = ns;
        DocumentNodeStore ns2 = builderProvider.newBuilder().setDocumentStore(store2)
                // use lenient mode because tests use a virtual clock
                .setLeaseCheckMode(LeaseCheckMode.LENIENT).setClusterId(2).clock(clock)
                // cancelInvalidation on this nodestore is what fixes the test
                .setCancelInvalidationFeature(createFeature(true))
                .setAsyncDelay(0).getNodeStore();

        {
            // setup
            NodeBuilder builder = ns4.getRoot().builder();
            builder.child("parent").child("foo");
            builder.child("parent").child("bar");
            merge(ns4, builder);
        }
        ns4.runBackgroundOperations();
        ns2.runBackgroundOperations();

        final Semaphore successOn2 = new Semaphore(0);
        final DocumentNodeStore finalNs2 = ns2;
        Runnable codeOn2 = new Runnable() {

            @Override
            public void run() {
                try {
                    // now delete but intercept the _revisions update
                    NodeBuilder builder = finalNs2.getRoot().builder();
                    assertTrue(builder.child("parent").child("foo").remove());
                    assertTrue(builder.child("parent").child("bar").remove());
                    merge(finalNs2, builder);
                    fail("supposed to fail");
                } catch (CommitFailedException e) {
                    // supposed to fail
                    successOn2.release();
                }
            }

        };
        ns4.runBackgroundOperations();

        // prepare a regular collision on 4
        NodeBuilder collisionBuilder4 = ns4.getRoot().builder();
        collisionBuilder4.child("parent").child("bar").setProperty("collideOnPurpose",
                "indeed");
        // do the collision also on /parent/foo
        collisionBuilder4.child("parent").child("foo").setProperty("someotherchange",
                "42");

        // start /parent/foo deletion on 2 in a separate thread
        Thread th2 = new Thread(codeOn2);
        th2.setDaemon(true);
        th2.start();

        // wait for the separate thread to update /parent/foo but not commit yet
        assertTrue(breakpoint1.tryAcquire(1, 5, TimeUnit.SECONDS));

        // then continue with the regular collision on 4
        // this will now put the rolled-back collision change into 4's cache.
        merge(ns4, collisionBuilder4);

        // check at this point though, /parent/foo is still there:
        assertTrue(ns4.getRoot().getChildNode("parent").hasChildNode("foo"));

        if (crashInsteadOfContinue && !crashAfterRollback) {
            store2.fail().after(0).eternally();
            breakpoint2.release();
            assertTrue(successOn2.tryAcquire(5, TimeUnit.SECONDS));
        } else if (crashInsteadOfContinue) {
            store2.fail().on(Collection.NODES).after(3).eternally();
            breakpoint2.release();
            assertTrue(successOn2.tryAcquire(5, TimeUnit.SECONDS));
            store2.fail().after(0).eternally();
        } else {
            // release things and go ahead
            breakpoint2.release();
            assertTrue(successOn2.tryAcquire(5, TimeUnit.SECONDS));
        }

        // some bg ops...
        ns4.runBackgroundOperations();
        ns2.runBackgroundOperations();
        ns4.runBackgroundOperations();

        // at this point /parent/foo still exists on 4
        // (due to lastRev on 2 not yet being updated)
        assertTrue(ns4.getRoot().getChildNode("parent").hasChildNode("foo"));
        if (crashInsteadOfContinue) {
            try {
                ns2.dispose();
            } catch(DocumentStoreException dse) {
                // ok
            }
            // start up a fresh ns2 for the below change, to update lastrev
            for(int seconds=0; seconds < 1800; seconds+=20) {
                clock.waitUntil(clock.getTime() + TimeUnit.SECONDS.toMillis(1));
                ns4.runBackgroundOperations();
            }
            assertTrue(ns4.getRoot().getChildNode("parent").hasChildNode("foo"));
            store2 = new FailingDocumentStore(fixture.createDocumentStore(2));
            ns2 = builderProvider.newBuilder().setDocumentStore(store2)
                    // use lenient mode because tests use a virtual clock
                    .setLeaseCheckMode(LeaseCheckMode.LENIENT).setClusterId(2).clock(clock)
                    .setAsyncDelay(0).getNodeStore();
            assertTrue(ns4.getRoot().getChildNode("parent").hasChildNode("foo"));
        }
        {
            // but with an update on /parent/bar on 2, _lastRev updates,
            // hence /parent/foo's rolled-back change on 4 now becomes visible
            NodeBuilder b2 = ns2.getRoot().builder();
            b2.child("parent").child("bar").setProperty("z", "v");
            merge(ns2, b2);
            assertTrue(ns4.getRoot().getChildNode("parent").hasChildNode("foo"));
            ns2.runBackgroundOperations();
            assertTrue(ns4.getRoot().getChildNode("parent").hasChildNode("foo"));
        }
        ns4.runBackgroundOperations();

        // this now fails since
        // 1) the uncommitted collison rollback deletion is still there
        // "_deleted":{"r123456789a-0-2":"true",..}
        // 2) plus it now resolves to commitValue "c" since it is now passed
        // the sweep revision (since we did another commit/bg just few lines above)
        assertTrue("/parent/foo should exist", ns4.getRoot().getChildNode("parent").hasChildNode("foo"));
    }

    private static Feature createFeature(boolean enabled) {
        Feature cancelInvalidation = mock(Feature.class);
        when(cancelInvalidation.isEnabled()).thenReturn(enabled);
        return cancelInvalidation;
    }

    private DocumentNodeStore createDocumentNodeStore(int clusterId) {
        return builderProvider.newBuilder().setDocumentStore(store)
                // use lenient mode because tests use a virtual clock
                .setLeaseCheckMode(LeaseCheckMode.LENIENT)
                .setClusterId(clusterId).clock(clock).setAsyncDelay(0)
                .getNodeStore();
    }

}
