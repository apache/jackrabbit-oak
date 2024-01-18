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

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.collect.Maps;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture.MongoFixture;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.prefetch.CountingMongoDatabase;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.Clock;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.TestUtils.NO_BINARY;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getAllDocuments;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getRootDocument;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DocumentNodeStoreSweepTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private Clock clock;

    private FailingDocumentStore store;

    private DocumentNodeStore ns;

    private MongoFixture mf;

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
        if (mf != null) {
            mf.dispose();
            mf = null;
        }
    }

    @AfterClass
    public static void resetClock() {
        Revision.resetClockToDefault();
        ClusterNodeInfo.resetClockToDefault();
    }

    interface UpdateCallback {
        /**
         * @return true to continue going via this UpdateHandler, false to stop using
         *         this UpdateHandler
         */
        boolean handleUpdate(UpdateOp op);
    }

    /** limited purpose MongoFixture that allows to pause after a specific update */
    MongoFixture pausableMongoDocumentStore(final String targetId,
            final UpdateCallback updateHandler) {
        return new MongoFixture() {
            @Override
            public DocumentStore createDocumentStore(Builder builder) {
                try {
                    MongoConnection connection = MongoUtils.getConnection();
                    CountingMongoDatabase db = new CountingMongoDatabase(
                            connection.getDatabase());
                    return new MongoDocumentStore(connection.getMongoClient(), db,
                            builder) {
                        volatile boolean done;

                        @Override
                        public <T extends Document> T findAndUpdate(
                                Collection<T> collection, UpdateOp update) {
                            try {
                                return super.findAndUpdate(collection, update);
                            } finally {
                                updateHandler(targetId, updateHandler, update);
                            }
                        }

                        private void updateHandler(final String targetId,
                                final UpdateCallback updateHandler, UpdateOp update) {
                            if (done) {
                                return;
                            }
                            if (update.getId().equals(targetId)) {
                                if (!updateHandler.handleUpdate(update)) {
                                    done = true;
                                    return;
                                }
                            }
                        }

                        @Override
                        public <T extends Document> T createOrUpdate(
                                Collection<T> collection, UpdateOp update) {
                            try {
                                return super.createOrUpdate(collection, update);
                            } finally {
                                updateHandler(targetId, updateHandler, update);
                            }
                        }

                        @Override
                        public <T extends Document> List<T> createOrUpdate(
                                Collection<T> collection, List<UpdateOp> updateOps) {
                            try {
                                return super.createOrUpdate(collection, updateOps);
                            } finally {
                                for (UpdateOp update : updateOps) {
                                    updateHandler(targetId, updateHandler, update);
                                }
                            }
                        }
                    };
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
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
     * <li>beforre clusterId 4 proceeds, it happens to force a read from
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
    @Ignore(value = "OAK-10595")
    public void cachingUncommittedBeforeCollisionRollback() throws Exception {
        // two nodes part of the game:
        // 1 : the main one that starts to do a subtree deletion
        // 2 : a peer one that gets in between the above and causes a collision
        // as a result 1 manages to read 2's rolled back (uncommitted) state as
        // committed

        ns.dispose();

        final Semaphore breakpoint1 = new Semaphore(0);
        final Semaphore breakpoint2 = new Semaphore(0);

        final AtomicReference<Thread> breakInThread = new AtomicReference<Thread>();
        UpdateCallback breakOnceInThread = new UpdateCallback() {

            @Override
            public boolean handleUpdate(UpdateOp update) {
                final Thread localThread = breakInThread.get();
                if (localThread == null || !localThread.equals(Thread.currentThread())) {
                    return true;
                }
                breakpoint1.release();
                try {
                    breakpoint2.tryAcquire(1, 30, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return false;
            }

        };

        mf = pausableMongoDocumentStore("2:/parent/foo",
                breakOnceInThread);
        DocumentStore store1 = mf.createDocumentStore(4);
        DocumentStore store2 = mf.createDocumentStore(2);
        ns = builderProvider.newBuilder().setDocumentStore(store1)
                // use lenient mode because tests use a virtual clock
                .setLeaseCheckMode(LeaseCheckMode.LENIENT).setClusterId(4).clock(clock)
                .setAsyncDelay(0).getNodeStore();
        DocumentNodeStore ns4 = ns;
        DocumentNodeStore ns2 = builderProvider.newBuilder().setDocumentStore(store2)
                // use lenient mode because tests use a virtual clock
                .setLeaseCheckMode(LeaseCheckMode.LENIENT).setClusterId(2).clock(clock)
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
        Runnable codeOn2 = new Runnable() {

            @Override
            public void run() {
                try {
                    // now delete but intercept the _revisions update
                    NodeBuilder builder = ns2.getRoot().builder();
                    assertTrue(builder.child("parent").child("foo").remove());
                    assertTrue(builder.child("parent").child("bar").remove());
                    breakInThread.set(Thread.currentThread());
                    merge(ns2, builder);
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
        assertTrue(breakpoint1.tryAcquire(1, 30, TimeUnit.SECONDS));

        // then continue with the regular collision on 4
        merge(ns4, collisionBuilder4);

        // check at this point though, /parent/foo is still there:
        assertTrue(ns4.getRoot().getChildNode("parent").hasChildNode("foo"));

        // release things and go ahead
        breakpoint2.release();
        assertTrue(successOn2.tryAcquire(5, TimeUnit.SECONDS));

        // some bg ops...
        ns4.runBackgroundOperations();
        ns2.runBackgroundOperations();
        ns4.runBackgroundOperations();

        assertTrue(ns4.getRoot().getChildNode("parent").hasChildNode("foo"));
        {
            NodeBuilder b2 = ns2.getRoot().builder();
            b2.child("parent").child("bar").setProperty("z", "v");
            merge(ns2, b2);
            ns2.runBackgroundOperations();
        }
        ns4.runBackgroundOperations();

        // this now fails since
        // 1) the uncommitted collison rollback deletion is still there
        // "_deleted":{"r123456789a-0-2":"true",..}
        // 2) plus it now resolves to commitValue "c" since it is now passed
        // the sweep revision (since we did another commit/bg just few lines above)
        assertTrue("/parent/foo should exist", ns4.getRoot().getChildNode("parent").hasChildNode("foo"));
    }

    @Test
    public void simple() throws Exception {
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        merge(ns, builder);

        RevisionVector head = ns.getHeadRevision();
        ns.dispose();

        NodeDocument rootDoc = getRootDocument(store);
        assertNotNull(rootDoc);
        // after dispose, the sweep revision must be at least the head revision
        Revision localHead = head.getRevision(ns.getClusterId());
        assertNotNull(localHead);
        assertFalse(rootDoc.getSweepRevisions().isRevisionNewer(localHead));
    }

    @Test
    public void rollbackFailed() throws Exception {
        createUncommittedChanges();
        // after a new head and a background sweep, the
        // uncommitted changes must be cleaned up
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        merge(ns, builder);
        ns.runBackgroundUpdateOperations();

        ns.runBackgroundSweepOperation();
        assertCleanStore();
    }

    @Test
    public void rollbackFailedWithDispose() throws Exception {
        createUncommittedChanges();
        // dispose must trigger sweeper
        ns.dispose();
        assertCleanStore();
    }

    @Test
    public void sameClusterNodeRecovery() throws Exception {
        int clusterId = ns.getClusterId();
        createUncommittedChanges();

        // simulate a crashed node store
        crashDocumentNodeStore();

        // store must be clean after restart
        ns = createDocumentNodeStore(clusterId);
        assertCleanStore();
    }

    @Test
    public void recoveryAfterGC() throws Exception {
        int clusterId = ns.getClusterId();

        for (int i = 0; i < 5; i++) {
            NodeBuilder builder = ns.getRoot().builder();
            builder.child("foo").child("node-" + i);
            merge(ns, builder);
            // root document must contain a revisions entry for this commit
            Revision r = ns.getHeadRevision().getRevision(clusterId);
            assertNotNull(r);
            assertTrue(getRootDocument(store).getLocalRevisions().containsKey(r));
        }
        // split the root
        NodeDocument doc = getRootDocument(store);
        List<UpdateOp> ops = SplitOperations.forDocument(doc, ns,
                ns.getHeadRevision(), NO_BINARY, 2);
        String prevId = null;
        for (UpdateOp op : ops) {
            if (Utils.isPreviousDocId(op.getId())) {
                prevId = op.getId();
            }
        }
        // there must be an operation for a split doc
        assertNotNull(prevId);
        store.createOrUpdate(Collection.NODES, ops);
        ns.runBackgroundOperations();

        // wait an hour
        clock.waitUntil(clock.getTime() + TimeUnit.HOURS.toMillis(1));

        // do some other changes not followed by background update
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo").child("node-0").child("bar");
        merge(ns, builder);

        // run GC
        ns.getVersionGarbageCollector().gc(30, TimeUnit.MINUTES);
        // now the split document must be gone
        assertNull(store.find(Collection.NODES, prevId));

        // simulate a crashed node store
        crashDocumentNodeStore();

        // store must be clean after restart
        ns = createDocumentNodeStore(clusterId);
        assertCleanStore();

        // and nodes must still exist
        for (int i = 0; i < 5; i++) {
            assertNodeExists("/foo/node-" + i);
            assertNodeExists("/foo/node-0/bar");
        }
    }

    @Test
    public void otherClusterNodeRecovery() throws Exception {
        int clusterId = ns.getClusterId();
        createUncommittedChanges();

        // simulate a crashed node store
        crashDocumentNodeStore();

        // wait for lease to expire
        clock.waitUntil(clock.getTime() + ClusterNodeInfo.DEFAULT_LEASE_DURATION_MILLIS);

        // start a new node store with a different clusterId
        ns = createDocumentNodeStore(clusterId + 1);

        // then run recovery for the other cluster node
        assertTrue(ns.getLastRevRecoveryAgent().recover(clusterId) > 0);
        // now the store must be clean
        assertCleanStore();
    }

    @Test
    public void pre18ClusterNodeRecovery() throws Exception {
        int clusterId = ns.getClusterId();
        createUncommittedChanges();
        // add a node, but do not run background write
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("node");
        merge(ns, builder);

        // simulate a crashed node store
        crashDocumentNodeStore();
        // and remove the sweep revision for clusterId
        // this will look like an upgraded and crashed pre 1.8 node store
        UpdateOp op = new UpdateOp(getIdFromPath(Path.ROOT), false);
        op.removeMapEntry("_sweepRev", new Revision(0, 0, clusterId));
        assertNotNull(store.findAndUpdate(Collection.NODES, op));
        NodeDocument rootDoc = getRootDocument(store);
        assertNull(rootDoc.getSweepRevisions().getRevision(clusterId));

        // wait for lease to expire
        clock.waitUntil(clock.getTime() + ClusterNodeInfo.DEFAULT_LEASE_DURATION_MILLIS);

        // start a new node store with a different clusterId
        ns = createDocumentNodeStore(clusterId + 1);

        // then run recovery for the other cluster node
        assertTrue(ns.getLastRevRecoveryAgent().recover(clusterId) > 0);
        // must not set a sweep revision
        rootDoc = getRootDocument(store);
        assertNull(rootDoc.getSweepRevisions().getRevision(clusterId));
    }

    @Test
    public void lowerSweepLimit() throws Exception {
        ns.dispose();
        // restart with a document store that tracks queries
        final Map<String, Long> queries = Maps.newHashMap();
        store = new FailingDocumentStore(new MemoryDocumentStore() {
            @NotNull
            @Override
            public <T extends Document> List<T> query(Collection<T> collection,
                                                      String fromKey,
                                                      String toKey,
                                                      String indexedProperty,
                                                      long startValue,
                                                      int limit) {
                queries.put(indexedProperty, startValue);
                return super.query(collection, fromKey, toKey,
                        indexedProperty, startValue, limit);
            }
        });
        ns = createDocumentNodeStore(0);

        createUncommittedChanges();
        // get the revision of the uncommitted changes
        Revision r = null;
        for (NodeDocument d : Utils.getAllDocuments(store)) {
            if (d.getPath().toString().startsWith("/node-")) {
                r = Iterables.getFirst(d.getAllChanges(), null);
                break;
            }
        }
        assertNotNull(r);
        // after a new head and a background sweep, the
        // uncommitted changes must be cleaned up
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        merge(ns, builder);
        queries.clear();
        ns.runBackgroundOperations();
        assertCleanStore();
        // sweeper must have looked at most recently modified documents
        Long modified = queries.get(NodeDocument.MODIFIED_IN_SECS);
        assertNotNull(modified);
        long startValue = NodeDocument.getModifiedInSecs(r.getTimestamp());
        assertEquals(startValue, modified.longValue());
    }

    private void assertNodeExists(String path) {
        NodeState n = ns.getRoot();
        for (String name : PathUtils.elements(path)) {
            n = n.getChildNode(name);
            assertTrue(name + " does not exist", n.exists());
        }
    }

    private void createUncommittedChanges() throws Exception {
        ns.setMaxBackOffMillis(0);
        NodeBuilder builder = ns.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            builder.child("node-" + i);
        }

        store.fail().after(5).eternally();
        try {
            merge(ns, builder);
            fail("must fail with exception");
        } catch (CommitFailedException e) {
            // expected
        }
        store.fail().never();

        // store must now contain uncommitted changes
        NodeDocument doc = null;
        for (NodeDocument d : Utils.getAllDocuments(store)) {
            if (d.getPath().toString().startsWith("/node-")) {
                doc = d;
                break;
            }
        }
        assertNotNull(doc);
        assertNull(doc.getNodeAtRevision(ns, ns.getHeadRevision(), null));
        SortedMap<Revision, String> deleted = doc.getLocalDeleted();
        assertEquals(1, deleted.size());
        assertNull(ns.getCommitValue(deleted.firstKey(), doc));
    }

    private void assertCleanStore() {
        for (NodeDocument doc : getAllDocuments(store)) {
            for (Revision c : doc.getAllChanges()) {
                String commitValue = ns.getCommitValue(c, doc);
                assertTrue("Revision " + c + " on " + doc.getId() + " is not committed: " + commitValue,
                        Utils.isCommitted(commitValue));
            }
        }
    }

    private DocumentNodeStore createDocumentNodeStore(int clusterId) {
        return builderProvider.newBuilder().setDocumentStore(store)
                // use lenient mode because tests use a virtual clock
                .setLeaseCheckMode(LeaseCheckMode.LENIENT)
                .setClusterId(clusterId).clock(clock).setAsyncDelay(0)
                .getNodeStore();
    }

    private void crashDocumentNodeStore() {
        // prevent writing anything in dispose
        store.fail().after(0).eternally();
        try {
            ns.dispose();
            fail("must fail with an exception");
        } catch (DocumentStoreException e) {
            // expected
        }
        // allow writes again
        store.fail().never();
    }

}
