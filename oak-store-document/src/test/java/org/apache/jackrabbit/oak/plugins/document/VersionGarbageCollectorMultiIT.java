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
package org.apache.jackrabbit.oak.plugins.document;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.NUM_REVS_THRESHOLD;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.NO_BINARY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.guava.common.base.Function;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture.RDBFixture;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoTestUtils;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBOptions;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.mongodb.ReadPreference;

@RunWith(Parameterized.class)
public class VersionGarbageCollectorMultiIT {

    private DocumentStoreFixture fixture;

    private Clock clock;

    private DocumentNodeStore store1, store2;

    private DocumentStore ds1, ds2;

    private VersionGarbageCollector gc;

    private ExecutorService execService;

    public VersionGarbageCollectorMultiIT(DocumentStoreFixture fixture) {
        this.fixture = fixture;
    }

    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> fixtures() throws IOException {
        return AbstractDocumentStoreTest.fixtures(true);
    }

    @Before
    public void setUp() throws InterruptedException {
        execService = Executors.newCachedThreadPool();
        clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);

        createPrimaryStore();

        // Enforce primary read preference, otherwise tests may fail on a
        // replica set with a read preference configured to secondary.
        // Revision GC usually runs with a modified range way in the past,
        // which means changes made it to the secondary, but not in this
        // test using a virtual clock
        MongoTestUtils.setReadPreference(store1, ReadPreference.primary());
        gc = store1.getVersionGarbageCollector();
    }

    private String rdbTablePrefix = "T" + Long.toHexString(System.currentTimeMillis());

    private void createPrimaryStore() {
        if (fixture instanceof RDBFixture) {
            ((RDBFixture) fixture).setRDBOptions(
                    new RDBOptions().tablePrefix(rdbTablePrefix).dropTablesOnClose(false));
        }
        ds1 = fixture.createDocumentStore();
        DocumentMK.Builder documentMKBuilder = new DocumentMK.Builder().clock(clock).setClusterId(1)
                .setLeaseCheckMode(LeaseCheckMode.DISABLED)
                .setDocumentStore(ds1).setAsyncDelay(0);
        store1 = documentMKBuilder.getNodeStore();
    }

    private void createSecondaryStore(LeaseCheckMode leaseCheckNode) {
        if (fixture instanceof RDBFixture) {
            ((RDBFixture) fixture).setRDBOptions(
                    new RDBOptions().tablePrefix(rdbTablePrefix).dropTablesOnClose(false));
        }
        ds2 = fixture.createDocumentStore();
        DocumentMK.Builder documentMKBuilder = new DocumentMK.Builder().clock(clock).setClusterId(2)
                .setLeaseCheckMode(leaseCheckNode)
                .setDocumentStore(ds2).setAsyncDelay(0);
        store2 = documentMKBuilder.getNodeStore();
    }

    @After
    public void tearDown() throws Exception {
        if (store2 != null) {
            store2.dispose();
        }
        if (store1 != null) {
            store1.dispose();
        }
        Revision.resetClockToDefault();
        execService.shutdown();
        execService.awaitTermination(1, MINUTES);
        fixture.dispose();
    }

    private static Set<Thread> tbefore = new HashSet<>();

    @BeforeClass
    public static void before() throws Exception {
        System.err.println("MULTI BEFORE");
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            tbefore.add(t);
            System.err.println(t);
        }
    }

    @AfterClass
    public static void after() throws Exception {
        System.err.println("MULTI AFTER");
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            System.err.println(t + (tbefore.contains(t) ? "" : " LEAKED"));
        }
    }

    /**
     * OAK-10542 with OAK-10526 : This reproduces a case where a _deleted revision
     * that is still used by a checkpoint is split away and then GCed. This variant
     * tests a checkpoint when /t/target is deleted.
     */
    @Test
    public void gcSplitDocWithReferencedDeleted_combined() throws Exception {

        createSecondaryStore(LeaseCheckMode.DISABLED);

        // step 1 : create a _delete entry with clusterId 2, plus do a GC
        createLeaf(store2, "t", "target");
        store2.runBackgroundOperations();
        assertEquals(0, store2.getVersionGarbageCollector().gc(24, HOURS).splitDocGCCount);

        // step 2 : nearly cause target docu split - via clusterId 1
        store1.runBackgroundOperations();
        for (int i = 0; i < (NUM_REVS_THRESHOLD / 2) - 1; i++) {
            deleteLeaf(store1, "t", "target");
            createLeaf(store1, "t", "target");
        }
        // last change should be deleted (that's what this test case is for)
        deleteLeaf(store1, "t", "target");
        store1.runBackgroundOperations();

        // step 3 : do a minimal sleep + bcOps between last change and the checkpoint to
        // ensure maxRev and checkpoint are more than precisionMs apart
        clock.waitUntil(clock.getTime() + TimeUnit.SECONDS.toMillis(61));
        store1.runBackgroundOperations();

        // step 4 : then take a checkpoint refering to the last rev in the split doc
        // (which is 'deleted')
        final String checkpoint = store1.checkpoint(TimeUnit.DAYS.toMillis(42));

        // step 5 : ensure another precisionMs apart between checkpoint and
        // split-triggering change
        clock.waitUntil(clock.getTime() + TimeUnit.SECONDS.toMillis(61));

        // step 6 : trigger the split - main doc will contain "_deleted"="false"
        createLeaf(store1, "t", "target");
        store1.runBackgroundOperations();

        // step 7 : wait for 25h - to also be more than 24 away from maxRev
        clock.waitUntil(clock.getTime() + TimeUnit.HOURS.toMillis(25));

        // step 8 : do the gc
        // expect a split doc at depth 4 for /t/target to exist
        assertEquals(1, store1.getDocumentStore()
                .query(NODES, "4:p/t/target/", "4:p/t/target/z", 5).size());
        gc.gc(24, HOURS);
        // before a fix the split doc is GCed (but can't make that an assert)
        //assertEquals(0, store.getDocumentStore()
        //        .query(NODES, "4:p/t/target/", "4:p/t/target/z", 5).size());

        // step 9 : make assertions about /t/target at root and checkpoint
        // invalidate node cache to ensure readNode/getNodeAtRevision is called below
        store1.getNodeCache().invalidateAll();
        assertTrue(store1.getRoot().getChildNode("t").getChildNode("target").exists());
        // invalidate node cache to ensure readNode/getNodeAtRevision is called below
        store1.getNodeCache().invalidateAll();
        assertEquals(false, store1.retrieve(checkpoint).getChildNode("t")
                .getChildNode("target").exists());
    }

    /**
     * OAK-10542 : This reproduces a case where a split doc is created that contains
     * a revision of _deleted that is still referred by a checkpoint. The fact that
     * _deleted is split "in the middle" used to confuse the getLiveRevision lookup,
     * as it was not considering split document for the _deleted property as long as
     * it found a valid revision in the main document. This variant tests a
     * checkpoint when /t/target is deleted.
     */
    @Test
    public void gcSplitDocWithReferencedDeleted_true() throws Exception {

        createSecondaryStore(LeaseCheckMode.DISABLED);

        // step 1 : create some _deleted entries with clusterId 2
        createLeaf(store2, "t", "target");
        deleteLeaf(store2, "t", "target");
        store2.runBackgroundOperations();

        // step 2 : create a _deleted=true entry with clusterId 1
        store1.runBackgroundOperations();
        createLeaf(store1, "t", "target");
        // create a checkpoint where /t/target should exist
        final String checkpoint = store1.checkpoint(TimeUnit.DAYS.toMillis(42));

        // step 3 : cause a split doc with _deleted with clusterId 1
        for (int i = 0; i < NUM_REVS_THRESHOLD; i++) {
            createLeaf(store1, "t", "target");
            deleteLeaf(store1, "t", "target");
        }
        store1.runBackgroundOperations();

        // step 4 : make assertions about /t/target at root and checkpoint
        // invalidate node cache to ensure readNode is called below
        store1.getNodeCache().invalidateAll();
        assertFalse(store1.getRoot().getChildNode("t").getChildNode("target").exists());
        // invalidate node cache to ensure readNode is called below
        store1.getNodeCache().invalidateAll();
        assertEquals(true, store1.retrieve(checkpoint).getChildNode("t")
                .getChildNode("target").exists());

    }

    /**
     * OAK-10542 : This reproduces a case where a split doc is created that contains
     * a revision of _deleted that is still referred by a checkpoint. The fact that
     * _deleted is split "in the middle" used to confuse the getLiveRevision lookup,
     * as it was not considering split document for the _deleted property as long as
     * it found a valid revision in the main document. This variant tests a
     * checkpoint when /t/target exists.
     */
    @Test
    public void gcSplitDocWithReferencedDeleted_false() throws Exception {

        createSecondaryStore(LeaseCheckMode.DISABLED);

        // step 1 : create a _delete entry with clusterId 2
        createLeaf(store2, "t", "target");
        store2.runBackgroundOperations();

        // step 2 : create a _deleted=true entry with clusterId 1
        store1.runBackgroundOperations();
        deleteLeaf(store1, "t", "target");
        // create a checkpoint where /t/target should not exist
        final String checkpoint = store1.checkpoint(TimeUnit.DAYS.toMillis(42));

        // step 2 : cause a split doc with _deleted with clusterId 1
        for (int i = 0; i < NUM_REVS_THRESHOLD; i++) {
            createLeaf(store1, "t", "target");
            deleteLeaf(store1, "t", "target");
        }
        store1.runBackgroundOperations();

        // step 4 : make assertions about /t/target at root and checkpoint
        // invalidate node cache to ensure readNode/getNodeAtRevision is called below
        store1.getNodeCache().invalidateAll();
        assertFalse(store1.getRoot().getChildNode("t").getChildNode("target").exists());
        // invalidate node cache to ensure readNode/getNodeAtRevision is called below
        store1.getNodeCache().invalidateAll();
        assertEquals(false, store1.retrieve(checkpoint).getChildNode("t")
                .getChildNode("target").exists());

    }

    private void createLeaf(DocumentNodeStore s, String... pathElems) throws Exception {
        createOrDeleteLeaf(s, false, pathElems);
    }

    private void deleteLeaf(DocumentNodeStore s, String... pathElems) throws Exception {
        createOrDeleteLeaf(s, true, pathElems);
    }

    private void createOrDeleteLeaf(DocumentNodeStore s, boolean delete,
            String... pathElems) throws Exception {
        clock.waitUntil(clock.getTime() + TimeUnit.SECONDS.toMillis(10));
        final NodeBuilder rb = s.getRoot().builder();
        NodeBuilder b = rb;
        for (String pathElem : pathElems) {
            b = b.child(pathElem);
        }
        if (delete) {
            b.remove();
        }
        s.merge(rb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    /**
     * OAK-10526 : This reproduces a case where a split doc is created then GCed,
     * while there is a checkpoint that still refers to a revision contained in that
     * split doc.
     */
    @Test
    public void gcSplitDocsWithReferencedRevisions() throws Exception {
        final String exp;

        // step 1 : create an old revision at t(0) with custerId 2
        createSecondaryStore(LeaseCheckMode.DISABLED);

        NodeBuilder b1 = store2.getRoot().builder();
        b1.child("t").setProperty("foo", "some-value-created-by-another-cluster-node");
        store2.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store2.runBackgroundOperations();
        store1.runBackgroundOperations();

        // step 2 : make sure GC was running once and sets oldest timestamp
        // (the value of oldest doesn't matter, but it should be <= t(0))
        assertEquals(0, gc.gc(24, HOURS).splitDocGCCount);

        // step 3 : wait for 1 week
        clock.waitUntil(clock.getTime() + TimeUnit.DAYS.toMillis(7));

        // step 4 : create old revisions at t(+1w) - without yet causing a split
        String lastValue = null;
        for (int i = 0; i < NUM_REVS_THRESHOLD - 1; i++) {
            b1 = store1.getRoot().builder();
            b1.child("t").setProperty("foo", lastValue = "bar" + i);
            store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }
        exp = lastValue;
        store1.runBackgroundOperations();

        // step 4b : another change to further lastRev for clusterId 1
        // required to ensure 5sec rounding of mongo variant is also covered
        clock.waitUntil(clock.getTime() + TimeUnit.SECONDS.toMillis(6));
        b1 = store1.getRoot().builder();
        b1.child("unrelated").setProperty("unrelated", "unrelated");
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // step 5 : create a checkpoint at t(+1w+6sec)
        String checkpoint = store1.checkpoint(TimeUnit.DAYS.toMillis(42));
        assertEquals(exp, store1.getRoot().getChildNode("t").getString("foo"));
        assertEquals(exp, store1.retrieve(checkpoint).getChildNode("t").getString("foo"));

        // step 6 : wait for 1 week
        clock.waitUntil(clock.getTime() + TimeUnit.DAYS.toMillis(7));

        // step 7 : do another change that fulfills the split doc condition at t(+2w)
        b1 = store1.getRoot().builder();
        b1.child("t").setProperty("foo", "barZ");
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();
        assertEquals("barZ", store1.getRoot().getChildNode("t").getString("foo"));
        assertEquals(exp, store1.retrieve(checkpoint).getChildNode("t").getString("foo"));

        // step 8 : move the clock a couple seconds to ensure GC maxRev condition hits
        // (without this it might not yet GC the split doc we want it to,
        // as we'd be in the same rounded second) -> t(+2w:30s)
        clock.waitUntil(clock.getTime() + TimeUnit.SECONDS.toMillis(30));

        // step 9 : trigger another GC - previously split away the referenced revision
        assertEquals(0, gc.gc(24, HOURS).splitDocGCCount);
        // flush the caches as otherwise it might deliver stale data
        store1.getNodeCache().invalidateAll();
        assertEquals("barZ", store1.getRoot().getChildNode("t").getString("foo"));
        assertEquals(exp, store1.retrieve(checkpoint).getChildNode("t").getString("foo"));
    }


    @Test
    public void invalidateCacheOnMissingPreviousDocument() throws Exception {
        assumeTrue(fixture.hasSinglePersistence());

        DocumentStore ds = store1.getDocumentStore();
        NodeBuilder builder = store1.getRoot().builder();
        builder.child("foo");
        store1.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        for (int i = 0; i < 60; i++) {
            builder = store1.getRoot().builder();
            builder.child("foo").setProperty("p", i);
            merge(store1, builder);
            RevisionVector head = store1.getHeadRevision();
            for (UpdateOp op : SplitOperations.forDocument(
                    ds.find(NODES, Utils.getIdFromPath("/foo")), store1, head,
                    NO_BINARY, 2)) {
                ds.createOrUpdate(NODES, op);
            }
            clock.waitUntil(clock.getTime() + TimeUnit.MINUTES.toMillis(1));
        }
        store1.runBackgroundOperations();
        NodeDocument foo = ds.find(NODES, Utils.getIdFromPath("/foo"));
        assertNotNull(foo);
        Long modCount = foo.getModCount();
        assertNotNull(modCount);
        List<String> prevIds = Lists.newArrayList(Iterators.transform(
                foo.getPreviousDocLeaves(), new Function<NodeDocument, String>() {
            @Override
            public String apply(NodeDocument input) {
                return input.getId();
            }
        }));

        // run gc on another document node store
        createSecondaryStore(LeaseCheckMode.LENIENT);

        try {
            VersionGarbageCollector gc = store2.getVersionGarbageCollector();
            // collect about half of the changes
            gc.gc(30, TimeUnit.MINUTES);
        } finally {
            store2.dispose();
        }
        // evict prev docs from cache and force DocumentStore
        // to check with storage again
        for (String id : prevIds) {
            ds.invalidateCache(NODES, id);
        }

        foo = ds.find(NODES, Utils.getIdFromPath("/foo"));
        assertNotNull(foo);
        Iterators.size(foo.getAllPreviousDocs());

        // foo must now reflect state after GC
        foo = ds.find(NODES, Utils.getIdFromPath("/foo"));
        assertNotEquals(modCount, foo.getModCount());
    }

    @Test
    public void gcOnStaleDocument() throws Exception {
        assumeTrue(fixture.hasSinglePersistence());

        String nodeName = "foo";
        Path path = new Path(Path.ROOT, nodeName);
        String docId = Utils.getIdFromPath(path);

        NodeBuilder builder = store1.getRoot().builder();
        builder.child(nodeName).setProperty("p", -1);
        merge(store1, builder);

        store1.runBackgroundOperations();

        for (int i = 0; i < NUM_REVS_THRESHOLD - 1; i++) {
            builder = store1.getRoot().builder();
            builder.child(nodeName).setProperty("p", i);
            merge(store1, builder);
        }

        createSecondaryStore(LeaseCheckMode.LENIENT);

        VersionGarbageCollector gc = store2.getVersionGarbageCollector();
        gc.gc(30, MINUTES);

        CountDownLatch bgOperationsDone = new CountDownLatch(1);
        // prepare commit that will trigger split
        Commit c = store1.newCommit(cb -> cb.updateProperty(path, "p", "0"),
                store1.getHeadRevision(), null);
        try {
            execService.submit(() -> {
                store1.runBackgroundOperations();
                bgOperationsDone.countDown();
            });
            // give the background operations some time to progress
            // past the check for split operations
            Thread.sleep(50);
            c.apply();
        } finally {
            store1.done(c, false, CommitInfo.EMPTY);
            store1.addSplitCandidate(docId);
        }

        // pick up the changes performed by first store
        bgOperationsDone.await();
        store2.runBackgroundOperations();

        // read the node /foo from the store that will perform the
        // revision garbage collection
        NodeState state = store2.getRoot().getChildNode(nodeName);
        assertTrue(state.exists());
        PropertyState prop = state.getProperty("p");
        assertNotNull(prop);
        assertEquals(0L, prop.getValue(Type.LONG).longValue());
        // must have the corresponding document in the cache now
        NodeDocument doc = ds2.getIfCached(NODES, docId);
        assertNotNull(doc);
        // must not yet have previous documents
        assertTrue(doc.getPreviousRanges().isEmpty());

        // write something else. this will ensure a journal entry is
        // pushed on the next background update operation
        builder = store1.getRoot().builder();
        builder.child("bar");
        merge(store1, builder);

        // trigger the overdue split on 1:/foo
        store1.runBackgroundOperations();
        store2.runBackgroundOperations();

        // wait some time and trigger RGC
        clock.waitUntil(clock.getTime() + HOURS.toMillis(1));

        gc = store2.getVersionGarbageCollector();
        VersionGCStats stats = gc.gc(30, MINUTES);
        assertEquals(1, stats.splitDocGCCount);

        // check how the document looks like, bypassing cache
        doc = store1.getDocumentStore().find(NODES, docId, 0);
        assertNotNull(doc);
        assertTrue(doc.getPreviousRanges().isEmpty());
    }

    private void merge(DocumentNodeStore store, NodeBuilder builder)
            throws CommitFailedException {
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }
}
