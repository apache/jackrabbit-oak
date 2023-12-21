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

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.jackrabbit.guava.common.collect.Iterables.filter;
import static org.apache.jackrabbit.guava.common.collect.Iterables.size;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.NUM_REVS_THRESHOLD;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.PREV_SPLIT_FACTOR;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.NO_BINARY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import org.apache.jackrabbit.guava.common.base.Function;
import org.apache.jackrabbit.guava.common.base.Predicate;
import org.apache.jackrabbit.guava.common.base.Strings;
import org.apache.jackrabbit.guava.common.collect.AbstractIterator;
import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.guava.common.collect.Queues;
import org.apache.jackrabbit.guava.common.collect.Sets;
import org.apache.jackrabbit.guava.common.util.concurrent.Atomics;
import com.mongodb.ReadPreference;

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
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.Clock;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class VersionGarbageCollectorIT {

    private DocumentStoreFixture fixture;

    private Clock clock;

    private DocumentMK.Builder documentMKBuilder;

    private DocumentStore ds1, ds2;

    private DocumentNodeStore store1, store2;

    private VersionGarbageCollector gc;

    private ExecutorService execService;

    public VersionGarbageCollectorIT(DocumentStoreFixture fixture) {
        this.fixture = fixture;
    }

    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> fixtures() throws IOException {
        return AbstractDocumentStoreTest.fixtures();
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

    private String rdbTablePrefix = "T" + Long.toHexString(System.currentTimeMillis());

    private void createPrimaryStore() {
        if (fixture instanceof RDBFixture) {
            ((RDBFixture) fixture).setRDBOptions(
                    new RDBOptions().tablePrefix(rdbTablePrefix).dropTablesOnClose(true));
        }
        ds1 = fixture.createDocumentStore();
        documentMKBuilder = new DocumentMK.Builder().clock(clock).setClusterId(1)
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
        DocumentMK.Builder documentMKBuilder2 = new DocumentMK.Builder().clock(clock).setClusterId(2)
                .setLeaseCheckMode(leaseCheckNode)
                .setDocumentStore(ds2).setAsyncDelay(0);
        store2 = documentMKBuilder2.getNodeStore();
    }

    private static Set<Thread> tbefore = new HashSet<>();

    @BeforeClass
    public static void before() throws Exception {
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            tbefore.add(t);
        }
    }

    @AfterClass
    public static void after() throws Exception {
        for (Thread t : Thread.getAllStackTraces().keySet()) {
            if (!tbefore.contains(t)) {
                System.err.println("potentially leaked thread: " + t);
            }
        }
    }

    @Test
    public void gcIgnoredForCheckpoint() throws Exception {
        long expiryTime = 100, maxAge = 20;

        Revision cp = Revision.fromString(store1.checkpoint(expiryTime));

        //Fast forward time to future but before expiry of checkpoint
        clock.waitUntil(cp.getTimestamp() + expiryTime - maxAge);
        VersionGCStats stats = gc.gc(maxAge, TimeUnit.MILLISECONDS);
        assertTrue(stats.ignoredGCDueToCheckPoint);

        //Fast forward time to future such that checkpoint get expired
        clock.waitUntil(clock.getTime() + expiryTime + 1);
        stats = gc.gc(maxAge, TimeUnit.MILLISECONDS);
        assertFalse("GC should be performed", stats.ignoredGCDueToCheckPoint);
    }

    @Test
    public void testGCDeletedDocument() throws Exception{
        //1. Create nodes
        NodeBuilder b1 = store1.getRoot().builder();
        b1.child("x").child("y");
        b1.child("z");
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        long maxAge = 1; //hours
        long delta = TimeUnit.MINUTES.toMillis(10);
        //1. Go past GC age and check no GC done as nothing deleted
        clock.waitUntil(Revision.getCurrentTimestamp() + maxAge);
        VersionGCStats stats = gc.gc(maxAge, HOURS);
        assertEquals(0, stats.deletedDocGCCount);

        //Remove x/y
        NodeBuilder b2 = store1.getRoot().builder();
        b2.child("x").child("y").remove();
        store1.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        store1.runBackgroundOperations();

        //2. Check that a deleted doc is not collected before
        //maxAge
        //Clock cannot move back (it moved forward in #1) so double the maxAge
        clock.waitUntil(clock.getTime() + delta);
        stats = gc.gc(maxAge*2, HOURS);
        assertEquals(0, stats.deletedDocGCCount);
        assertEquals(0, stats.deletedLeafDocGCCount);

        //3. Check that deleted doc does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);

        stats = gc.gc(maxAge*2, HOURS);
        assertEquals(1, stats.deletedDocGCCount);
        assertEquals(1, stats.deletedLeafDocGCCount);

        //4. Check that a revived doc (deleted and created again) does not get gc
        NodeBuilder b3 = store1.getRoot().builder();
        b3.child("z").remove();
        store1.merge(b3, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeBuilder b4 = store1.getRoot().builder();
        b4.child("z");
        store1.merge(b4, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);
        stats = gc.gc(maxAge*2, HOURS);
        assertEquals(0, stats.deletedDocGCCount);
        assertEquals(0, stats.deletedLeafDocGCCount);
        assertEquals(1, stats.updateResurrectedGCCount);
    }

    @Test
    public void gcSplitDocs() throws Exception {
        gcSplitDocsInternal("foo");
    }

    @Test
    public void gcLongPathSplitDocs() throws Exception {
        gcSplitDocsInternal(Strings.repeat("sub", 120));
    }

    private void gcSplitDocsInternal(String subNodeName) throws Exception {
        long maxAge = 1; //hrs
        long delta = TimeUnit.MINUTES.toMillis(10);

        NodeBuilder b1 = store1.getRoot().builder();
        b1.child("test").child(subNodeName).child("bar");
        b1.child("test2").child(subNodeName);
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //Commit on a node which has a child and where the commit root
        // is parent
        for (int i = 0; i < NUM_REVS_THRESHOLD; i++) {
            b1 = store1.getRoot().builder();
            //This updates a middle node i.e. one which has child bar
            //Should result in SplitDoc of type PROP_COMMIT_ONLY
            b1.child("test").child(subNodeName).setProperty("prop",i);

            //This should result in SplitDoc of type DEFAULT_NO_CHILD
            b1.child("test2").child(subNodeName).setProperty("prop", i);
            store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }
        store1.runBackgroundOperations();

        // perform a change to make sure the sweep rev will be newer than
        // the split revs, otherwise revision GC won't remove the split doc
        clock.waitUntil(clock.getTime() + TimeUnit.SECONDS.toMillis(NodeDocument.MODIFIED_IN_SECS_RESOLUTION * 2));
        NodeBuilder builder = store1.getRoot().builder();
        builder.child("qux");
        merge(store1, builder);
        store1.runBackgroundOperations();

        List<NodeDocument> previousDocTestFoo =
                ImmutableList.copyOf(getDoc("/test/" + subNodeName).getAllPreviousDocs());
        List<NodeDocument> previousDocTestFoo2 =
                ImmutableList.copyOf(getDoc("/test2/" + subNodeName).getAllPreviousDocs());
        List<NodeDocument> previousDocRoot =
                ImmutableList.copyOf(getDoc("/").getAllPreviousDocs());

        assertEquals(1, previousDocTestFoo.size());
        assertEquals(1, previousDocTestFoo2.size());
        assertEquals(1, previousDocRoot.size());

        assertEquals(SplitDocType.COMMIT_ROOT_ONLY, previousDocTestFoo.get(0).getSplitDocType());
        assertEquals(SplitDocType.DEFAULT_LEAF, previousDocTestFoo2.get(0).getSplitDocType());
        assertEquals(SplitDocType.DEFAULT_NO_BRANCH, previousDocRoot.get(0).getSplitDocType());

        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge) + delta);
        VersionGCStats stats = gc.gc(maxAge, HOURS);
        assertEquals(3, stats.splitDocGCCount);
        assertEquals(0, stats.deletedLeafDocGCCount);

        //Previous doc should be removed
        assertNull(getDoc(previousDocTestFoo.get(0).getPath()));
        assertNull(getDoc(previousDocTestFoo2.get(0).getPath()));
        assertNull(getDoc(previousDocRoot.get(0).getPath()));

        //Following would not work for Mongo as the delete happened on the server side
        //And entries from cache are not evicted
        //assertTrue(ImmutableList.copyOf(getDoc("/test2/foo").getAllPreviousDocs()).isEmpty());
    }

    /**
     * OAK-10542 with OAK-10526 : This reproduces a case where a _deleted revision
     * that is still used by a checkpoint is split away and then GCed. This variant
     * tests a checkpoint when /t/target is deleted.
     */
    @Test
    public void gcSplitDocWithReferencedDeleted_combined() throws Exception {

        assumeTrue(fixture.hasSinglePersistence());
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

        assumeTrue(fixture.hasSinglePersistence());
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

        assumeTrue(fixture.hasSinglePersistence());
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

        assumeTrue(fixture.hasSinglePersistence());
        createSecondaryStore(LeaseCheckMode.DISABLED);

        // step 1 : create an old revision at t(0) with custerId 2
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

        // step 5 : create a checkpoint (valid for 42 days) at t(+1w+6sec)
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

    // OAK-1729
    @Test
    public void gcIntermediateDocs() throws Exception {
        long maxAge = 1; //hrs
        long delta = TimeUnit.MINUTES.toMillis(10);

        NodeBuilder b1 = store1.getRoot().builder();
        // adding the test node will cause the commit root to be placed
        // on the root document, because the children flag is set on the
        // root document
        b1.child("test");
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        assertTrue(getDoc("/test").getLocalRevisions().isEmpty());
        // setting the test property afterwards will use the new test document
        // as the commit root. this what we want for the test.
        b1 = store1.getRoot().builder();
        b1.child("test").setProperty("test", "value");
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        assertTrue(!getDoc("/test").getLocalRevisions().isEmpty());

        for (int i = 0; i < PREV_SPLIT_FACTOR; i++) {
            for (int j = 0; j < NUM_REVS_THRESHOLD; j++) {
                b1 = store1.getRoot().builder();
                b1.child("test").setProperty("prop", i * NUM_REVS_THRESHOLD + j);
                store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            }
            store1.runBackgroundOperations();
        }
        // trigger another split, now that we have 10 previous docs
        // this will create an intermediate previous doc
        store1.addSplitCandidate(Utils.getIdFromPath("/test"));
        store1.runBackgroundOperations();

        Map<Revision, Range> prevRanges = getDoc("/test").getPreviousRanges();
        boolean hasIntermediateDoc = false;
        for (Map.Entry<Revision, Range> entry : prevRanges.entrySet()) {
            if (entry.getValue().getHeight() > 0) {
                hasIntermediateDoc = true;
                break;
            }
        }
        assertTrue("Test data does not have intermediate previous docs",
                hasIntermediateDoc);

        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge) + delta);
        VersionGCStats stats = gc.gc(maxAge, HOURS);
        assertEquals(10, stats.splitDocGCCount);
        assertEquals(0, stats.deletedLeafDocGCCount);

        DocumentNodeState test = getDoc("/test").getNodeAtRevision(
                store1, store1.getHeadRevision(), null);
        assertNotNull(test);
        assertTrue(test.hasProperty("test"));
    }

    // OAK-1779
    @Test
    public void cacheConsistency() throws Exception {
        long maxAge = 1; //hrs
        long delta = TimeUnit.MINUTES.toMillis(10);

        Set<String> names = Sets.newHashSet();
        NodeBuilder b1 = store1.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            String name = "test-" + i;
            b1.child(name);
            names.add(name);
        }
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        for (ChildNodeEntry entry : store1.getRoot().getChildNodeEntries()) {
            entry.getNodeState();
        }

        b1 = store1.getRoot().builder();
        b1.getChildNode("test-7").remove();
        names.remove("test-7");
    
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge) + delta);

        VersionGCStats stats = gc.gc(maxAge, HOURS);
        assertEquals(1, stats.deletedDocGCCount);
        assertEquals(1, stats.deletedLeafDocGCCount);

        Set<String> children = Sets.newHashSet();
        for (ChildNodeEntry entry : store1.getRoot().getChildNodeEntries()) {
            children.add(entry.getName());
        }
        assertEquals(names, children);
    }

    // OAK-1793
    @Test
    public void gcPrevWithMostRecentModification() throws Exception {
        long maxAge = 1; //hrs
        long delta = TimeUnit.MINUTES.toMillis(10);

        for (int i = 0; i < NUM_REVS_THRESHOLD + 1; i++) {
            NodeBuilder builder = store1.getRoot().builder();
            builder.child("foo").setProperty("prop", "v" + i);
            builder.child("bar").setProperty("prop", "v" + i);
            store1.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }

        store1.runBackgroundOperations();

        // perform a change to make sure the sweep rev will be newer than
        // the split revs, otherwise revision GC won't remove the split doc
        clock.waitUntil(clock.getTime() + TimeUnit.SECONDS.toMillis(NodeDocument.MODIFIED_IN_SECS_RESOLUTION * 2));
        NodeBuilder builder = store1.getRoot().builder();
        builder.child("qux");
        merge(store1, builder);
        store1.runBackgroundOperations();

        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge) + delta);

        VersionGCStats stats = gc.gc(maxAge, HOURS);
        // one split doc each on: /foo, /bar and root document
        assertEquals(3, stats.splitDocGCCount);
        assertEquals(0, stats.deletedLeafDocGCCount);

        NodeDocument doc = getDoc("/foo");
        assertNotNull(doc);
        DocumentNodeState state = doc.getNodeAtRevision(
                store1, store1.getHeadRevision(), null);
        assertNotNull(state);
    }

    // OAK-1791
    @Test
    public void gcDefaultLeafSplitDocs() throws Exception {
        Revision.setClock(clock);

        NodeBuilder builder = store1.getRoot().builder();
        builder.child("test").setProperty("prop", -1);
        merge(store1, builder);

        String id = Utils.getIdFromPath("/test");
        long start = Revision.getCurrentTimestamp();
        // simulate continuous writes once a second for one day
        // collect garbage older than one hour
        int hours = 24;
        if (fixture instanceof DocumentStoreFixture.MongoFixture) {
            // only run for 6 hours on MongoDB to
            // keep time to run on a reasonable level
            hours = 6;
        }
        for (int i = 0; i < 3600 * hours; i++) {
            clock.waitUntil(start + i * 1000);
            builder = store1.getRoot().builder();
            builder.child("test").setProperty("prop", i);
            merge(store1, builder);
            if (i % 10 == 0) {
                store1.runBackgroundOperations();
            }
            // trigger GC twice an hour
            if (i % 1800 == 0) {
                gc.gc(1, HOURS);
                NodeDocument doc = store1.getDocumentStore().find(NODES, id);
                assertNotNull(doc);
                int numPrevDocs = Iterators.size(doc.getAllPreviousDocs());
                assertTrue("too many previous docs: " + numPrevDocs,
                        numPrevDocs < 70);
            }
        }
        NodeDocument doc = store1.getDocumentStore().find(NODES, id);
        assertNotNull(doc);
        int numRevs = size(doc.getValueMap("prop").entrySet());
        assertTrue("too many revisions: " + numRevs, numRevs < 6000);
    }

    // OAK-2778
    @Test
    public void gcWithConcurrentModification() throws Exception {
        Revision.setClock(clock);
        DocumentStore ds = store1.getDocumentStore();

        // create test content
        createTestNode("foo");
        createTestNode("bar");

        // remove again
        NodeBuilder builder = store1.getRoot().builder();
        builder.getChildNode("foo").remove();
        builder.getChildNode("bar").remove();
        merge(store1, builder);

        // wait one hour
        clock.waitUntil(clock.getTime() + HOURS.toMillis(1));

        final BlockingQueue<NodeDocument> docs = Queues.newSynchronousQueue();
        VersionGCSupport gcSupport = new VersionGCSupport(store1.getDocumentStore()) {
            @Override
            public Iterable<NodeDocument> getPossiblyDeletedDocs(long fromModified, long toModified) {
                return filter(super.getPossiblyDeletedDocs(fromModified, toModified),
                        new Predicate<NodeDocument>() {
                            @Override
                            public boolean apply(NodeDocument input) {
                                try {
                                    docs.put(input);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                                return true;
                            }
                        });
            }
        };
        final VersionGarbageCollector gc = new VersionGarbageCollector(store1, gcSupport);
        // start GC -> will try to remove /foo and /bar
        Future<VersionGCStats> f = execService.submit(new Callable<VersionGCStats>() {
            @Override
            public VersionGCStats call() throws Exception {
                return gc.gc(30, MINUTES);
            }
        });

        NodeDocument doc = docs.take();
        String name = doc.getPath().getName();
        // recreate node, which hasn't been removed yet
        name = name.equals("foo") ? "bar" : "foo";
        builder = store1.getRoot().builder();
        builder.child(name);
        merge(store1, builder);

        // loop over child node entries -> will populate nodeChildrenCache
        for (ChildNodeEntry cne : store1.getRoot().getChildNodeEntries()) {
            cne.getName();
        }
        // invalidate cached DocumentNodeState
        DocumentNodeState state = (DocumentNodeState) store1.getRoot().getChildNode(name);
        store1.invalidateNodeCache(state.getPath().toString(), store1.getRoot().getLastRevision());

        while (!f.isDone()) {
            docs.poll();
        }

        // read children again after GC finished
        List<String> names = Lists.newArrayList();
        for (ChildNodeEntry cne : store1.getRoot().getChildNodeEntries()) {
            names.add(cne.getName());
        }
        assertEquals(1, names.size());

        doc = ds.find(NODES, Utils.getIdFromPath("/" + names.get(0)));
        assertNotNull(doc);
        assertEquals(0, Iterators.size(doc.getAllPreviousDocs()));

        VersionGCStats stats = f.get();
        assertEquals(1, stats.deletedDocGCCount);
        assertEquals(2, stats.splitDocGCCount);
        assertEquals(0, stats.deletedLeafDocGCCount);
    }

    // OAK-4819
    @Test
    public void malformedId() throws Exception {
        long maxAge = 1; //hrs
        long delta = TimeUnit.MINUTES.toMillis(10);

        NodeBuilder builder = store1.getRoot().builder();
        builder.child("foo");
        store1.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        // remove again
        builder = store1.getRoot().builder();
        builder.child("foo").remove();
        store1.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        store1.runBackgroundOperations();

        // add a document with a malformed id
        String id = "42";
        UpdateOp op = new UpdateOp(id, true);
        NodeDocument.setDeletedOnce(op);
        NodeDocument.setModified(op, store1.newRevision());
        store1.getDocumentStore().create(NODES, Lists.newArrayList(op));

        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge) + delta);

        // gc must not fail
        VersionGCStats stats = gc.gc(maxAge, HOURS);
        assertEquals(1, stats.deletedDocGCCount);
        assertEquals(1, stats.deletedLeafDocGCCount);
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

        VersionGarbageCollector gc = store2.getVersionGarbageCollector();
        // collect about half of the changes
        gc.gc(30, TimeUnit.MINUTES);

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
    public void cancelGCBeforeFirstPhase() throws Exception {
        createTestNode("foo");

        NodeBuilder builder = store1.getRoot().builder();
        builder.child("foo").child("bar");
        merge(store1, builder);

        builder = store1.getRoot().builder();
        builder.child("foo").remove();
        merge(store1, builder);
        store1.runBackgroundOperations();

        clock.waitUntil(clock.getTime() + TimeUnit.HOURS.toMillis(1));

        final AtomicReference<VersionGarbageCollector> gcRef = Atomics.newReference();
        VersionGCSupport gcSupport = new VersionGCSupport(store1.getDocumentStore()) {
            @Override
            public Iterable<NodeDocument> getPossiblyDeletedDocs(long fromModified, long toModified) {
                // cancel as soon as it runs
                gcRef.get().cancel();
                return super.getPossiblyDeletedDocs(fromModified, toModified);
            }
        };
        gcRef.set(new VersionGarbageCollector(store1, gcSupport));
        VersionGCStats stats = gcRef.get().gc(30, TimeUnit.MINUTES);
        assertTrue(stats.canceled);
        assertEquals(0, stats.deletedDocGCCount);
        assertEquals(0, stats.deletedLeafDocGCCount);
        assertEquals(0, stats.intermediateSplitDocGCCount);
        assertEquals(0, stats.splitDocGCCount);
    }

    @Test
    public void cancelGCAfterFirstPhase() throws Exception {
        createTestNode("foo");

        NodeBuilder builder = store1.getRoot().builder();
        builder.child("foo").child("bar");
        merge(store1, builder);

        builder = store1.getRoot().builder();
        builder.child("foo").remove();
        merge(store1, builder);
        store1.runBackgroundOperations();

        clock.waitUntil(clock.getTime() + TimeUnit.HOURS.toMillis(1));

        final AtomicReference<VersionGarbageCollector> gcRef = Atomics.newReference();
        VersionGCSupport gcSupport = new VersionGCSupport(store1.getDocumentStore()) {
            @Override
            public Iterable<NodeDocument> getPossiblyDeletedDocs(final long fromModified, final long toModified) {
                return new Iterable<NodeDocument>() {
                    @NotNull
                    @Override
                    public Iterator<NodeDocument> iterator() {
                        return new AbstractIterator<NodeDocument>() {
                            private Iterator<NodeDocument> it = candidates(fromModified, toModified);
                            @Override
                            protected NodeDocument computeNext() {
                                if (it.hasNext()) {
                                    return it.next();
                                }
                                // cancel when we reach the end
                                gcRef.get().cancel();
                                return endOfData();
                            }
                        };
                    }
                };
            }

            private Iterator<NodeDocument> candidates(long prevLastModifiedTime, long lastModifiedTime) {
                return super.getPossiblyDeletedDocs(prevLastModifiedTime, lastModifiedTime).iterator();
            }
        };
        gcRef.set(new VersionGarbageCollector(store1, gcSupport));
        VersionGCStats stats = gcRef.get().gc(30, TimeUnit.MINUTES);
        assertTrue(stats.canceled);
        assertEquals(0, stats.deletedDocGCCount);
        assertEquals(0, stats.deletedLeafDocGCCount);
        assertEquals(0, stats.intermediateSplitDocGCCount);
        assertEquals(0, stats.splitDocGCCount);
    }

    // OAK-3070
    @Test
    public void lowerBoundOfModifiedDocs() throws Exception {
        Revision.setClock(clock);
        final VersionGCSupport fixtureGCSupport = documentMKBuilder.createVersionGCSupport();
        final AtomicInteger docCounter = new AtomicInteger();
        VersionGCSupport nonReportingGcSupport = new VersionGCSupport(store1.getDocumentStore()) {
            @Override
            public Iterable<NodeDocument> getPossiblyDeletedDocs(final long fromModified, long toModified) {
                return filter(fixtureGCSupport.getPossiblyDeletedDocs(fromModified, toModified),
                        new Predicate<NodeDocument>() {
                            @Override
                            public boolean apply(NodeDocument input) {
                                docCounter.incrementAndGet();
                                return false;// don't report any doc to be
                                             // GC'able
                            }
                        });
            }
        };
        final VersionGarbageCollector gc = new VersionGarbageCollector(store1, nonReportingGcSupport);
        final long maxAgeHours = 1;
        final long clockDelta = HOURS.toMillis(maxAgeHours) + MINUTES.toMillis(5);

        // create and delete node
        NodeBuilder builder = store1.getRoot().builder();
        builder.child("foo1");
        merge(store1, builder);
        builder = store1.getRoot().builder();
        builder.getChildNode("foo1").remove();
        merge(store1, builder);
        store1.runBackgroundOperations();

        clock.waitUntil(clock.getTime() + clockDelta);
        gc.gc(maxAgeHours, HOURS);
        assertEquals("Not all deletable docs got reported on first run", 1, docCounter.get());

        docCounter.set(0);
        // create and delete another node
        builder = store1.getRoot().builder();
        builder.child("foo2");
        merge(store1, builder);
        builder = store1.getRoot().builder();
        builder.getChildNode("foo2").remove();
        merge(store1, builder);
        store1.runBackgroundOperations();

        // wait another hour and GC in last 1 hour
        clock.waitUntil(clock.getTime() + clockDelta);
        gc.gc(maxAgeHours, HOURS);
        assertEquals(1, docCounter.get());
    }

    @Test
    public void gcDefaultNoBranchSplitDoc() throws Exception {
        long maxAge = 1; //hrs
        long delta = TimeUnit.MINUTES.toMillis(10);

        NodeBuilder builder = store1.getRoot().builder();
        builder.child("foo").child("bar");
        merge(store1, builder);
        String value = "";
        for (int i = 0; i < NUM_REVS_THRESHOLD; i++) {
            builder = store1.getRoot().builder();
            value = "v" + i;
            builder.child("foo").setProperty("prop", value);
            merge(store1, builder);
        }
        store1.runBackgroundOperations();

        // perform a change to make sure the sweep rev will be newer than
        // the split revs, otherwise revision GC won't remove the split doc
        clock.waitUntil(clock.getTime() + TimeUnit.SECONDS.toMillis(NodeDocument.MODIFIED_IN_SECS_RESOLUTION * 2));
        builder = store1.getRoot().builder();
        builder.child("qux");
        merge(store1, builder);
        store1.runBackgroundOperations();

        NodeDocument doc = getDoc("/foo");
        assertNotNull(doc);
        List<NodeDocument> prevDocs = ImmutableList.copyOf(doc.getAllPreviousDocs());
        assertEquals(1, prevDocs.size());
        assertEquals(SplitDocType.DEFAULT_NO_BRANCH, prevDocs.get(0).getSplitDocType());

        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge) + delta);

        VersionGCStats stats = gc.gc(maxAge, HOURS);
        assertEquals(1, stats.splitDocGCCount);

        doc = getDoc("/foo");
        assertNotNull(doc);
        prevDocs = ImmutableList.copyOf(doc.getAllPreviousDocs());
        assertEquals(0, prevDocs.size());

        assertEquals(value, store1.getRoot().getChildNode("foo").getString("prop"));
    }

    @Test
    public void gcWithOldSweepRev() throws Exception {
        long maxAge = 1; //hrs
        long delta = TimeUnit.MINUTES.toMillis(10);

        NodeBuilder builder = store1.getRoot().builder();
        builder.child("foo").child("bar");
        merge(store1, builder);
        String value = "";
        for (int i = 0; i < NUM_REVS_THRESHOLD; i++) {
            builder = store1.getRoot().builder();
            value = "v" + i;
            builder.child("foo").setProperty("prop", value);
            merge(store1, builder);
        }

        // trigger split of /foo
        store1.runBackgroundUpdateOperations();

        // now /foo must have previous docs
        NodeDocument doc = getDoc("/foo");
        List<NodeDocument> prevDocs = ImmutableList.copyOf(doc.getAllPreviousDocs());
        assertEquals(1, prevDocs.size());
        assertEquals(SplitDocType.DEFAULT_NO_BRANCH, prevDocs.get(0).getSplitDocType());

        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge) + delta);

        // revision gc must not collect previous doc because sweep did not run
        VersionGCStats stats = gc.gc(maxAge, HOURS);
        assertEquals(0, stats.splitDocGCCount);

        // write something to make sure sweep rev is after the split revs
        // otherwise GC won't collect the split doc
        builder = store1.getRoot().builder();
        builder.child("qux");
        merge(store1, builder);

        // run full background operations with sweep
        clock.waitUntil(clock.getTime() + TimeUnit.SECONDS.toMillis(NodeDocument.MODIFIED_IN_SECS_RESOLUTION * 2));
        store1.runBackgroundOperations();

        // now sweep rev must be updated and revision GC can collect prev doc
        stats = gc.gc(maxAge, HOURS);
        assertEquals(1, stats.splitDocGCCount);

        doc = getDoc("/foo");
        assertNotNull(doc);
        prevDocs = ImmutableList.copyOf(doc.getAllPreviousDocs());
        assertEquals(0, prevDocs.size());
        // check value
        assertEquals(value, store1.getRoot().getChildNode("foo").getString("prop"));
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

    private void createTestNode(String name) throws CommitFailedException {
        DocumentStore ds = store1.getDocumentStore();
        NodeBuilder builder = store1.getRoot().builder();
        builder.child(name);
        merge(store1, builder);
        String id = Utils.getIdFromPath("/" + name);
        int i = 0;
        while (ds.find(NODES, id).getPreviousRanges().isEmpty()) {
            builder = store1.getRoot().builder();
            builder.getChildNode(name).setProperty("p", i++);
            merge(store1, builder);
            store1.runBackgroundOperations();
        }
    }

    private void merge(DocumentNodeStore store, NodeBuilder builder)
            throws CommitFailedException {
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private NodeDocument getDoc(String path) {
        return getDoc(Path.fromString(path));
    }

    private NodeDocument getDoc(Path path) {
        return store1.getDocumentStore().find(NODES, Utils.getIdFromPath(path), 0);
    }
}
