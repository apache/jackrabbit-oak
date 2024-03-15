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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static java.util.List.of;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.apache.commons.lang3.reflect.FieldUtils.writeField;
import static org.apache.jackrabbit.guava.common.base.Strings.repeat;
import static org.apache.jackrabbit.guava.common.collect.Iterables.filter;
import static org.apache.jackrabbit.guava.common.collect.Iterables.size;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.api.Type.STRING;
import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo.DEFAULT_LEASE_DURATION_MILLIS;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.Collection.SETTINGS;
import static org.apache.jackrabbit.oak.plugins.document.DetailGCHelper.assertBranchRevisionNotRemovedFromAllDocuments;
import static org.apache.jackrabbit.oak.plugins.document.DetailGCHelper.assertBranchRevisionRemovedFromAllDocuments;
import static org.apache.jackrabbit.oak.plugins.document.DetailGCHelper.enableDetailGC;
import static org.apache.jackrabbit.oak.plugins.document.DetailGCHelper.enableDetailGCDryRun;
import static org.apache.jackrabbit.oak.plugins.document.DetailGCHelper.mergedBranchCommit;
import static org.apache.jackrabbit.oak.plugins.document.DetailGCHelper.unmergedBranchCommit;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.MIN_ID_VALUE;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.NUM_REVS_THRESHOLD;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.PREV_SPLIT_FACTOR;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.isDeletedEntry;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.setModified;
import static org.apache.jackrabbit.oak.plugins.document.Revision.getCurrentTimestamp;
import static org.apache.jackrabbit.oak.plugins.document.Revision.newRevision;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.NO_BINARY;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.createChild;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.disposeQuietly;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.childBuilder;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation.Type.SET_MAP_ENTRY;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_DETAILED_GC_DOCUMENT_ID_PROP;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_DETAILED_GC_DRY_RUN_DOCUMENT_ID_PROP;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_DETAILED_GC_DRY_RUN_TIMESTAMP_PROP;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_DETAILED_GC_TIMESTAMP_PROP;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_ID;
import static org.apache.jackrabbit.oak.plugins.document.bundlor.DocumentBundlor.META_PROP_PATTERN;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.PATH_LONG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import org.apache.jackrabbit.guava.common.base.Function;
import org.apache.jackrabbit.guava.common.base.Predicate;
import org.apache.jackrabbit.guava.common.collect.AbstractIterator;
import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.guava.common.collect.Queues;
import org.apache.jackrabbit.guava.common.collect.Sets;
import org.apache.jackrabbit.guava.common.util.concurrent.Atomics;
import com.mongodb.ReadPreference;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture.RDBFixture;
import org.apache.jackrabbit.oak.plugins.document.FailingDocumentStore.FailedUpdateOpListener;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.document.bundlor.BundlingConfigInitializer;
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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class VersionGarbageCollectorIT {

    private final DocumentStoreFixture fixture;

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
        ClusterNodeInfo.setClock(clock);
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
        ClusterNodeInfo.resetClockToDefault();
        Revision.resetClockToDefault();
        execService.shutdown();
        execService.awaitTermination(1, MINUTES);
        fixture.dispose();
    }

    private final String rdbTablePrefix = "T" + Long.toHexString(System.currentTimeMillis());

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
        createSecondaryStore(leaseCheckNode, false);
    }

    private void createSecondaryStore(LeaseCheckMode leaseCheckNode, boolean withFailingDS) {
        if (fixture instanceof RDBFixture) {
            ((RDBFixture) fixture).setRDBOptions(
                    new RDBOptions().tablePrefix(rdbTablePrefix).dropTablesOnClose(false));
        }
        ds2 = fixture.createDocumentStore();
        if (withFailingDS) {
            FailingDocumentStore failingDs = new FailingDocumentStore(ds2);
            failingDs.noDispose();
            ds2 = failingDs;
        }
        DocumentMK.Builder documentMKBuilder2 = new DocumentMK.Builder().clock(clock).setClusterId(2)
                .setLeaseCheckMode(leaseCheckNode)
                .setDocumentStore(ds2).setAsyncDelay(0);
        store2 = documentMKBuilder2.getNodeStore();
    }

    private static final Set<Thread> tbefore = new HashSet<>();

    @BeforeClass
    public static void before() throws Exception {
        tbefore.addAll(Thread.getAllStackTraces().keySet());
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
        VersionGCStats stats = gc(gc, maxAge, TimeUnit.MILLISECONDS);
        assertTrue(stats.ignoredGCDueToCheckPoint);
        assertFalse(stats.ignoredDetailedGCDueToCheckPoint);
        assertFalse(stats.detailedGCDryRunMode);
        assertTrue(stats.canceled);

        //Fast forward time to future such that checkpoint get expired
        clock.waitUntil(clock.getTime() + expiryTime + 1);
        stats = gc(gc, maxAge, TimeUnit.MILLISECONDS);
        assertFalse("GC should be performed", stats.ignoredGCDueToCheckPoint);
        assertFalse("Detailed GC shouldn't be performed", stats.ignoredDetailedGCDueToCheckPoint);
        assertFalse(stats.canceled);
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
        clock.waitUntil(getCurrentTimestamp() + maxAge);
        VersionGCStats stats = gc(gc, maxAge, HOURS);
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
        stats = gc(gc, maxAge*2, HOURS);
        assertEquals(0, stats.deletedDocGCCount);
        assertEquals(0, stats.deletedLeafDocGCCount);

        //3. Check that deleted doc does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);

        stats = gc(gc, maxAge*2, HOURS);
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
        stats = gc(gc, maxAge*2, HOURS);
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
        gcSplitDocsInternal(repeat("sub", 120));
    }

    private VersionGCStats gc(VersionGarbageCollector gc, long maxRevisionAge, TimeUnit unit) throws IOException {
        final VersionGCStats stats = gc.gc(maxRevisionAge, unit);
        if (stats.skippedDetailedGCDocsCount != 0) {
            (new Exception("here: " + stats.skippedDetailedGCDocsCount)).printStackTrace(System.out);
        }
        assertEquals(0, stats.skippedDetailedGCDocsCount);
        return stats;
    }

    // OAK-10199
    @Test
    public void testDetailedGCNeedRepeat() throws Exception {
        long expiryTime = 5001, maxAge = 20, batchSize = /*PROGRESS_BATCH_SIZE+1*/ 10001;
        // enable the detailed gc flag
        writeField(gc, "detailedGCEnabled", true, true);

        // create a bunch of garbage
        NodeBuilder b1 = store1.getRoot().builder();
        for( int i = 0; i < batchSize; i++ ) {
	        b1.child("c" + i).setProperty("test", "t", STRING);
        }
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        for( int i = 0; i < batchSize; i++ ) {
	        b1.child("c" + i).removeProperty("test");
        }
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        //Fast forward time to future such that checkpoint get expired
        clock.waitUntil(clock.getTime() + expiryTime);
        VersionGCStats stats = gc(gc, maxAge, TimeUnit.MILLISECONDS);
        assertFalse("Detailed GC should be performed", stats.ignoredDetailedGCDueToCheckPoint);
        assertFalse(stats.canceled);
        assertEquals(batchSize, stats.updatedDetailedGCDocsCount);
        assertFalse(stats.needRepeat);
    }

    // OAK-10199
    @Test
    public void detailedGCIgnoredForCheckpoint() throws Exception {
        long expiryTime = 100, maxAge = 20;
        // enable the detailed gc flag
        writeField(gc, "detailedGCEnabled", true, true);

        Revision cp = Revision.fromString(store1.checkpoint(expiryTime));

        //Fast forward time to future but before expiry of checkpoint
        clock.waitUntil(cp.getTimestamp() + expiryTime - maxAge);
        VersionGCStats stats = gc(gc, maxAge, TimeUnit.MILLISECONDS);
        assertTrue(stats.ignoredDetailedGCDueToCheckPoint);
        assertTrue(stats.canceled);

        //Fast forward time to future such that checkpoint get expired
        clock.waitUntil(clock.getTime() + expiryTime + 1);
        stats = gc(gc, maxAge, TimeUnit.MILLISECONDS);
        assertFalse("Detailed GC should be performed", stats.ignoredDetailedGCDueToCheckPoint);
        assertFalse(stats.canceled);
    }

    @Test
    public void testDetailedGCNotIgnoredForRGCCheckpoint() throws Exception {

        // enable the detailed gc flag
        writeField(gc, "detailedGCEnabled", true, true);

        //1. Create nodes with properties
        NodeBuilder b1 = store1.getRoot().builder();

        // Add property to node & save
        b1.child("x").setProperty("test", "t", STRING);
        b1.child("z").setProperty("test", "t", STRING);
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //Remove property
        NodeBuilder b2 = store1.getRoot().builder();
        b2.getChildNode("x").removeProperty("test");
        store1.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        //2. move clock forward with 2 hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));

        //3. Create a checkpoint now with expiry of 1 hour
        long expiryTime = 1, delta = MINUTES.toMillis(10);
        NodeBuilder b3 = store1.getRoot().builder();
        b3.getChildNode("z").removeProperty("test");
        store1.merge(b3, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        Revision.fromString(store1.checkpoint(HOURS.toMillis(expiryTime)));

        //4. move clock forward by 10 mins
        clock.waitUntil(clock.getTime() + delta);

        // 5. Remove a node
        NodeBuilder b4 = store1.getRoot().builder();
        b4.getChildNode("z").remove();
        store1.merge(b4, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        // 6. Now run gc after checkpoint and see removed properties gets collected
        clock.waitUntil(clock.getTime() + delta*2);
        VersionGCStats stats = gc(gc, delta, MILLISECONDS);
        assertEquals(1, stats.deletedPropsCount);
        assertEquals(1, stats.updatedDetailedGCDocsCount);
        assertTrue(stats.ignoredGCDueToCheckPoint);
        assertTrue(stats.ignoredDetailedGCDueToCheckPoint);
        assertTrue(stats.canceled);
    }

    @Test
    public void testGCDeletedProps() throws Exception{
        //1. Create nodes with properties
        NodeBuilder b1 = store1.getRoot().builder();

        // Add property to node & save
        b1.child("x").setProperty("test", "t", STRING);
        b1.child("z").setProperty("prop", "foo", STRING);
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // update the property
        b1 = store1.getRoot().builder();
        b1.getChildNode("z").setProperty("prop", "bar", STRING);
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // update property again
        b1 = store1.getRoot().builder();
        b1.getChildNode("z").setProperty("prop", "baz", STRING);
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // enable the detailed gc flag
        writeField(gc, "detailedGCEnabled", true, true);
        long maxAge = 1; //hours
        long delta = MINUTES.toMillis(10);
        //1. Go past GC age and check no GC done as nothing deleted
        clock.waitUntil(getCurrentTimestamp() + maxAge);
        VersionGCStats stats = gc(gc, maxAge, HOURS);
        assertEquals(0, stats.deletedPropsCount);
        assertEquals(0, stats.updatedDetailedGCDocsCount);

        //Remove property
        NodeBuilder b2 = store1.getRoot().builder();
        b2.getChildNode("z").removeProperty("prop");
        store1.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        store1.runBackgroundOperations();

        //2. Check that a deleted property is not collected before maxAge
        //Clock cannot move back (it moved forward in #1) so double the maxAge
        clock.waitUntil(clock.getTime() + delta);
        stats = gc(gc, maxAge*2, HOURS);
        assertEquals(0, stats.deletedPropsCount);
        assertEquals(0, stats.updatedDetailedGCDocsCount);

        //3. Check that deleted property does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);

        stats = gc(gc, maxAge*2, HOURS);
        assertEquals(1, stats.deletedPropsCount);
        assertEquals(1, stats.updatedDetailedGCDocsCount);
        assertEquals(MIN_ID_VALUE, stats.oldestModifiedDocId);

        //4. Check that a revived property (deleted and created again) does not get gc
        NodeBuilder b3 = store1.getRoot().builder();
        b3.child("x").removeProperty("test");
        store1.merge(b3, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeBuilder b4 = store1.getRoot().builder();
        b4.child("x").setProperty("test", "t", STRING);
        store1.merge(b4, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);
        stats = gc(gc, maxAge*2, HOURS);
        assertEquals(0, stats.deletedPropsCount);
        assertEquals(0, stats.updatedDetailedGCDocsCount);
        assertEquals(MIN_ID_VALUE, stats.oldestModifiedDocId);
    }

    @Test
    public void testGCDeletedProps_MoreThan_1000_WithSameRevision() throws Exception {
        //1. Create nodes with properties
        NodeBuilder b1 = store1.getRoot().builder();

        // Add property to node & save
        for (int i = 0; i < 5_000; i++) {
            for (int j = 0; j < 10; j++) {
                b1.child("z"+i).setProperty("prop"+j, "foo", STRING);
            }
        }
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // enable the detailed gc flag
        writeField(gc, "detailedGCEnabled", true, true);
        long maxAge = 1; //hours
        long delta = TimeUnit.MINUTES.toMillis(10);

        //Remove property
        NodeBuilder b2 = store1.getRoot().builder();
        for (int i = 0; i < 5_000; i++) {
            for (int j = 0; j < 10; j++) {
                b2.getChildNode("z"+i).removeProperty("prop"+j);
            }
        }
        store1.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        store1.runBackgroundOperations();

        //3. Check that deleted property does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);

        VersionGCStats stats = gc(gc, maxAge*2, HOURS);
        assertEquals(50_000, stats.deletedPropsCount);
        assertEquals(5_000, stats.updatedDetailedGCDocsCount);
        assertEquals(MIN_ID_VALUE, stats.oldestModifiedDocId);
    }

    @Test
    public void testGCDeletedProps_MoreThan_1000_WithDifferentRevision() throws Exception {
        //1. Create nodes with properties
        NodeBuilder b1 = store1.getRoot().builder();
        for (int k = 0; k < 50; k ++) {
            b1 = store1.getRoot().builder();
            // Add property to node & save
            for (int i = 0; i < 100; i++) {
                for (int j = 0; j < 10; j++) {
                    b1.child(k + "z" + i).setProperty("prop" + j, "foo", STRING);
                }
            }
            store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            // increase the clock to create new revision for next batch
            clock.waitUntil(getCurrentTimestamp() + (k * 5));
        }
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // enable the detailed gc flag
        writeField(gc, "detailedGCEnabled", true, true);
        long maxAge = 1; //hours
        long delta = MINUTES.toMillis(20);

        //Remove property
        NodeBuilder b2 = store1.getRoot().builder();
        for (int k = 0; k < 50; k ++) {
            b2 = store1.getRoot().builder();
            for (int i = 0; i < 100; i++) {
                for (int j = 0; j < 10; j++) {
                    b2.getChildNode(k + "z" + i).removeProperty("prop" + j);
                }
            }
            store1.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            // increase the clock to create new revision for next batch
            clock.waitUntil(getCurrentTimestamp() + (k * 5));
        }
        store1.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        store1.runBackgroundOperations();
        //3. Check that deleted property does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);

        VersionGCStats stats = gc(gc, maxAge, HOURS);
        assertEquals(50_000, stats.deletedPropsCount);
        assertEquals(5_000, stats.updatedDetailedGCDocsCount);
        assertEquals(MIN_ID_VALUE, stats.oldestModifiedDocId);
    }

    @Test
    public void testGC_WithNoDeletedProps_And_MoreThan_10_000_DocWithDifferentRevision() throws Exception {
        //1. Create nodes with properties
        NodeBuilder b1 = store1.getRoot().builder();
        for (int k = 0; k < 50; k ++) {
            b1 = store1.getRoot().builder();
            // Add property to node & save
            for (int i = 0; i < 500; i++) {
                for (int j = 0; j < 10; j++) {
                    b1.child(k + "z" + i).setProperty("prop" + j, "foo", STRING);
                }
            }
            store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            // increase the clock to create new revision for next batch
            clock.waitUntil(getCurrentTimestamp() + (k * 5));
        }
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // enable the detailed gc flag
        writeField(gc, "detailedGCEnabled", true, true);
        long maxAge = 1; //hours
        long delta = MINUTES.toMillis(20);


        store1.runBackgroundOperations();
        //3. Check that deleted property does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);

        for (int i = 0; i < 3 ; i++) {

            VersionGCStats stats = gc(gc, maxAge, HOURS);
            String oldestModifiedDocId = stats.oldestModifiedDocId;
            long oldestModifiedDocTimeStamp = stats.oldestModifiedDocTimeStamp;

            Document document = store1.getDocumentStore().find(SETTINGS, SETTINGS_COLLECTION_ID);
            assert document != null;
            assertEquals(document.get(SETTINGS_COLLECTION_DETAILED_GC_TIMESTAMP_PROP), oldestModifiedDocTimeStamp);
            assertEquals(document.get(SETTINGS_COLLECTION_DETAILED_GC_DOCUMENT_ID_PROP), oldestModifiedDocId);
        }
    }

    @Test
    public void testGCDeletedPropsAlreadyGCed() throws Exception {
        //1. Create nodes with properties
        NodeBuilder b1 = store1.getRoot().builder();
        // Add property to node & save
        for (int i = 0; i < 10; i++) {
            b1.child("z" + i).setProperty("prop" + i, "foo", STRING);
        }
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // enable the detailed gc flag
        writeField(gc, "detailedGCEnabled", true, true);
        long maxAge = 1; //hours
        long delta = MINUTES.toMillis(10);
        //1. Go past GC age and check no GC done as nothing deleted
        clock.waitUntil(getCurrentTimestamp() + maxAge);
        VersionGCStats stats = gc(gc, maxAge, HOURS);
        assertEquals(0, stats.deletedPropsCount);

        //Remove property
        NodeBuilder b2 = store1.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            b2.getChildNode("z" + i).removeProperty("prop" + i);
        }
        store1.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        //2. Check that deleted property does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);

        stats = gc(gc, maxAge*2, HOURS);
        assertEquals(10, stats.deletedPropsCount);
        assertEquals(10, stats.updatedDetailedGCDocsCount);
        assertEquals(MIN_ID_VALUE, stats.oldestModifiedDocId);

        //3. now reCreate those properties again
        NodeBuilder b3 = store1.getRoot().builder();
        // Add property to node & save
        for (int i = 0; i < 10; i++) {
            b3.child("z" + i).setProperty("prop" + i, "bar", STRING);
        }
        store1.merge(b3, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //Remove properties again
        NodeBuilder b4 = store1.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            b4.getChildNode("z" + i).removeProperty("prop" + i);
        }
        store1.merge(b4, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();


        //4. Check that deleted property does get collected again
        // increment the clock again by more than 2 hours + delta
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);
        stats = gc(gc, maxAge*2, HOURS);
        assertEquals(10, stats.deletedPropsCount);
        assertEquals(10, stats.updatedDetailedGCDocsCount);
        assertEquals(MIN_ID_VALUE, stats.oldestModifiedDocId);
    }

    @Test
    public void testGCDeletedPropsAfterSystemCrash() throws Exception {
        if (store1 != null) {
            store1.dispose();
        }
        final FailingDocumentStore fds = new FailingDocumentStore(fixture.createDocumentStore(), 42) {
            @Override
            public void dispose() {}
        };
        store1 = new DocumentMK.Builder().clock(clock).setLeaseCheckMode(LeaseCheckMode.DISABLED)
                .setDocumentStore(fds).setAsyncDelay(0).getNodeStore();

        assertTrue(store1.getDocumentStore() instanceof FailingDocumentStore);

        MongoTestUtils.setReadPreference(store1, ReadPreference.primary());
        gc = store1.getVersionGarbageCollector();

        //1. Create nodes with properties
        NodeBuilder b1 = store1.getRoot().builder();
        // Add property to node & save
        for (int i = 0; i < 10; i++) {
            b1.child("z" + i).setProperty("prop" + i, "foo", STRING);
        }
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //2. Remove property
        NodeBuilder b2 = store1.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            b2.getChildNode("z" + i).removeProperty("prop" + i);
        }
        store1.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        // enable the detailed gc flag
        writeField(gc, "detailedGCEnabled", true, true);
        long maxAge = 1; //hours
        long delta = MINUTES.toMillis(10);

        //3. Check that deleted property does get collected again
        // increment the clock again by more than 2 hours + delta
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);

        fds.fail().after(0).eternally();
        try {
            store1.dispose();
            fail("dispose() must fail with an exception");
        } catch (DocumentStoreException e) {
            // expected
        }
        fds.fail().never();

        // create new store
        store1 = new DocumentMK.Builder().clock(clock).setLeaseCheckMode(LeaseCheckMode.DISABLED)
                .setDocumentStore(fds).setAsyncDelay(0)
                .getNodeStore();
        assertTrue(store1.getDocumentStore() instanceof FailingDocumentStore);
        MongoTestUtils.setReadPreference(store1, ReadPreference.primary());
        gc = store1.getVersionGarbageCollector();
        store1.runBackgroundOperations();
        // enable the detailed gc flag
        writeField(gc, "detailedGCEnabled", true, true);

        //4. Check that deleted property does get collected again
        // increment the clock again by more than 2 hours + delta
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);
        VersionGCStats stats = gc(gc, maxAge*2, HOURS);
        assertEquals(10, stats.deletedPropsCount);
        assertEquals(10, stats.updatedDetailedGCDocsCount);
        assertEquals(MIN_ID_VALUE, stats.oldestModifiedDocId);
    }

    @Test
    public void testGCDeletedEscapeProps() throws Exception {
        //1. Create nodes with properties
        NodeBuilder b1 = store1.getRoot().builder();

        // Add property to node & save
        for (int i = 0; i < 10; i++) {
            b1.child("x").setProperty("test."+i, "t", STRING);
        }
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // enable the detailed gc flag
        writeField(gc, "detailedGCEnabled", true, true);
        long maxAge = 1; //hours
        long delta = MINUTES.toMillis(10);
        //1. Go past GC age and check no GC done as nothing deleted
        clock.waitUntil(getCurrentTimestamp() + maxAge);
        VersionGCStats stats = gc(gc, maxAge, HOURS);
        assertEquals(0, stats.deletedPropsCount);
        assertEquals(0, stats.updatedDetailedGCDocsCount);

        //Remove property
        NodeBuilder b2 = store1.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            b2.getChildNode("x").removeProperty("test."+i);
        }
        store1.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        store1.runBackgroundOperations();

        //2. Check that a deleted property is not collected before maxAge
        //Clock cannot move back (it moved forward in #1) so double the maxAge
        clock.waitUntil(clock.getTime() + delta);
        stats = gc(gc, maxAge*2, HOURS);
        assertEquals(0, stats.deletedPropsCount);
        assertEquals(0, stats.updatedDetailedGCDocsCount);

        //3. Check that deleted property does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);

        stats = gc(gc, maxAge*2, HOURS);
        assertEquals(10, stats.deletedPropsCount);

        //4. Check that a revived property (deleted and created again) does not get gc
        NodeBuilder b4 = store1.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            b4.child("x").setProperty("test."+i, "t", STRING);
        }
        store1.merge(b4, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);
        stats = gc(gc, maxAge*2, HOURS);
        assertEquals(0, stats.deletedPropsCount);
        assertEquals(0, stats.updatedDetailedGCDocsCount);
    }

    @Test
    public void testGCDeletedLongPathProps() throws Exception {
        //1. Create nodes with properties
        NodeBuilder b1 = store1.getRoot().builder();
        String longPath = repeat("p", PATH_LONG + 1);
        b1.child(longPath);

        // Add property to node & save
        for (int i = 0; i < 10; i++) {
            b1.child(longPath).child("foo").setProperty("test"+i, "t", STRING);
        }
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // enable the detailed gc flag
        writeField(gc, "detailedGCEnabled", true, true);
        long maxAge = 1; //hours
        long delta = MINUTES.toMillis(10);
        //1. Go past GC age and check no GC done as nothing deleted
        clock.waitUntil(getCurrentTimestamp() + maxAge);
        VersionGCStats stats = gc(gc, maxAge, HOURS);
        assertEquals(0, stats.deletedPropsCount);
        assertEquals(0, stats.updatedDetailedGCDocsCount);

        //Remove property
        NodeBuilder b2 = store1.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            b2.child(longPath).child("foo").removeProperty("test"+i);
        }
        store1.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        store1.runBackgroundOperations();

        //2. Check that a deleted property is not collected before maxAge
        //Clock cannot move back (it moved forward in #1) so double the maxAge
        clock.waitUntil(clock.getTime() + delta);
        stats = gc(gc, maxAge*2, HOURS);
        assertEquals(0, stats.deletedPropsCount);
        assertEquals(0, stats.updatedDetailedGCDocsCount);

        //3. Check that deleted property does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);

        stats = gc(gc, maxAge*2, HOURS);
        assertEquals(10, stats.deletedPropsCount);

        //4. Check that a revived property (deleted and created again) does not get gc
        NodeBuilder b4 = store1.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            b4.child(longPath).child("foo").setProperty("test"+i, "t", STRING);
        }
        store1.merge(b4, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);
        stats = gc(gc, maxAge*2, HOURS);
        assertEquals(0, stats.deletedPropsCount);
        assertEquals(0, stats.updatedDetailedGCDocsCount);
    }

    @Test
    public void testGCDeletedNonBundledProps() throws Exception {

        //0. Initialize bundling configs
        final NodeBuilder builder = store1.getRoot().builder();
        new InitialContent().initialize(builder);
        BundlingConfigInitializer.INSTANCE.initialize(builder);
        merge(store1, builder);
        store1.runBackgroundOperations();

        //1. Create nodes with properties
        NodeBuilder b1 = store1.getRoot().builder();
        b1.child("x").setProperty("jcr:primaryType", "nt:file", NAME);

        // Add property to node & save
        for (int i = 0; i < 10; i++) {
            b1.child("x").child("jcr:content").setProperty("prop"+i, "t", STRING);
            b1.child("x").setProperty(META_PROP_PATTERN, of("jcr:content"), STRINGS);
            b1.child("x").setProperty("prop"+i, "bar", STRING);
        }
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // enable the detailed gc flag
        writeField(gc, "detailedGCEnabled", true, true);
        long maxAge = 1; //hours
        long delta = MINUTES.toMillis(10);
        //1. Go past GC age and check no GC done as nothing deleted
        clock.waitUntil(getCurrentTimestamp() + maxAge);
        VersionGCStats stats = gc(gc, maxAge, HOURS);
        assertEquals(0, stats.deletedPropsCount);
        assertEquals(0, stats.updatedDetailedGCDocsCount);

        //Remove property
        NodeBuilder b2 = store1.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            b2.getChildNode("x").removeProperty("prop"+i);
        }
        store1.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        store1.runBackgroundOperations();

        //2. Check that a deleted property is not collected before maxAge
        //Clock cannot move back (it moved forward in #1) so double the maxAge
        clock.waitUntil(clock.getTime() + delta);
        stats = gc(gc, maxAge*2, HOURS);
        assertEquals(0, stats.deletedPropsCount);
        assertEquals(0, stats.updatedDetailedGCDocsCount);

        //3. Check that deleted property does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);

        stats = gc(gc, maxAge*2, HOURS);
        assertEquals(10, stats.deletedPropsCount);
    }

    @Test
    public void testGCDeletedBundledProps() throws Exception {

        //0. Initialize bundling configs
        final NodeBuilder builder = store1.getRoot().builder();
        new InitialContent().initialize(builder);
        BundlingConfigInitializer.INSTANCE.initialize(builder);
        merge(store1, builder);
        store1.runBackgroundOperations();

        //1. Create nodes with properties
        NodeBuilder b1 = store1.getRoot().builder();
        b1.child("x").setProperty("jcr:primaryType", "nt:file", NAME);

        // Add property to node & save
        for (int i = 0; i < 10; i++) {
            b1.child("x").child("jcr:content").setProperty("prop"+i, "t", STRING);
            b1.child("x").setProperty(META_PROP_PATTERN, of("jcr:content"), STRINGS);
            b1.child("x").setProperty("prop"+i, "bar", STRING);
        }
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // enable the detailed gc flag
        writeField(gc, "detailedGCEnabled", true, true);
        long maxAge = 1; //hours
        long delta = MINUTES.toMillis(10);
        //1. Go past GC age and check no GC done as nothing deleted
        clock.waitUntil(getCurrentTimestamp() + maxAge);
        VersionGCStats stats = gc(gc, maxAge, HOURS);
        assertEquals(0, stats.deletedPropsCount);
        assertEquals(0, stats.updatedDetailedGCDocsCount);

        //Remove property
        NodeBuilder b2 = store1.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            b2.getChildNode("x").getChildNode("jcr:content").removeProperty("prop"+i);
        }
        store1.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        store1.runBackgroundOperations();

        //2. Check that a deleted property is not collected before maxAge
        //Clock cannot move back (it moved forward in #1) so double the maxAge
        clock.waitUntil(clock.getTime() + delta);
        stats = gc(gc, maxAge*2, HOURS);
        assertEquals(0, stats.deletedPropsCount);
        assertEquals(0, stats.updatedDetailedGCDocsCount);

        //3. Check that deleted property does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);

        stats = gc(gc, maxAge*2, HOURS);
        assertEquals(10, stats.deletedPropsCount);
    }

    @Test
    public void testGCDeletedPropsWhenModifiedConcurrently() throws Exception {
        //1. Create nodes with properties
        NodeBuilder b1 = store1.getRoot().builder();

        // Add property to node & save
        for (int i = 0; i < 10; i++) {
            b1.child("x"+i).setProperty("test"+i, "t", STRING);
        }
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // enable the detailed gc flag
        writeField(gc, "detailedGCEnabled", true, true);
        long maxAge = 1; //hours
        long delta = MINUTES.toMillis(10);
        //1. Go past GC age and check no GC done as nothing deleted
        clock.waitUntil(getCurrentTimestamp() + maxAge);
        VersionGCStats stats = gc(gc, maxAge, HOURS);
        assertEquals(0, stats.deletedPropsCount);
        assertEquals(0, stats.updatedDetailedGCDocsCount);

        //Remove property
        NodeBuilder b2 = store1.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            b2.getChildNode("x"+i).removeProperty("test"+i);
        }
        store1.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        //2. Check that a deleted property is not collected before maxAge
        //Clock cannot move back (it moved forward in #1) so double the maxAge
        clock.waitUntil(clock.getTime() + delta);
        stats = gc(gc, maxAge*2, HOURS);
        assertEquals(0, stats.deletedPropsCount);
        assertEquals(0, stats.updatedDetailedGCDocsCount);
        assertEquals(MIN_ID_VALUE, stats.oldestModifiedDocId); // as GC hadn't run

        //3. Check that deleted property does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);

        VersionGCSupport gcSupport = new VersionGCSupport(store1.getDocumentStore()) {

            @Override
            public Iterable<NodeDocument> getModifiedDocs(long fromModified, long toModified, int limit, @NotNull String fromId) {
                Iterable<NodeDocument> modifiedDocs = super.getModifiedDocs(fromModified, toModified, limit, fromId);
                List<NodeDocument> result = stream(modifiedDocs.spliterator(), false).collect(toList());
                final Revision updateRev = newRevision(1);
                store1.getDocumentStore().findAndUpdate(NODES, stream(modifiedDocs.spliterator(), false)
                        .map(doc -> {
                            UpdateOp op = new UpdateOp(requireNonNull(doc.getId()), false);
                            setModified(op, updateRev);
                            return op;
                        }).
                        collect(toList())
                );
                return result;
            }
        };

        VersionGarbageCollector gc = new VersionGarbageCollector(store1, gcSupport, true, false, false);
        stats = gc.gc(maxAge*2, HOURS);
        assertEquals(0, stats.updatedDetailedGCDocsCount);
        assertEquals(0, stats.deletedPropsCount);
        assertEquals(MIN_ID_VALUE, stats.oldestModifiedDocId);
    }

    @Test
    public void cancelDetailedGCAfterFirstBatch() throws Exception {
        //1. Create nodes with properties
        NodeBuilder b1 = store1.getRoot().builder();

        // Add property to node & save
        for (int i = 0; i < 5_000; i++) {
            for (int j = 0; j < 10; j++) {
                b1.child("z"+i).setProperty("prop"+j, "foo", STRING);
            }
        }
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        // enable the detailed gc flag
        writeField(gc, "detailedGCEnabled", true, true);
        long maxAge = 1; //hours
        long delta = MINUTES.toMillis(10);
        //1. Go past GC age and check no GC done as nothing deleted
        clock.waitUntil(getCurrentTimestamp() + maxAge);
        VersionGCStats stats = gc(gc, maxAge, HOURS);
        assertEquals(0, stats.deletedPropsCount);
        assertEquals(0, stats.updatedDetailedGCDocsCount);

        //Remove property
        NodeBuilder b2 = store1.getRoot().builder();
        for (int i = 0; i < 5_000; i++) {
            for (int j = 0; j < 10; j++) {
                b2.getChildNode("z"+i).removeProperty("prop"+j);
            }
        }
        store1.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        final AtomicReference<VersionGarbageCollector> gcRef = Atomics.newReference();
        final VersionGCSupport gcSupport = new VersionGCSupport(store1.getDocumentStore()) {

            @Override
            public Iterable<NodeDocument> getModifiedDocs(long fromModified, long toModified, int limit, @NotNull String fromId) {
                return () -> new AbstractIterator<>() {
                    private final Iterator<NodeDocument> it = candidates(fromModified, toModified, limit, fromId);

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

            private Iterator<NodeDocument> candidates(long fromModified, long toModified, int limit, @NotNull String fromId) {
                return super.getModifiedDocs(fromModified, toModified, limit, fromId).iterator();
            }
        };

        gcRef.set(new VersionGarbageCollector(store1, gcSupport, true, false, false));

        //3. Check that deleted property does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);
        stats = gcRef.get().gc(maxAge*2, HOURS);
        assertTrue(stats.canceled);
        assertEquals(0, stats.updatedDetailedGCDocsCount);
        assertEquals(0, stats.deletedPropsCount);
        assertEquals(MIN_ID_VALUE, stats.oldestModifiedDocId);
    }

    // OAK-10199 END

    // OAK-8646
    @Test
    public void testDeletedPropsAndUnmergedBCWithoutCollision() throws Exception {
        // create a node with property.
        NodeBuilder nb = store1.getRoot().builder();
        nb.child("bar").setProperty("prop", "value");
        nb.child("bar").setProperty("x", "y");
        store1.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        // remove the property
        nb = store1.getRoot().builder();
        nb.child("bar").removeProperty("prop");
        store1.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        // create branch commits
        mergedBranchCommit(store1, b -> b.child("foo").setProperty("p", "prop"));
        RevisionVector br1 = unmergedBranchCommit(store1, b -> b.child("foo").setProperty("a", "b"));
        RevisionVector br4 = unmergedBranchCommit(store1, b -> b.child("bar").setProperty("x", "z"));
        mergedBranchCommit(store1, b -> b.child("foo").removeProperty("p"));
        store1.runBackgroundOperations();

        // enable the detailed gc flag
        writeField(gc, "detailedGCEnabled", true, true);

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGCStats stats = gc(gc, 1, HOURS);

        assertEquals(3, stats.updatedDetailedGCDocsCount);
        // deleted properties are : 1:/foo -> prop, a & p && 1:/bar -> _bc
        assertEquals(4, stats.deletedPropsCount);
        assertEquals(2, stats.deletedUnmergedBCCount);
        assertBranchRevisionRemovedFromAllDocuments(store1, br1);
        assertBranchRevisionRemovedFromAllDocuments(store1, br4);
    }

    @Test
    public void testDeletedPropsAndUnmergedBCWithCollision() throws Exception {
        // create a node with property.
        NodeBuilder nb = store1.getRoot().builder();
        nb.child("bar").setProperty("prop", "value");
        nb.child("bar").setProperty("x", "y");
        store1.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        // remove the property
        nb = store1.getRoot().builder();
        nb.child("bar").removeProperty("prop");
        store1.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        // create branch commits
        mergedBranchCommit(store1, b -> b.child("foo").setProperty("p", "prop"));
        RevisionVector br1 = unmergedBranchCommit(store1, b -> b.child("foo").setProperty("a", "b"));
        RevisionVector br2 = unmergedBranchCommit(store1, b -> b.child("foo").setProperty("a", "c"));
        RevisionVector br3 = unmergedBranchCommit(store1, b -> b.child("foo").setProperty("a", "d"));
        RevisionVector br4 = unmergedBranchCommit(store1, b -> b.child("bar").setProperty("x", "z"));
        mergedBranchCommit(store1, b -> b.child("foo").removeProperty("p"));
        store1.runBackgroundOperations();

        // enable the detailed gc flag
        writeField(gc, "detailedGCEnabled", true, true);

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGCStats stats = gc(gc, 1, HOURS);

        assertEquals(3, stats.updatedDetailedGCDocsCount);
        // deleted properties are : 1:/foo -> prop, a, _collisions & p && 1:/bar -> _bc
        assertEquals(5, stats.deletedPropsCount);
        assertEquals(4, stats.deletedUnmergedBCCount);
        assertBranchRevisionRemovedFromAllDocuments(store1, br1);
        assertBranchRevisionRemovedFromAllDocuments(store1, br2);
        assertBranchRevisionRemovedFromAllDocuments(store1, br3);
        assertBranchRevisionRemovedFromAllDocuments(store1, br4);
    }
    // OAK-8646 END

    /**
     * Tests whether DetailedGC properly deletes a late-written addChild "/grand/parent/a"
     */
    @Test
    public void lateWriteCreateChildGC() throws Exception {
        doLateWriteCreateChildrenGC(of("/grand/parent"), of("/grand/parent/a"), 1, "/d");
    }

    /**
     * Tests whether DetailedGC can delete a whole subtree "/a/b/c/d/**" that was
     * added via late-writes.
     */
    @Test
    public void lateWriteCreateChildTreeGC() throws Exception {
        doLateWriteCreateChildrenGC(of("/a", "/a/b/c"), of("/a/b/c/d", "/a/b/c/d/e/f"), 3, "/d");
    }

    /**
     * Tests whether DetailedGC can delete a large amount of randomly
     * created orphans (that were added in a late-write manner)
     */
    @Test
    public void lateWriteCreateManyChildrenGC() throws Exception {
        List<String> nonOrphans = of("/a", "/b", "/c");
        createNodes(nonOrphans);
        Set<String> orphans = new HashSet<>();
        Set<String> commonOrphanParents = new HashSet<>();
        Random r = new Random(43);
        for(int i = 0; i < 900; i++) {
            String orphanParent = nonOrphans.get(r.nextInt(3)) + "/" + r.nextInt(42);
            commonOrphanParents.add(orphanParent);
            orphans.add(orphanParent + "/" + r.nextInt(24));
        }
        doLateWriteCreateChildrenGC(nonOrphans, orphans, orphans.size() + commonOrphanParents.size(), "/d");
    }

    @Test
    @Ignore(value = "OAK-10535 : fails currently as uncommitted revisions aren't yet removed")
    public void lateWriteRemoveChildGC_noSweep() throws Exception {
        assumeTrue(fixture.hasSinglePersistence());
        enableDetailGC(store1.getVersionGarbageCollector());
        createNodes("/a/b/c/d");
        lateWriteRemoveNodes(of("/a/b"), null);

        assertTrue(getChildeNodeState(store1, "/a/b/c/d", true).exists());

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGCStats stats = gc(store1.getVersionGarbageCollector(), 1, HOURS);
        assertNotNull(stats);

        assertNotNull(store1.getDocumentStore().find(NODES, "2:/a/b"));
        assertNotNull(store1.getDocumentStore().find(NODES, "4:/a/b/c/d"));
        assertTrue(getChildeNodeState(store1, "/a/b/c/d", true).exists());
        //TODO: below assert fails currently as uncommitted revisions aren't yet removed
        // should be 3 as it should clean up the _deleted from /a/b, /a/b/c and /a/b/c/d
        assertEquals(3, stats.updatedDetailedGCDocsCount);
    }

    /**
     * This (re)produces a case where classic GC deletes nodes
     * but they are still in the nodes cache, eg:
     *  org.apache.jackrabbit.oak.plugins.document.ConflictException: 
     * The node 4:/a/b/c/d does not exist or is already deleted 
     * at base revision r2-0-1,r1-0-2,
     * branch: null, commit revision: re-0-1]
     */
    @Test
    @Ignore(value = "OAK-10658 : fails currently as invalidation is missing (in classic GC) after late-write-then-sweep-then-GC")
    public void lateWriteRemoveChildGC_withSweep() throws Exception {
        assumeTrue(fixture.hasSinglePersistence());
        enableDetailGC(store1.getVersionGarbageCollector());
        createNodes("/a/b/c/d");
        lateWriteRemoveNodes(of("/a/b"), "/foo");

        getChildeNodeState(store1, "/a/b/c/d", true);

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        getChildeNodeState(store1, "/a/b/c/d", true);
        assertNotNull(store1.getDocumentStore().find(NODES, "4:/a/b/c/d"));
        assertNotNull(store1.getDocumentStore().find(NODES, "3:/a/b/c"));
        assertNotNull(store1.getDocumentStore().find(NODES, "2:/a/b"));

        VersionGCStats stats = gc(store1.getVersionGarbageCollector(), 1, HOURS);
        assertNotNull(stats);

        assertNull(store1.getDocumentStore().find(NODES, "4:/a/b/c/d"));
        assertNull(store1.getDocumentStore().find(NODES, "3:/a/b/c"));
        assertNull(store1.getDocumentStore().find(NODES, "2:/a/b"));

        // invalidating store1's nodeCache would fix it
        // but we need that to happen in prod code, not test
//        store1.getNodeCache().invalidateAll();

        // creating /a/b/c again, below late-write-removed /a/b
        // triggered a ConflictException
        createNodes("/a/b/c/d/e");
        getChildeNodeState(store1, "/a/b/c/d/e", true);
    }

    @Test
    public void orphanedChildGC() throws Exception {
        assumeTrue(fixture.hasSinglePersistence());
        createSecondaryStore(LeaseCheckMode.LENIENT);
        createNodes(store2, "/a/b/c", "/a/b/c/d/e", "/a/f/g");
        ds2.remove(NODES, "3:/a/b/c");
        store2.dispose();

        store1.runBackgroundOperations();

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        enableDetailGC(store1.getVersionGarbageCollector());
        VersionGCStats stats = gc(store1.getVersionGarbageCollector(), 1, HOURS);
        assertNotNull(stats);
        // expected 2 updated (deletions): /a/b/c/d and /a/b/c/d/e
        assertEquals(2, stats.updatedDetailedGCDocsCount);
        assertEquals(2, stats.deletedDocGCCount);

        createNodes("/a/b/c/d/e");
    }

    // OAK-10370
    @Test
    public void testGCDeletedPropsWithDryRunMode() throws Exception {
        //1. Create nodes with properties
        NodeBuilder b1 = store1.getRoot().builder();
        b1.child("x").setProperty("test", "t", STRING);
        b1.child("z").setProperty("prop", "foo", STRING);
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // enable the detailed gc flag
        enableDetailGC(gc);
        long maxAge = 1; //hours
        long delta = MINUTES.toMillis(10);

        //2. Remove property
        NodeBuilder b2 = store1.getRoot().builder();
        b2.getChildNode("z").removeProperty("prop");
        store1.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        store1.runBackgroundOperations();

        //3. Check that deleted property does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);

        VersionGCStats stats = gc(gc, maxAge*2, HOURS);
        assertEquals(1, stats.deletedPropsCount);
        assertEquals(1, stats.updatedDetailedGCDocsCount);
        assertEquals(MIN_ID_VALUE, stats.oldestModifiedDocId);

        // 4. Save values of detailedGC settings collection fields
        final String oldestModifiedDocId = stats.oldestModifiedDocId;
        final long oldestModifiedDocTimeStamp = stats.oldestModifiedDocTimeStamp;

        final Document documentBefore = store1.getDocumentStore().find(SETTINGS, SETTINGS_COLLECTION_ID);
        assert documentBefore != null;
        assertEquals(documentBefore.get(SETTINGS_COLLECTION_DETAILED_GC_TIMESTAMP_PROP), oldestModifiedDocTimeStamp);
        assertEquals(documentBefore.get(SETTINGS_COLLECTION_DETAILED_GC_DOCUMENT_ID_PROP), oldestModifiedDocId);

        //5. Verify that in dryRun mode property does not get gc and detailedGC fields remain the same
        NodeBuilder b3 = store1.getRoot().builder();
        b3.child("x").removeProperty("test");
        store1.merge(b3, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);
        // enabled dryRun mode
        enableDetailGCDryRun(gc);
        stats = gc(gc, maxAge*2, HOURS);

        final String oldestModifiedDryRunDocId = stats.oldestModifiedDocId;
        final long oldestModifiedDocDryRunTimeStamp = stats.oldestModifiedDocTimeStamp;

        assertEquals(0, stats.deletedPropsCount);
        assertEquals(0, stats.updatedDetailedGCDocsCount);
        assertEquals(MIN_ID_VALUE, stats.oldestModifiedDocId);
        assertTrue(stats.detailedGCDryRunMode);

        final Document documentAfter = store1.getDocumentStore().find(SETTINGS, SETTINGS_COLLECTION_ID);
        assert documentAfter != null;
        assertEquals(documentAfter.get(SETTINGS_COLLECTION_DETAILED_GC_TIMESTAMP_PROP), oldestModifiedDocTimeStamp);
        assertEquals(documentAfter.get(SETTINGS_COLLECTION_DETAILED_GC_DOCUMENT_ID_PROP), oldestModifiedDocId);

        assertEquals(documentAfter.get(SETTINGS_COLLECTION_DETAILED_GC_DRY_RUN_TIMESTAMP_PROP), oldestModifiedDocDryRunTimeStamp);
        assertEquals(documentAfter.get(SETTINGS_COLLECTION_DETAILED_GC_DRY_RUN_DOCUMENT_ID_PROP), oldestModifiedDryRunDocId);
    }

    @Test
    public void testDeletedPropsAndUnmergedBCWithCollisionWithDryRunMode() throws Exception {
        // create a node with property.
        NodeBuilder nb = store1.getRoot().builder();
        nb.child("bar").setProperty("prop", "value");
        nb.child("bar").setProperty("x", "y");
        store1.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        // remove the property
        nb = store1.getRoot().builder();
        nb.child("bar").removeProperty("prop");
        store1.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        // create branch commits
        mergedBranchCommit(store1, b -> b.child("foo").setProperty("p", "prop"));
        RevisionVector br1 = unmergedBranchCommit(store1, b -> b.child("foo").setProperty("a", "b"));
        RevisionVector br2 = unmergedBranchCommit(store1, b -> b.child("foo").setProperty("a", "c"));
        RevisionVector br3 = unmergedBranchCommit(store1, b -> b.child("foo").setProperty("a", "d"));
        RevisionVector br4 = unmergedBranchCommit(store1, b -> b.child("bar").setProperty("x", "z"));
        mergedBranchCommit(store1, b -> b.child("foo").removeProperty("p"));
        store1.runBackgroundOperations();

        // enable the detailed gc flag
        enableDetailGC(gc);
        enableDetailGCDryRun(gc);

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGCStats stats = gc(gc, 1, HOURS);

        assertEquals(0, stats.updatedDetailedGCDocsCount);
        // deleted properties are : 1:/foo -> prop, a, _collisions & p && 1:/bar -> _bc
        assertEquals(0, stats.deletedPropsCount);
        assertEquals(0, stats.deletedUnmergedBCCount);
        assertTrue(stats.detailedGCDryRunMode);

        assertBranchRevisionNotRemovedFromAllDocuments(store1, br1);
        assertBranchRevisionNotRemovedFromAllDocuments(store1, br2);
        assertBranchRevisionNotRemovedFromAllDocuments(store1, br3);
        assertBranchRevisionNotRemovedFromAllDocuments(store1, br4);
    }

    // OAK-10370 END

    // OAK-10676
    @Test
    public void removePropertyAddedByLateWriteWithoutUnrelatedPath() throws Exception {
        // create a node with property.
        assumeTrue(fixture.hasSinglePersistence());
        NodeBuilder nb = store1.getRoot().builder();
        nb.child("bar").setProperty("p", "v");
        store1.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        // remove the property
        nb = store1.getRoot().builder();
        nb.child("bar").removeProperty("p");
        store1.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        // unrelated path should be such that the paths and unrelated path shouldn't have common parent
        // for e.g. if path is /bar & unrelated is /d then there common ancestor is "/" i.e. root.
        lateWriteAddPropertiesNodes(of("/bar"), null, "p", "v2");

        assertDocumentsExist(of("/bar"));
        assertPropertyExist("/bar", "p");

        enableDetailGC(store1.getVersionGarbageCollector());

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGCStats stats = gc(store1.getVersionGarbageCollector(), 1, HOURS);
        assertNotNull(stats);
        assertEquals(0, stats.deletedDocGCCount);
        assertEquals(1, stats.deletedPropsCount);

        assertDocumentsExist(of("/bar"));
    }

    @Test
    public void removePropertyAddedByLateWriteWithoutUnrelatedPath_2() throws Exception {
        // create a node with property.
        assumeTrue(fixture.hasSinglePersistence());
        NodeBuilder nb = store1.getRoot().builder();
        nb.child("foo").child("bar").child("baz").setProperty("prop", "value");
        store1.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        // remove the property
        nb = store1.getRoot().builder();
        nb.child("foo").child("bar").child("baz").removeProperty("prop");
        store1.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        // unrelated path should be such that the paths and unrelated path shouldn't have common parent
        // for e.g. if path is /bar & unrelated is /d then there common ancestor is "/" i.e. root.
        lateWriteAddPropertiesNodes(of("/foo/bar/baz"), "/a", "prop", "value2");

        assertDocumentsExist(of("/foo/bar/baz"));
        assertPropertyExist("/foo/bar/baz", "prop");

        enableDetailGC(store1.getVersionGarbageCollector());

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGCStats stats = gc(store1.getVersionGarbageCollector(), 1, HOURS);
        assertNotNull(stats);
        assertEquals(0, stats.deletedDocGCCount);
        // since we have updated a totally unrelated path i.e. "/a", we should still be seeing the garbage from late write and
        // thus it will be collected.
        assertEquals(1, stats.deletedPropsCount);

        assertDocumentsExist(of("/foo/bar/baz"));
    }

    @Test
    public void removePropertyAddedByLateWriteWithRelatedPath() throws Exception {
        // create a node with property.
        assumeTrue(fixture.hasSinglePersistence());
        NodeBuilder nb = store1.getRoot().builder();
        nb.child("bar").setProperty("prop", "value");
        store1.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        // remove the property
        nb = store1.getRoot().builder();
        nb.child("bar").removeProperty("prop");
        store1.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        // unrelated path should be such that the paths and unrelated path shouldn't have common parent
        // for e.g. if path is /bar & unrelated is /d then there common ancestor is "/" i.e. root.
        lateWriteAddPropertiesNodes(of("/bar"), "/d", "prop", "value2");

        assertDocumentsExist(of("/bar"));
        assertPropertyExist("/bar", "prop");

        enableDetailGC(store1.getVersionGarbageCollector());

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGCStats stats = gc(store1.getVersionGarbageCollector(), 1, HOURS);
        assertNotNull(stats);
        assertEquals(0, stats.deletedDocGCCount);
        // we shouldn't be able to remove the property since we have updated an related path that has lead to an update
        // of common ancestor and this would make late write visible
        assertEquals(0, stats.deletedPropsCount);

        assertDocumentsExist(of("/bar"));
    }

    @Test
    public void skipPropertyRemovedByLateWriteWithoutUnrelatedPath() throws Exception {
        // create a node with property.
        assumeTrue(fixture.hasSinglePersistence());
        NodeBuilder nb = store1.getRoot().builder();
        nb.child("bar").setProperty("p", "value");
        store1.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        // unrelated path should be such that the paths and unrelated path shouldn't have common parent
        // for e.g. if path is /bar & unrelated is /d then there common ancestor is "/" i.e. root.
        lateWriteRemovePropertiesNodes(of("/bar"), null, "p");

        assertDocumentsExist(of("/bar"));
        assertPropertyNotExist("/bar", "p");

        enableDetailGC(store1.getVersionGarbageCollector());

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGCStats stats = gc(store1.getVersionGarbageCollector(), 1, HOURS);
        assertNotNull(stats);
        assertEquals(0, stats.deletedDocGCCount);
        assertEquals(0, stats.deletedPropsCount);

        assertDocumentsExist(of("/bar"));
    }

    @Test
    public void skipPropertyRemovedByLateWriteWithoutUnrelatedPath_2() throws Exception {
        // create a node with property.
        assumeTrue(fixture.hasSinglePersistence());
        NodeBuilder nb = store1.getRoot().builder();
        nb.child("foo").child("bar").child("baz").setProperty("prop", "value");
        store1.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        // unrelated path should be such that the paths and unrelated path shouldn't have common parent
        // for e.g. if path is /bar & unrelated is /d then there common ancestor is "/" i.e. root.
        lateWriteRemovePropertiesNodes(of("/foo/bar/baz"), "/a", "prop");

        assertDocumentsExist(of("/foo/bar/baz"));
        assertPropertyNotExist("/foo/bar/baz", "prop");

        enableDetailGC(store1.getVersionGarbageCollector());

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGCStats stats = gc(store1.getVersionGarbageCollector(), 1, HOURS);
        assertNotNull(stats);
        assertEquals(0, stats.deletedDocGCCount);
        // since we have updated an totally unrelated path i.e. "/a", we should still be seeing the garbage from late write and
        // thus it will be collected.
        assertEquals(0, stats.deletedPropsCount);

        assertDocumentsExist(of("/foo/bar/baz"));
    }

    @Test
    public void skipPropertyRemovedByLateWriteWithRelatedPath() throws Exception {
        // create a node with property.
        assumeTrue(fixture.hasSinglePersistence());
        NodeBuilder nb = store1.getRoot().builder();
        nb.child("bar").setProperty("prop", "value");
        store1.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        // unrelated path should be such that the paths and unrelated path shouldn't have common parent
        // for e.g. if path is /bar & unrelated is /d then there common ancestor is "/" i.e. root.
        lateWriteRemovePropertiesNodes(of("/bar"), "/d", "prop");

        assertDocumentsExist(of("/bar"));
        assertPropertyNotExist("/bar", "prop");

        enableDetailGC(store1.getVersionGarbageCollector());

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGCStats stats = gc(store1.getVersionGarbageCollector(), 1, HOURS);
        assertNotNull(stats);
        assertEquals(0, stats.deletedDocGCCount);
        // we should be able to remove the property since we have updated an related path that has lead to an update
        // of common ancestor and this would make late write visible
        assertEquals(1, stats.deletedPropsCount);

        assertDocumentsExist(of("/bar"));
    }
    // OAK-10676 END

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
        VersionGCStats stats = gc(gc, maxAge, HOURS);
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
        gc(gc, 24, HOURS);
        // before a fix the split doc is GCed (but can't make that an assert)
        //assertEquals(0, store.getDocumentStore()
        //        .query(NODES, "4:p/t/target/", "4:p/t/target/z", 5).size());

        // step 9 : make assertions about /t/target at root and checkpoint
        // invalidate node cache to ensure readNode/getNodeAtRevision is called below
        store1.getNodeCache().invalidateAll();
        assertTrue(store1.getRoot().getChildNode("t").getChildNode("target").exists());
        // invalidate node cache to ensure readNode/getNodeAtRevision is called below
        store1.getNodeCache().invalidateAll();
        assertFalse(requireNonNull(store1.retrieve(checkpoint)).getChildNode("t").getChildNode("target").exists());
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
        assertTrue(requireNonNull(store1.retrieve(checkpoint)).getChildNode("t").getChildNode("target").exists());

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
        assertFalse(requireNonNull(store1.retrieve(checkpoint)).getChildNode("t").getChildNode("target").exists());

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
        assertEquals(0, gc(gc, 24, HOURS).splitDocGCCount);

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
        assertEquals(exp, requireNonNull(store1.retrieve(checkpoint)).getChildNode("t").getString("foo"));

        // step 6 : wait for 1 week
        clock.waitUntil(clock.getTime() + TimeUnit.DAYS.toMillis(7));

        // step 7 : do another change that fulfills the split doc condition at t(+2w)
        b1 = store1.getRoot().builder();
        b1.child("t").setProperty("foo", "barZ");
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();
        assertEquals("barZ", store1.getRoot().getChildNode("t").getString("foo"));
        assertEquals(exp, requireNonNull(store1.retrieve(checkpoint)).getChildNode("t").getString("foo"));

        // step 8 : move the clock a couple seconds to ensure GC maxRev condition hits
        // (without this it might not yet GC the split doc we want it to,
        // as we'd be in the same rounded second) -> t(+2w:30s)
        clock.waitUntil(clock.getTime() + TimeUnit.SECONDS.toMillis(30));

        // step 9 : trigger another GC - previously split away the referenced revision
        assertEquals(0, gc(gc, 24, HOURS).splitDocGCCount);
        // flush the caches as otherwise it might deliver stale data
        store1.getNodeCache().invalidateAll();
        assertEquals("barZ", store1.getRoot().getChildNode("t").getString("foo"));
        assertEquals(exp, requireNonNull(store1.retrieve(checkpoint)).getChildNode("t").getString("foo"));
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
        VersionGCStats stats = gc(gc, maxAge, HOURS);
        assertEquals(10, stats.splitDocGCCount);
        assertEquals(0, stats.deletedLeafDocGCCount);

        DocumentNodeState test = getDoc("/test").getNodeAtRevision(store1, store1.getHeadRevision(), null);
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

        VersionGCStats stats = gc(gc, maxAge, HOURS);
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

        VersionGCStats stats = gc(gc, maxAge, HOURS);
        // one split doc each on: /foo, /bar and root document
        assertEquals(3, stats.splitDocGCCount);
        assertEquals(0, stats.deletedLeafDocGCCount);

        NodeDocument doc = getDoc("/foo");
        assertNotNull(doc);
        DocumentNodeState state = doc.getNodeAtRevision(store1, store1.getHeadRevision(), null);
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
        long start = getCurrentTimestamp();
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
                gc(gc, 1, HOURS);
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
        final VersionGarbageCollector gc = new VersionGarbageCollector(store1, gcSupport, false, false, false);
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
        VersionGCStats stats = gc(gc, maxAge, HOURS);
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
        gcRef.set(new VersionGarbageCollector(store1, gcSupport, false, false, false));
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
        gcRef.set(new VersionGarbageCollector(store1, gcSupport, false, false, false));
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
        final VersionGarbageCollector gc = new VersionGarbageCollector(store1, nonReportingGcSupport, false, false, false);
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
        gc(gc, maxAgeHours, HOURS);
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

        VersionGCStats stats = gc(gc, maxAge, HOURS);
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
        VersionGCStats stats = gc(gc, maxAge, HOURS);
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
        stats = gc(gc, maxAge, HOURS);
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

    // helper methods

    private void assertPropertyExist(String path, String propertyName) {
        Path p = Path.fromString(path);
        String id = Utils.getIdFromPath(p);
        assertNotNull(requireNonNull(store1.getDocumentStore().find(NODES, id, -1)).get(propertyName));
    }

    private void assertPropertyNotExist(String path, String propertyName) {
        Path p = Path.fromString(path);
        String id = Utils.getIdFromPath(p);
        SortedMap<Revision, String> valueMap = requireNonNull(store1.getDocumentStore().find(NODES, id, -1)).getLocalMap(propertyName);
        assertNull(valueMap.get(valueMap.firstKey()));
    }

    private static final Predicate<UpdateOp> ADD_NODE_OPS = updateOp -> {
        for (UpdateOp.Key key : updateOp.getChanges().keySet()) {
            if (isDeletedEntry(key.getName()) && updateOp.getChanges().get(key).value.equals("false")) {
                return true;
            }
        }
        return false;
    };

    private static final Predicate<UpdateOp> REMOVE_NODE_OPS = updateOp -> {
        for (UpdateOp.Key key : updateOp.getChanges().keySet()) {
            if (isDeletedEntry(key.getName()) && updateOp.getChanges().get(key).value.equals("true")) {
                return true;
            }
        }
        return false;
    };

    private static FailedUpdateOpListener filter0(List<UpdateOp> ops, Predicate<UpdateOp> predicate) {
        return op -> {
            if (predicate.test(op)) {
                ops.add(op);
            }
        };
    }

    private static Predicate<UpdateOp> setPropertyOps(String propertyName) {
        return updateOp -> {
            for (UpdateOp.Key key : updateOp.getChanges().keySet()) {
                if (propertyName.equals(key.getName()) && updateOp.getChanges().get(key).type == SET_MAP_ENTRY) {
                    return true;
                }
            }
            return false;
        };
    }

    private static NodeState getChildeNodeState(DocumentNodeStore ns2, String path, boolean assertIntermediatesExist) {
        final Path p = Path.fromString(path);
        NodeState state = ns2.getRoot();
        for (String name : p.elements()) {
            state = state.getChildNode(name);
            if (assertIntermediatesExist) {
                assertTrue(state.exists());
            }
        }
        return state;
    }

    private void assertDocumentsDontExist(Collection<String> nonExistingPaths) {
        for (String nonExistingPath : nonExistingPaths) {
            Path p = Path.fromString(nonExistingPath);
            assertFalse(getChildeNodeState(store1, nonExistingPath, false).exists());
            String id = Utils.getIdFromPath(p);
            assertNull(store1.getDocumentStore().find(NODES, id));
        }
    }

    private void assertDocumentsExist(Collection<String> paths) {
        for (String aPath : paths) {
            Path p = Path.fromString(aPath);
            String id = Utils.getIdFromPath(p);
            assertNotNull(store1.getDocumentStore().find(NODES, id, -1));
        }
    }

    private void createNodes(Collection<String> paths) throws Exception {
        createNodes(paths.toArray(new String[0]));
    }

    private void createNodes(String... paths) throws CommitFailedException {
        createNodes(store1, paths);
    }

    private void createNodes(DocumentNodeStore dns,
                             String... paths) throws CommitFailedException {
        for (String path : paths) {
            merge(dns, createChild(dns.getRoot().builder(), path));
        }
        dns.runBackgroundOperations();
    }

    interface LateWriteChangesBuilder {
        void apply(NodeBuilder root, String path);
    }

    private void lateWriteCreateNodes(Collection<String> orphanedPaths,
                                      String unrelatedPathOrNull) throws Exception {
        lateWrite(orphanedPaths, TestUtils::createChild,
                unrelatedPathOrNull, ADD_NODE_OPS, (ds, ops) -> ds.createOrUpdate(NODES, ops));
    }

    private void lateWriteRemoveNodes(Collection<String> orphanedPaths,
                                      String unrelatedPathOrNull) throws Exception {
        lateWrite(orphanedPaths, (rb, path) -> childBuilder(rb, path).remove(),
                unrelatedPathOrNull, REMOVE_NODE_OPS, (ds, ops) -> ds.createOrUpdate(NODES, ops));
    }

    private void lateWriteAddPropertiesNodes(Collection<String> paths, String unrelatedPath, String propertyName,
                                             String propertyValue) throws Exception {
        lateWrite(paths, (rb, path) -> childBuilder(rb, path).setProperty(propertyName, propertyValue), unrelatedPath,
                setPropertyOps(propertyName), (ds, ops) -> ds.findAndUpdate(NODES, ops));
    }

    private void lateWriteRemovePropertiesNodes(Collection<String> paths, String unrelatedPath, String propertyName)
            throws Exception {
        lateWrite(paths, (rb, path) -> childBuilder(rb, path).removeProperty(propertyName), unrelatedPath,
                setPropertyOps(propertyName), (ds, ops) -> ds.findAndUpdate(NODES, ops));
    }

    /**
     *
     * Add late write properties on given paths. Assumes the secondary store is not
     * in use as it needs to control its creation and disposal.
     *
     * @param paths paths on which we need to update the property
     * @param lateWriteChangesBuilder builder to create lateWrite operations
     * @param unrelatedPath this path needs to be totally unrelated to above paths i.e. they shouldn't have common parent
     * @param filterPredicate to filter operations on FallingDocumentStore
     * @param dataStoreConsumer persist late write changes to DocumentStore
     * @throws Exception in case of merge failure we throw exception
     */
    private void lateWrite(Collection<String> paths, LateWriteChangesBuilder lateWriteChangesBuilder, String unrelatedPath,
                           Predicate<UpdateOp> filterPredicate, BiConsumer<DocumentStore, List<UpdateOp>> dataStoreConsumer) throws Exception {
        // this method requires store2 to be null as a prerequisite
        assertNull(store2);
        // as it creates store2 itself - then disposes it later too
        createSecondaryStore(LeaseCheckMode.LENIENT, true);
        // create the orphaned paths
        final List<UpdateOp> failed = new ArrayList<>();
        final FailingDocumentStore fds = (FailingDocumentStore) ds2;
        fds.addListener(filter0(failed, filterPredicate));
        fds.fail().after(0).eternally();
        for (String path : paths) {
            try {
                NodeBuilder rb = store2.getRoot().builder();
                lateWriteChangesBuilder.apply(rb, path);
                merge(store2, rb);
                fail("merge must fail");
            } catch (CommitFailedException e) {
                // expected
                String msg = e.getMessage();
                e.printStackTrace();
                assertEquals("OakOak0001: write operation failed", msg);
            }
        }
        disposeQuietly(store2);
        fds.fail().never();

        // wait until lease expires
        clock.waitUntil(clock.getTime() + DEFAULT_LEASE_DURATION_MILLIS + 1);

        {
            store1.renewClusterIdLease();
            assertTrue(store1.getLastRevRecoveryAgent().isRecoveryNeeded());
            assertEquals(0, store1.getLastRevRecoveryAgent().recover(2));
        }

        // 'late write'
        dataStoreConsumer.accept(fds, failed);

        if (unrelatedPath == null || unrelatedPath.isEmpty()) {
            return;
        }

        // revive clusterId 2
        createSecondaryStore(LeaseCheckMode.LENIENT);
        merge(store2, createChild(store2.getRoot().builder(), unrelatedPath));
        store2.runBackgroundOperations();
        store2.dispose();
        store1.runBackgroundOperations();
    }

    /**
     * Creates a bunch of parents properly, then creates a bunch of orphans in
     * late-write manner (i.e. not properly), then runs DetailedGC and assets that
     * everything was deleted as expected
     *
     * @param parents                 the nodes that should be created properly -
     *                                each one in a separate merge
     * @param orphans                 the nodes that should be created inproperly -
     *                                each one in a separate late-write way
     * @param expectedNumOrphanedDocs the expected number of orphan documents that
     *                                DetailedGC should cleanup
     * @param unrelatedPath           an unrelated path that should be merged after
     *                                late-write - ensures lastRev is updated on
     *                                root to allow detecting late-writes as such
     */
    private void doLateWriteCreateChildrenGC(Collection<String> parents,
                                             Collection<String> orphans, int expectedNumOrphanedDocs, String unrelatedPath)
            throws Exception {
        assumeTrue(fixture.hasSinglePersistence());
        createNodes(parents);
        lateWriteCreateNodes(orphans, unrelatedPath);

        assertDocumentsExist(parents);
        assertDocumentsExist(orphans);
        assertNodesDontExist(parents, orphans);

        enableDetailGC(store1.getVersionGarbageCollector());

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGCStats stats = gc(store1.getVersionGarbageCollector(), 1, HOURS);
        assertNotNull(stats);
        assertEquals(expectedNumOrphanedDocs, stats.deletedDocGCCount);

        assertDocumentsExist(parents);
        // and the main assert being: have those lateCreated (orphans) docs been deleted
        assertNodesDontExist(parents, orphans);
        assertDocumentsDontExist(orphans);
    }

    private void assertNodesDontExist(Collection<String> existingNodes,
                                      Collection<String> missingNodes) {
        for (String aMissingNode : missingNodes) {
            assertChildNotExists(existingNodes, aMissingNode);
        }
    }

    private void assertChildNotExists(Collection<String> existingNodes, String aMissingNode) {
        final Path aMissingPath = Path.fromString(aMissingNode);
        String nearestParent = null;
        Path nearestParentPath = null;
        for (String anExistingNode : existingNodes) {
            final Path anExistingPath = Path.fromString(anExistingNode);
            if (!anExistingPath.isAncestorOf(aMissingPath)) {
                // skip
                continue;
            }
            if (nearestParent == null || nearestParentPath.isAncestorOf(anExistingPath)) {
                nearestParent = anExistingNode;
                nearestParentPath = anExistingPath;
            }
        }
        assertNotNull(nearestParent);
        Path nearestChildPath = aMissingPath;
        Path childParentPath = nearestChildPath.getParent();
        while(nearestParentPath.isAncestorOf(childParentPath)) {
            nearestChildPath = childParentPath;
            childParentPath = childParentPath.getParent();
        }
        assertFalse(getChildeNodeState(store1, nearestParent, true).hasChildNode(nearestChildPath.getName()));
    }

    private void createLeaf(DocumentNodeStore s, String... pathElems) throws Exception {
        createOrDeleteLeaf(s, false, pathElems);
    }

    private void deleteLeaf(DocumentNodeStore s, String... pathElems) throws Exception {
        createOrDeleteLeaf(s, true, pathElems);
    }

    private void createOrDeleteLeaf(DocumentNodeStore s, boolean delete, String... pathElems) throws Exception {
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
