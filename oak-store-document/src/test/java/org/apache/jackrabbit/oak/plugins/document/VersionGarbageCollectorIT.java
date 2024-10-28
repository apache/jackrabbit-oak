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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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
import java.util.function.Predicate;
import java.util.stream.StreamSupport;

import static java.util.List.of;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.reflect.FieldUtils.readField;
import static org.apache.commons.lang3.reflect.FieldUtils.writeField;
import static org.apache.commons.lang3.reflect.FieldUtils.writeStaticField;

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
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService.DEFAULT_FGC_BATCH_SIZE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService.DEFAULT_FGC_DELAY_FACTOR;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService.DEFAULT_FGC_PROGRESS_SIZE;
import static org.apache.jackrabbit.oak.plugins.document.FullGCHelper.assertBranchRevisionNotRemovedFromAllDocuments;
import static org.apache.jackrabbit.oak.plugins.document.FullGCHelper.assertBranchRevisionRemovedFromAllDocuments;
import static org.apache.jackrabbit.oak.plugins.document.FullGCHelper.enableFullGC;
import static org.apache.jackrabbit.oak.plugins.document.FullGCHelper.enableFullGCDryRun;
import static org.apache.jackrabbit.oak.plugins.document.FullGCHelper.mergedBranchCommit;
import static org.apache.jackrabbit.oak.plugins.document.FullGCHelper.unmergedBranchCommit;
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
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_FULL_GC_DOCUMENT_ID_PROP;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_FULL_GC_DRY_RUN_DOCUMENT_ID_PROP;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_FULL_GC_DRY_RUN_TIMESTAMP_PROP;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.SETTINGS_COLLECTION_FULL_GC_TIMESTAMP_PROP;
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

import org.apache.jackrabbit.guava.common.cache.Cache;
import org.apache.jackrabbit.guava.common.collect.AbstractIterator;
import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.guava.common.collect.Queues;
import com.mongodb.ReadPreference;

import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture.RDBFixture;
import org.apache.jackrabbit.oak.plugins.document.FailingDocumentStore.FailedUpdateOpListener;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.FullGCMode;
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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(Parameterized.class)
public class VersionGarbageCollectorIT {

    @Rule
    public TestName name = new TestName();

    private static final Logger LOG = LoggerFactory.getLogger(VersionGarbageCollectorIT.class);

    static class GCCounts {
        private final FullGCMode mode;
        int deletedDocGCCount, deletedPropsCount, deletedInternalPropsCount,
                deletedPropRevsCount, deletedInternalPropRevsCount,
                deletedUnmergedBCCount, updatedFullGCDocsCount;

        public GCCounts(FullGCMode mode) {
            this(mode, 0, 0, 0, 0, 0, 0, 0);
        }

        public GCCounts(FullGCMode mode, int deletedDocGCCount, int deletedPropsCount,
                        int deletedInternalPropsCount, int deletedPropRevsCount,
                        int deletedInternalPropRevsCount, int deletedUnmergedBCCount,
                        int updatedFullGCDocsCount) {
            this.mode = mode;
            assertTrue(deletedDocGCCount != -1);
            assertTrue(deletedPropsCount != -1);
            assertTrue(deletedInternalPropsCount != -1);
            assertTrue(deletedPropRevsCount != -1);
            assertTrue(deletedInternalPropRevsCount != -1);
            assertTrue(deletedUnmergedBCCount != -1);
            assertTrue(updatedFullGCDocsCount != -1);
            if (mode == FullGCMode.GAP_ORPHANS
                    || mode == FullGCMode.GAP_ORPHANS_EMPTYPROPS
                    || mode == FullGCMode.ALL_ORPHANS_EMPTYPROPS) {
                assertEquals(0, deletedPropRevsCount);
                assertEquals(0, deletedInternalPropRevsCount);
                assertEquals(0, deletedUnmergedBCCount);
            } else if (mode == FullGCMode.ORPHANS_EMPTYPROPS_KEEP_ONE_USER_PROPS) {
                assertEquals(0, deletedInternalPropsCount);
                assertEquals(0, deletedInternalPropRevsCount);
            }
            this.deletedDocGCCount = deletedDocGCCount;
            this.deletedPropsCount = deletedPropsCount;
            this.deletedInternalPropsCount = deletedInternalPropsCount;
            this.deletedPropRevsCount = deletedPropRevsCount;
            this.deletedInternalPropRevsCount = deletedInternalPropRevsCount;
            this.deletedUnmergedBCCount = deletedUnmergedBCCount;
            this.updatedFullGCDocsCount = updatedFullGCDocsCount;
        }
    }

    @Parameter(0)
    public DocumentStoreFixture fixture;

    @Parameter(1)
    public FullGCMode fullGcMode;

    private Clock clock;

    private DocumentMK.Builder documentMKBuilder;

    private DocumentStore ds1, ds2;

    private DocumentNodeStore store1, store2;

    private List<DocumentStore> documentStoresToBeDisposedAfter = new ArrayList<>();

    private VersionGarbageCollector gc;

    private ExecutorService execService;

    private FullGCMode originalFullGcMode;

    private long startTime = System.currentTimeMillis();

    @Parameterized.Parameters(name="{index}: {0} with {1}")
    public static java.util.Collection<Object[]> params() throws IOException {
        java.util.Collection<Object[]> params = new LinkedList<>();
        for (Object[] fixture : AbstractDocumentStoreTest.fixtures()) {
            DocumentStoreFixture f = (DocumentStoreFixture)fixture[0];
            for (FullGCMode gcType : FullGCMode.values()) {
                if (gcType == FullGCMode.ORPHANS_EMPTYPROPS_BETWEEN_CHECKPOINTS_NO_UNMERGED_BC
                        || gcType == FullGCMode.ORPHANS_EMPTYPROPS_BETWEEN_CHECKPOINTS_WITH_UNMERGED_BC) {
                    // temporarily skip due to flakyness
                    continue;
                }
                if (f.getName().equals("Memory") || f.getName().startsWith("RDB")) {
                    // then only run NONE and EMPTY_PROPS, cause we are rolling EMPTY_PROPS first
                    if (gcType != FullGCMode.NONE && gcType != FullGCMode.EMPTYPROPS
                            && gcType != FullGCMode.GAP_ORPHANS_EMPTYPROPS) {
                        // temporarily skip due to slowness
                        continue;
                    }
                }
                params.add(new Object[] {f, gcType});
            }
        }
        return params;
    }

    @Before
    public void setUp() throws Exception {
        LOG.info("setUp: START. fullGcMode = {}, fixture = {}", fullGcMode, fixture);
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

        originalFullGcMode = VersionGarbageCollector.getFullGcMode();
        writeStaticField(VersionGarbageCollector.class, "fullGcMode", fullGcMode, true);
        LOG.info("setUp: DONE. fullGcMode = {}, test = {}", fullGcMode, name.getMethodName());
    }

    @After
    public void tearDown() throws Exception {
        LOG.info("tearDown: START. fullGcMode = {}, fixture = {}", fullGcMode, fixture);
        writeStaticField(VersionGarbageCollector.class, "fullGcMode", originalFullGcMode, true);
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
        for (DocumentStore ds : documentStoresToBeDisposedAfter) {
            try {
                LOG.info("Try to dispose {} used in test {}", ds, name.getMethodName());
                ds.dispose();
                LOG.info("Disposed {} used in test {}", ds, name.getMethodName());
            } catch (Throwable e) {
                LOG.error("Failed to dispose" + ds, e);
            }
        }

        long elapsed = System.currentTimeMillis() - startTime;
        LOG.info("tearDown: DONE. fullGcMode = {}, test = {}, elapsed = {}ms ({})", fullGcMode, name.getMethodName(), elapsed,
                Duration.ofMillis(elapsed));
    }

    private final String internalRdbTablePrefix = "VGCIT" + Long.toHexString(startTime);

    private String getRdbTablePrefix() {
        // log which test case used which table prefix in case some leak
        LOG.info("RDB table prefix for {} is {}", name.getMethodName(), internalRdbTablePrefix);
        return internalRdbTablePrefix;
    }

    private void createPrimaryStore() {
        if (fixture instanceof RDBFixture) {
            ((RDBFixture) fixture).setRDBOptions(
                    new RDBOptions().tablePrefix(getRdbTablePrefix()).dropTablesOnClose(true));
        }
        ds1 = fixture.createDocumentStore();
        documentMKBuilder = new DocumentMK.Builder().clock(clock).setClusterId(1)
                .setLeaseCheckMode(LeaseCheckMode.DISABLED)
                .setDocumentStore(ds1).setAsyncDelay(0);
        store1 = documentMKBuilder.getNodeStore();
    }

    private void createSecondaryStore(LeaseCheckMode leaseCheckNode)
            throws IllegalAccessException {
        createSecondaryStore(leaseCheckNode, false);
    }

    private void createSecondaryStore(LeaseCheckMode leaseCheckNode, boolean withFailingDS)
            throws IllegalAccessException {
        LOG.info("createSecondaryStore: creating secondary store with leaseCheckNode = {}, withFailingDS = {}",
                leaseCheckNode, withFailingDS);
        if (fixture instanceof RDBFixture) {
            ((RDBFixture) fixture).setRDBOptions(
                    // dropping the tables not needed here because done for the primary store
                    new RDBOptions().tablePrefix(getRdbTablePrefix()).dropTablesOnClose(false));
        }
        ds2 = fixture.createDocumentStore();
        if (withFailingDS) {
            documentStoresToBeDisposedAfter.add(ds2);
            FailingDocumentStore failingDs = new FailingDocumentStore(ds2);
            failingDs.noDispose();
            ds2 = failingDs;
        }
        DocumentMK.Builder documentMKBuilder2 = new DocumentMK.Builder().clock(clock).setClusterId(2)
                .setLeaseCheckMode(leaseCheckNode)
                .setDocumentStore(ds2).setAsyncDelay(0);
        store2 = documentMKBuilder2.getNodeStore();
        // custom fullGcMode needs to be set after each node store creation, since the VersionGarbageCollector
        // constructor sets the fullGcMode to the value read from OSGI Configuration - method setFullGcMode
        writeStaticField(VersionGarbageCollector.class, "fullGcMode", fullGcMode, true);
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
        assertFalse(stats.ignoredFullGCDueToCheckPoint);
        assertFalse(stats.fullGCDryRunMode);
        assertTrue(stats.canceled);

        //Fast forward time to future such that checkpoint get expired
        clock.waitUntil(clock.getTime() + expiryTime + 1);
        stats = gc(gc, maxAge, TimeUnit.MILLISECONDS);
        assertFalse("GC should be performed", stats.ignoredGCDueToCheckPoint);
        assertFalse("Full GC shouldn't be performed", stats.ignoredFullGCDueToCheckPoint);
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
        clock.waitUntil(Revision.getCurrentTimestamp() + maxAge);
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
        gcSplitDocsInternal("sub".repeat(120));
    }

    private VersionGCStats gc(VersionGarbageCollector gc, long maxRevisionAge, TimeUnit unit) throws IOException {
        final VersionGCStats stats = gc.gc(maxRevisionAge, unit);
        if (stats.skippedFullGCDocsCount != 0) {
            (new Exception("here: " + stats.skippedFullGCDocsCount)).printStackTrace(System.out);
        }
        assertEquals(0, stats.skippedFullGCDocsCount);
        return stats;
    }

    // OAK-10199
    @Test
    public void testFullGCNeedRepeat() throws Exception {
        long expiryTime = 5001, maxAge = 20, batchSize = /*PROGRESS_BATCH_SIZE+1*/ 10001;
        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);

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
        assertFalse("Full GC should be performed", stats.ignoredFullGCDueToCheckPoint);
        assertFalse(stats.canceled);
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(0, (int)batchSize, 0, 0, 0, 0, (int)batchSize),
                gapOrphProp(0, (int)batchSize, 0, 0, 0, 0, (int)batchSize),
                allOrphProp(0, (int)batchSize, 0, 0, 0, 0, (int)batchSize),
                keepOneFull(0, (int)batchSize, 0, 0, 0, 0, (int)batchSize),
                keepOneUser(0, (int)batchSize, 0, 0, 0, 0, (int)batchSize),
                unmergedBcs(0, (int)batchSize, 0, 0, 0, 0, (int)batchSize),
                betweenChkp(0, (int)batchSize, 0, 0, 2, 0, (int)batchSize + 1),
                btwnChkpUBC(0, (int)batchSize, 0, 0, 2, 0, (int)batchSize + 1));
        assertFalse(stats.needRepeat);
    }

    // OAK-10199
    @Test
    public void fullGCIgnoredForCheckpoint() throws Exception {
        long expiryTime = 100, maxAge = 20;
        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);

        Revision cp = Revision.fromString(store1.checkpoint(expiryTime));

        //Fast forward time to future but before expiry of checkpoint
        clock.waitUntil(cp.getTimestamp() + expiryTime - maxAge);
        VersionGCStats stats = gc(gc, maxAge, TimeUnit.MILLISECONDS);
        assertTrue(stats.ignoredFullGCDueToCheckPoint);
        assertTrue(stats.canceled);

        //Fast forward time to future such that checkpoint get expired
        clock.waitUntil(clock.getTime() + expiryTime + 1);
        stats = gc(gc, maxAge, TimeUnit.MILLISECONDS);
        assertFalse("Full GC should be performed", stats.ignoredFullGCDueToCheckPoint);
        assertFalse(stats.canceled);
    }

    @Test
    public void testFullGCNotIgnoredForRGCCheckpoint() throws Exception {

        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);

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
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(0, 1, 0, 0, 0, 0, 1),
                gapOrphProp(0, 1, 0, 0, 0, 0, 1),
                allOrphProp(0, 1, 0, 0, 0, 0, 1),
                keepOneFull(0, 1, 0, 0, 0, 0, 1),
                keepOneUser(0, 1, 0, 0, 0, 0, 1),
                unmergedBcs(0, 1, 0, 0, 0, 0, 1),
                betweenChkp(0, 1, 0, 0, 0, 0, 1),
                btwnChkpUBC(0, 1, 0, 0, 0, 0, 1));
        assertTrue(stats.ignoredGCDueToCheckPoint);
        assertTrue(stats.ignoredFullGCDueToCheckPoint);
        assertTrue(stats.canceled);
    }

    @Test
    public void testGCDeletedLongPathPropsInclExcl_excludes() throws Exception {
        String longName = "p".repeat(PATH_LONG + 1);
        createEmptyProps("/a/b/" + longName + "/x", "/b/c/" + longName + "/x",
                "/c/d/" + longName + "/x");
        setGCIncludeExcludes(Set.of(), Set.of("/b/c", "/c"));
        doTestDeletedPropsGC(1, 1);
    }

    @Test
    public void testGCDeletedPropsInclExcl_oneInclude() throws Exception {
        createEmptyProps("/a/b/c", "/b/c/d", "/c/d/e");
        setGCIncludeExcludes(Set.of("/a"), Set.of());
        doTestDeletedPropsGC(1, 1);
    }

    @Test
    public void testGCDeletedPropsInclExcl_twoIncludes() throws Exception {
        createEmptyProps("/a/b/c", "/b/c/d", "/c/d/e");
        setGCIncludeExcludes(Set.of("/a", "/c"), Set.of());
        doTestDeletedPropsGC(2, 2);
    }

    @Test
    public void testGCDeletedPropsInclExcl_inclAndExcl() throws Exception {
        createEmptyProps("/a/b/c", "/b/c/d", "/c/d/e");
        setGCIncludeExcludes(Set.of("/a", "/c"), Set.of("/c/d"));
        doTestDeletedPropsGC(1, 1);
    }

    @Test
    public void testGCDeletedPropsInclExcl_excludes() throws Exception {
        createEmptyProps("/a/b/c", "/b/c/d", "/c/d/e");
        setGCIncludeExcludes(Set.of(), Set.of("/b", "/c"));
        doTestDeletedPropsGC(1, 1);
    }

    @Test
    public void testGCDeletedPropsInclExcl_emptyEmpty() throws Exception {
        createEmptyProps("/a/b/c", "/b/c/d", "/c/d/e");
        setGCIncludeExcludes(Collections.emptySet(), Collections.emptySet());
        doTestDeletedPropsGC(3, 3);
    }

    private void setGCIncludeExcludes(Set<String> includes, Set<String> excludes) {
        gc.setFullGCPaths(requireNonNull(includes), requireNonNull(excludes));
    }

    private void doTestDeletedPropsGC(int deletedPropsCount, int updatedDocsCount)
            throws Exception {
        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);
        long maxAge = 1; //hours
        clock.waitUntil(getCurrentTimestamp() + TimeUnit.HOURS.toMillis(maxAge));
        VersionGCStats stats = gc(gc, maxAge, HOURS);
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(0, deletedPropsCount, 0, 0, 0, 0, updatedDocsCount),
                gapOrphProp(0, deletedPropsCount, 0, 0, 0, 0, updatedDocsCount),
                allOrphProp(0, deletedPropsCount, 0, 0, 0, 0, updatedDocsCount),
                keepOneFull(0, deletedPropsCount, 0, 0, 0, 0, updatedDocsCount),
                keepOneUser(0, deletedPropsCount, 0, 0, 0, 0, updatedDocsCount),
                unmergedBcs(0, deletedPropsCount, 0, 0, 0, 0, updatedDocsCount),
                betweenChkp(0, deletedPropsCount, 0, 0, 3, 0, updatedDocsCount),
                btwnChkpUBC(0, deletedPropsCount, 0, 0, 3, 0, updatedDocsCount));
    }

    /**
     * Utility method to create empty properties, meaning they
     * are created, then removed again. That leaves them
     * as not existing properties, except they still exist
     * in the document. FullGC can then remove them after
     * a certain amount of time
     */
    private void createEmptyProps(String... paths) throws CommitFailedException {
        // 1. create nodes with properties
        NodeBuilder rb1 = store1.getRoot().builder();
        for (String path : paths) {
            NodeBuilder b1 = rb1;
            for (String name : Path.fromString(path).elements()) {
                b1 = b1.child(name);
            }
            b1.setProperty("foo", "bar", STRING);
        }
        store1.merge(rb1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        // 2. refresh the builder
        rb1 = store1.getRoot().builder();
        // 3. empty the properties
        for (String path : paths) {
            NodeBuilder b1 = rb1;
            for (String name : Path.fromString(path).elements()) {
                b1 = b1.child(name);
            }
            b1.removeProperty("foo");
        }
        store1.merge(rb1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    @Test
    public void testGCDeletedProps() throws Exception {
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

        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);
        long maxAge = 1; //hours
        long delta = MINUTES.toMillis(10);
        //1. Go past GC age and check no GC done as nothing deleted
        clock.waitUntil(getCurrentTimestamp() + maxAge);
        VersionGCStats stats = gc(gc, maxAge, HOURS);
        assertStatsCountsZero(stats);

        //Remove property
        NodeBuilder b2 = store1.getRoot().builder();
        b2.getChildNode("z").removeProperty("prop");
        store1.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        store1.runBackgroundOperations();

        //2. Check that a deleted property is not collected before maxAge
        //Clock cannot move back (it moved forward in #1) so double the maxAge
        clock.waitUntil(clock.getTime() + delta);
        stats = gc(gc, maxAge*2, HOURS);
        assertStatsCountsZero(stats);

        //3. Check that deleted property does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);

        stats = gc(gc, maxAge*2, HOURS);
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(0, 1, 0, 0, 0, 0, 1),
                gapOrphProp(0, 1, 0, 0, 0, 0, 1),
                allOrphProp(0, 1, 0, 0, 0, 0, 1),
                keepOneFull(0, 1, 0, 0, 0, 0, 1),
                keepOneUser(0, 1, 0, 0, 0, 0, 1),
                unmergedBcs(0, 1, 0, 0, 0, 0, 1),
                betweenChkp(0, 1, 0, 0, 3, 0, 2),
                btwnChkpUBC(0, 1, 0, 0, 3, 0, 2));
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
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(),
                gapOrphProp(),
                allOrphProp(),
                keepOneFull(0, 0, 0, 2, 0, 0, 1),
                keepOneUser(0, 0, 0, 2, 0, 0, 1),
                unmergedBcs(),
                betweenChkp(0, 0, 0, 1, 0, 0, 1),
                btwnChkpUBC(0, 0, 0, 1, 0, 0, 1));
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

        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);
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
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(0, 50_000, 0, 0, 0, 0, 5_000),
                gapOrphProp(0, 50_000, 0, 0, 0, 0, 5_000),
                allOrphProp(0, 50_000, 0, 0, 0, 0, 5_000),
                keepOneFull(0, 50_000, 0, 0, 0, 0, 5_000),
                keepOneUser(0, 50_000, 0, 0, 0, 0, 5_000),
                unmergedBcs(0, 50_000, 0, 0, 0, 0, 5_000),
                betweenChkp(0, 50_000, 0, 0, 2, 0, 5_000 + 1),
                btwnChkpUBC(0, 50_000, 0, 0, 2, 0, 5_000 + 1));
        assertEquals(MIN_ID_VALUE, stats.oldestModifiedDocId);
    }

    @Test
    public void testGCDeletedProps_MoreThan_1000_WithDifferentRevision() throws Exception {
        //1. Create nodes with properties
        NodeBuilder b1 = store1.getRoot().builder();
        for (int k = 0; k < 50; k ++) {
            // Add property to node & save
            for (int i = 0; i < 100; i++) {
                for (int j = 0; j < 10; j++) {
                    b1.child(k + "z" + i).setProperty("prop" + j, "foo", STRING);
                }
            }
        }
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);
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

        store1.runBackgroundOperations();
        //3. Check that deleted property does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);

        VersionGCStats stats = gc(gc, maxAge, HOURS);
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(0, 50_000, 0, 0, 0, 0, 5_000),
                gapOrphProp(0, 50_000, 0, 0, 0, 0, 5_000),
                allOrphProp(0, 50_000, 0, 0, 0, 0, 5_000),
                keepOneFull(0, 50_000, 0, 0, 0, 0, 5_000),
                keepOneUser(0, 50_000, 0, 0, 0, 0, 5_000),
                unmergedBcs(0, 50_000, 0, 0, 0, 0, 5_000),
                betweenChkp(0, 50_000, 0, 0, 51, 0, 5_000 + 1),
                btwnChkpUBC(0, 50_000, 0, 0, 51, 0, 5_000 + 1));
        assertEquals(MIN_ID_VALUE, stats.oldestModifiedDocId);
    }

    @Test
    @Ignore(value = "OAK-10844 ignoring due to slowness")
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

        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);
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
            assertEquals(document.get(SETTINGS_COLLECTION_FULL_GC_TIMESTAMP_PROP), oldestModifiedDocTimeStamp);
            assertEquals(document.get(SETTINGS_COLLECTION_FULL_GC_DOCUMENT_ID_PROP), oldestModifiedDocId);
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

        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);
        long maxAge = 1; //hours
        long delta = MINUTES.toMillis(10);
        //1. Go past GC age and check no GC done as nothing deleted
        clock.waitUntil(getCurrentTimestamp() + maxAge);
        VersionGCStats stats = gc(gc, maxAge, HOURS);
        assertStatsCountsZero(stats);

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
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(0, 10, 0, 0, 0, 0, 10),
                gapOrphProp(0, 10, 0, 0, 0, 0, 10),
                allOrphProp(0, 10, 0, 0, 0, 0, 10),
                keepOneFull(0, 10, 0, 0, 0, 0, 10),
                keepOneUser(0, 10, 0, 0, 0, 0, 10),
                unmergedBcs(0, 10, 0, 0, 0, 0, 10),
                betweenChkp(0, 10, 0, 0, 2, 0, 11),
                btwnChkpUBC(0, 10, 0, 0, 2, 0, 11));
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
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(0, 10, 0, 0, 0, 0, 10),
                gapOrphProp(0, 10, 0, 0, 0, 0, 10),
                allOrphProp(0, 10, 0, 0, 0, 0, 10),
                keepOneFull(0, 10, 0, 0, 0, 0, 10),
                keepOneUser(0, 10, 0, 0, 0, 0, 10),
                unmergedBcs(0, 10, 0, 0, 0, 0, 10),
                betweenChkp(0, 10, 0, 0, 2, 0, 11),
                btwnChkpUBC(0, 10, 0, 0, 2, 0, 11));
        assertEquals(MIN_ID_VALUE, stats.oldestModifiedDocId);
    }

    @Test
    public void testGCDeletedPropsAfterSystemCrash() throws Exception {
        if (store1 != null) {
            store1.dispose();
        }

        DocumentStore hiddenStore = fixture.createDocumentStore();
        documentStoresToBeDisposedAfter.add(hiddenStore);
        final FailingDocumentStore fds = new FailingDocumentStore(hiddenStore, 42) {
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

        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);
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
        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);

        //4. Check that deleted property does get collected again
        // increment the clock again by more than 2 hours + delta
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);
        VersionGCStats stats = gc(gc, maxAge*2, HOURS);
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(0, 10, 0, 0, 0, 0, 10),
                gapOrphProp(0, 10, 0, 0, 0, 0, 10),
                allOrphProp(0, 10, 0, 0, 0, 0, 10),
                keepOneFull(0, 10, 0, 0, 0, 0, 10),
                keepOneUser(0, 10, 0, 0, 0, 0, 10),
                unmergedBcs(0, 10, 0, 0, 0, 0, 10),
                betweenChkp(0, 10, 0, 0, 2, 0, 11),
                btwnChkpUBC(0, 10, 0, 0, 2, 0, 11));
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

        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);
        long maxAge = 1; //hours
        long delta = MINUTES.toMillis(10);
        //1. Go past GC age and check no GC done as nothing deleted
        clock.waitUntil(getCurrentTimestamp() + maxAge);
        VersionGCStats stats = gc(gc, maxAge, HOURS);
        assertStatsCountsZero(stats);

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
        assertStatsCountsZero(stats);

        //3. Check that deleted property does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);

        stats = gc(gc, maxAge*2, HOURS);
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(0, 10, 0, 0, 0, 0, 1),
                gapOrphProp(0, 10, 0, 0, 0, 0, 1),
                allOrphProp(0, 10, 0, 0, 0, 0, 1),
                keepOneFull(0, 10, 0, 0, 0, 0, 1),
                keepOneUser(0, 10, 0, 0, 0, 0, 1),
                unmergedBcs(0, 10, 0, 0, 0, 0, 1),
                betweenChkp(0, 10, 0, 0, 1, 0, 2),
                btwnChkpUBC(0, 10, 0, 0, 1, 0, 2));

        //4. Check that a revived property (deleted and created again) does not get gc
        NodeBuilder b4 = store1.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            b4.child("x").setProperty("test."+i, "t", STRING);
        }
        store1.merge(b4, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);
        stats = gc(gc, maxAge*2, HOURS);
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(),
                gapOrphProp(),
                allOrphProp(),
                keepOneFull(),
                keepOneUser(),
                unmergedBcs(),
                betweenChkp(0, 0, 0, 0, 1, 0, 1),
                btwnChkpUBC(0, 0, 0, 0, 1, 0, 1));
    }

    @Test
    public void testGCDeletedLongPathProps() throws Exception {
        //1. Create nodes with properties
        NodeBuilder b1 = store1.getRoot().builder();
        String longPath = "p".repeat(PATH_LONG + 1);
        b1.child(longPath);

        // Add property to node & save
        for (int i = 0; i < 10; i++) {
            b1.child(longPath).child("foo").setProperty("test"+i, "t", STRING);
        }
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);
        long maxAge = 1; //hours
        long delta = MINUTES.toMillis(10);
        //1. Go past GC age and check no GC done as nothing deleted
        clock.waitUntil(getCurrentTimestamp() + maxAge);
        VersionGCStats stats = gc(gc, maxAge, HOURS);
        assertStatsCountsZero(stats);

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
        assertStatsCountsZero(stats);

        //3. Check that deleted property does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);

        stats = gc(gc, maxAge*2, HOURS);
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(0, 10, 0, 0, 0, 0, 1),
                gapOrphProp(0, 10, 0, 0, 0, 0, 1),
                allOrphProp(0, 10, 0, 0, 0, 0, 1),
                keepOneFull(0, 10, 0, 0, 0, 0, 1),
                keepOneUser(0, 10, 0, 0, 0, 0, 1),
                unmergedBcs(0, 10, 0, 0, 0, 0, 1),
                betweenChkp(0, 10, 0, 0, 1, 0, 2),
                btwnChkpUBC(0, 10, 0, 0, 1, 0, 2));

        //4. Check that a revived property (deleted and created again) does not get gc
        NodeBuilder b4 = store1.getRoot().builder();
        for (int i = 0; i < 10; i++) {
            b4.child(longPath).child("foo").setProperty("test"+i, "t", STRING);
        }
        store1.merge(b4, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);
        stats = gc(gc, maxAge*2, HOURS);
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(),
                gapOrphProp(),
                allOrphProp(),
                keepOneFull(),
                keepOneUser(),
                unmergedBcs(),
                betweenChkp(0, 0, 0, 0, 1, 0, 1),
                btwnChkpUBC(0, 0, 0, 0, 1, 0, 1));
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

        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);
        long maxAge = 1; //hours
        long delta = MINUTES.toMillis(10);
        //1. Go past GC age and check no GC done as nothing deleted
        clock.waitUntil(getCurrentTimestamp() + maxAge);
        VersionGCStats stats = gc(gc, maxAge, HOURS);
        assertStatsCountsZero(stats);

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
        assertStatsCountsZero(stats);

        //3. Check that deleted property does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);

        stats = gc(gc, maxAge*2, HOURS);
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(0, 10, 0, 0, 0, 0, 1),
                gapOrphProp(0, 10, 0, 0, 0, 0, 1),
                allOrphProp(0, 10, 0, 0, 0, 0, 1),
                keepOneFull(0, 10, 0, 0, 0, 0, 1),
                keepOneUser(0, 10, 0, 0, 0, 0, 1),
                unmergedBcs(0, 10, 0, 0, 0, 0, 1),
                betweenChkp(0, 10, 0, 0, 1, 0, 2),
                btwnChkpUBC(0, 10, 0, 0, 1, 0, 2));
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
        // adding 11 to have the 10th being a special case as we're not removing it below
        for (int i = 0; i < 11; i++) {
            b1.child("x").child("jcr:content").setProperty("prop"+i, "t", STRING);
            b1.child("x").setProperty(META_PROP_PATTERN, of("jcr:content"), STRINGS);
            b1.child("x").setProperty("prop"+i, "bar", STRING);
        }
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeState x = store1.getRoot().getChildNode("x");
        assertTrue(x.exists());
        assertTrue(x.hasProperty("prop0"));
        assertTrue(x.hasProperty("prop10"));
        NodeState jcrContent = x.getChildNode("jcr:content");
        assertTrue(jcrContent.exists());
        assertTrue(jcrContent.hasProperty("prop10"));
        assertTrue(jcrContent.hasProperty("prop0"));

        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);
        long maxAge = 1; //hours
        long delta = MINUTES.toMillis(10);
        //1. Go past GC age and check no GC done as nothing deleted
        clock.waitUntil(getCurrentTimestamp() + maxAge);
        VersionGCStats stats = gc(gc, maxAge, HOURS);
        assertStatsCountsZero(stats);

        x = store1.getRoot().getChildNode("x");
        assertTrue(x.exists());
        assertTrue(x.hasProperty("prop0"));
        assertTrue(x.hasProperty("prop10"));
        jcrContent = x.getChildNode("jcr:content");
        assertTrue(jcrContent.exists());
        assertTrue(jcrContent.hasProperty("prop10"));
        assertTrue(jcrContent.hasProperty("prop0"));

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
        assertStatsCountsZero(stats);

        x = store1.getRoot().getChildNode("x");
        assertTrue(x.exists());
        assertTrue(x.hasProperty("prop0"));
        assertTrue(x.hasProperty("prop10"));
        jcrContent = x.getChildNode("jcr:content");
        assertTrue(jcrContent.exists());
        assertTrue(jcrContent.hasProperty("prop10"));
        assertFalse(jcrContent.hasProperty("prop0"));

        //3. Check that deleted property does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);

        stats = gc(gc, maxAge*2, HOURS);
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(0, 10, 0, 0, 0, 0, 1),
                gapOrphProp(0, 10, 0, 0, 0, 0, 1),
                allOrphProp(0, 10, 0, 0, 0, 0, 1),
                keepOneFull(0, 10, 0, 0, 0, 0, 1),
                keepOneUser(0, 10, 0, 0, 0, 0, 1),
                unmergedBcs(0, 10, 0, 0, 0, 0, 1),
                betweenChkp(0, 10, 0, 0, 1, 0, 2),
                btwnChkpUBC(0, 10, 0, 0, 1, 0, 2));

        x = store1.getRoot().getChildNode("x");
        assertTrue(x.exists());
        assertTrue(x.hasProperty("prop0"));
        assertTrue(x.hasProperty("prop10"));
        jcrContent = x.getChildNode("jcr:content");
        assertTrue(jcrContent.exists());
        assertTrue(jcrContent.hasProperty("prop10"));
        assertFalse(jcrContent.hasProperty("prop0"));
    }

    /**
     * To test behaviour when a bundled node having child nodes is goes missing.
     * These children must be GCed.
     */
    @Test
    public void testGCMissingBundledNode() throws Exception {

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
        // adding 11 to have the 10th being a special case as we're not removing it below
        for (int i = 0; i < 11; i++) {
            b1.child("x").child("jcr:content").setProperty("prop"+i, "t", STRING);
            b1.child("x").child("jcr:content").child("y").setProperty("prop"+i, "t", STRING);
            b1.child("x").child("jcr:content").child("y").child("z").setProperty("prop"+i, "t", STRING);
            b1.child("x").setProperty(META_PROP_PATTERN, of("jcr:content"), STRINGS);
            b1.child("x").setProperty("prop"+i, "bar", STRING);
        }
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);
        long maxAge = 1; //hours
        long delta = MINUTES.toMillis(10);
        //1. Go past GC age and check no GC done as nothing deleted
        clock.waitUntil(getCurrentTimestamp() + maxAge);
        VersionGCStats stats = gc(gc, maxAge, HOURS);
        assertStatsCountsZero(stats);

        // Remove bundled node
        store1.getDocumentStore().remove(org.apache.jackrabbit.oak.plugins.document.Collection.NODES, "1:/x");
        // invalidate cached DocumentNodeState
        store1.invalidateNodeCache("/x", store1.getRoot().getLastRevision());
        store1.runBackgroundOperations();

        //2. Check that a deleted property is not collected before maxAge
        //Clock cannot move back (it moved forward in #1) so double the maxAge
        clock.waitUntil(clock.getTime() + delta);
        stats = gc(gc, maxAge*2, HOURS);
        assertStatsCountsZero(stats);

        //3. Check that deleted property does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);

        stats = gc(gc, maxAge*2, HOURS);
        assertStatsCountsEqual(stats,
                empPropOnly(),
                gapOrphOnly(2, 0, 0, 0, 0, 0, 2),
                gapOrphProp(2, 0, 0, 0, 0, 0, 2),
                allOrphProp(2, 0, 0, 0, 0, 0, 2),
                keepOneFull(2, 0, 0, 0, 0, 0, 2),
                keepOneUser(2, 0, 0, 0, 0, 0, 2),
                unmergedBcs(2, 0, 0, 0, 0, 0, 2),
                betweenChkp(2, 0, 0, 0, 1, 0, 2),
                btwnChkpUBC(2, 0, 0, 0, 1, 0, 2));
    }

    /**
     * To test behaviour when a bundled node having child nodes is deleted.
     * These children must be GCed along with bundled properties.
     */
    @Test
    public void testGCDeletedBundledNode() throws Exception {

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
        // adding 11 to have the 10th being a special case as we're not removing it below
        for (int i = 0; i < 11; i++) {
            b1.child("x").child("jcr:content").setProperty("prop"+i, "t", STRING);
            b1.child("x").child("jcr:content").child("y").setProperty("prop"+i, "t", STRING);
            b1.child("x").child("jcr:content").child("y").child("z").setProperty("prop"+i, "t", STRING);
            b1.child("x").setProperty(META_PROP_PATTERN, of("jcr:content"), STRINGS);
            b1.child("x").setProperty("prop"+i, "bar", STRING);
        }
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);
        long maxAge = 1; //hours
        long delta = MINUTES.toMillis(10);
        //1. Go past GC age and check no GC done as nothing deleted
        clock.waitUntil(getCurrentTimestamp() + maxAge);
        VersionGCStats stats = gc(gc, maxAge, HOURS);
        assertStatsCountsZero(stats);

        // Remove bundled node
        NodeBuilder b2 = store1.getRoot().builder();
        b2.getChildNode("x").getChildNode("jcr:content").remove();
        store1.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        store1.runBackgroundOperations();

        //2. Check that a deleted property is not collected before maxAge
        //Clock cannot move back (it moved forward in #1) so double the maxAge
        clock.waitUntil(clock.getTime() + delta);
        stats = gc(gc, maxAge*2, HOURS);
        assertStatsCountsZero(stats);

        //3. Check that deleted property does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);

        stats = gc(gc, maxAge*2, HOURS);
        assertStatsCountsEqual(stats,
                new GCCounts(FullGCMode.NONE, 2, 0,0,0,0,0,0),
                empPropOnly(2, 13, 0, 0, 0, 0, 1),
                gapOrphOnly(2, 0, 0, 0, 0, 0, 0),
                gapOrphProp(2, 13, 0, 0, 0, 0, 1),
                allOrphProp(2, 13, 0, 0, 0, 0, 1),
                keepOneFull(2, 13, 0, 0, 0, 0, 1),
                keepOneUser(2, 13, 0, 0, 0, 0, 1),
                unmergedBcs(2, 13, 0, 0, 0, 0, 1),
                betweenChkp(2, 13, 0, 0, 1, 0, 2),
                btwnChkpUBC(2, 13, 0, 0, 1, 0, 2));
    }

    static void assertStatsCountsZero(VersionGCStats stats) {
        GCCounts c = new GCCounts(VersionGarbageCollector.getFullGcMode());
        assertStatsCountsEqual(stats, c);
    }

    static void assertStatsCountsEqual(VersionGCStats stats, GCCounts... counts) {
        GCCounts c = null;
        for (GCCounts a : counts) {
            if (a.mode == VersionGarbageCollector.getFullGcMode()) {
                c = a;
                break;
            }
        }
        if (c == null && VersionGarbageCollector.getFullGcMode() == FullGCMode.NONE) {
            c = new GCCounts(FullGCMode.NONE);
        }
        assertNotNull(stats);
        assertNotNull(c);
        assertEquals(c.mode + "/docGC", c.deletedDocGCCount, stats.deletedDocGCCount);
        assertEquals(c.mode + "/props", c.deletedPropsCount, stats.deletedPropsCount);
        assertEquals(c.mode + "/internalProps", c.deletedInternalPropsCount, stats.deletedInternalPropsCount);
        assertEquals(c.mode + "/propRevs", c.deletedPropRevsCount, stats.deletedPropRevsCount);
        assertEquals(c.mode + "/internalPropRevs", c.deletedInternalPropRevsCount, stats.deletedInternalPropRevsCount);
        assertEquals(c.mode + "/unmergedBC", c.deletedUnmergedBCCount, stats.deletedUnmergedBCCount);
        assertEquals(c.mode + "/updatedFullGCDocsCount", c.updatedFullGCDocsCount, stats.updatedFullGCDocsCount);
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

        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);
        long maxAge = 1; //hours
        long delta = MINUTES.toMillis(10);
        //1. Go past GC age and check no GC done as nothing deleted
        clock.waitUntil(getCurrentTimestamp() + maxAge);
        VersionGCStats stats = gc(gc, maxAge, HOURS);
        assertEquals(0, stats.deletedPropsCount);
        assertEquals(0, stats.updatedFullGCDocsCount);

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
        assertEquals(0, stats.updatedFullGCDocsCount);
        assertEquals(MIN_ID_VALUE, stats.oldestModifiedDocId); // as GC hadn't run

        //3. Check that deleted property does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);

        VersionGCSupport gcSupport = new VersionGCSupport(store1.getDocumentStore()) {

            @Override
            public Iterable<NodeDocument> getModifiedDocs(long fromModified,
                    long toModified, int limit, @NotNull String fromId,
                    @NotNull final Set<String> includePaths, @NotNull final Set<String> excludePaths) {
                Iterable<NodeDocument> modifiedDocs = super.getModifiedDocs(fromModified,
                        toModified, limit, fromId, includePaths, excludePaths);
                List<NodeDocument> result = StreamSupport.stream(modifiedDocs.spliterator(), false).collect(toList());
                final Revision updateRev = newRevision(1);
                store1.getDocumentStore().findAndUpdate(NODES, StreamSupport.stream(modifiedDocs.spliterator(), false)
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
        assertEquals(0, stats.updatedFullGCDocsCount);
        assertEquals(0, stats.deletedPropsCount);
        assertEquals(MIN_ID_VALUE, stats.oldestModifiedDocId);
    }

    @Test
    public void cancelFullGCAfterFirstBatch() throws Exception {
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

        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);
        long maxAge = 1; //hours
        long delta = MINUTES.toMillis(10);
        //1. Go past GC age and check no GC done as nothing deleted
        clock.waitUntil(getCurrentTimestamp() + maxAge);
        VersionGCStats stats = gc(gc, maxAge, HOURS);
        assertStatsCountsZero(stats);

        //Remove property
        NodeBuilder b2 = store1.getRoot().builder();
        for (int i = 0; i < 5_000; i++) {
            for (int j = 0; j < 10; j++) {
                b2.getChildNode("z"+i).removeProperty("prop"+j);
            }
        }
        store1.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        final AtomicReference<VersionGarbageCollector> gcRef = new AtomicReference<>();
        final VersionGCSupport gcSupport = new VersionGCSupport(store1.getDocumentStore()) {

            @Override
            public Iterable<NodeDocument> getModifiedDocs(long fromModified, long toModified, int limit, @NotNull String fromId,
                                                          final @NotNull Set<String> includePaths, final @NotNull Set<String> excludePaths) {
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
                return super.getModifiedDocs(fromModified, toModified, limit, fromId,
                        Collections.emptySet(), Collections.emptySet()).iterator();
            }
        };

        gcRef.set(new VersionGarbageCollector(store1, gcSupport, true, false, false));

        //3. Check that deleted property does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);
        stats = gcRef.get().gc(maxAge*2, HOURS);
        assertTrue(stats.canceled);
        assertEquals(0, stats.updatedFullGCDocsCount);
        assertEquals(0, stats.deletedPropsCount);
        assertEquals(MIN_ID_VALUE, stats.oldestModifiedDocId);
    }

    // OAK-10199 END

    // OAK-10921
    @Test
    public void resetGCFromOakRunWhileRunning() throws Exception {
        // if we reset fullGC from any external source while GC is running,
        // it should not update the fullGC variables.
        resetFullGCExternally(false);
    }

    @Test
    public void resetFullGCFromOakRunWhileRunning() throws Exception {
        // if we reset fullGC from any external source while GC is running,
        // it should not update the fullGC variables.
        resetFullGCExternally(true);
    }

    private void resetFullGCExternally(final boolean resetFullGCOnly) throws Exception {
        //1. Create nodes with properties
        NodeBuilder b1 = store1.getRoot().builder();

        // Add property to node & save
        for (int i = 0; i < 5; i++) {
            b1.child("z"+i).setProperty("prop", "foo", STRING);
        }
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);
        long maxAge = 1; //hours
        long delta = MINUTES.toMillis(10);
        //1. Go past GC age and check no GC done as nothing deleted
        clock.waitUntil(getCurrentTimestamp() + maxAge);
        VersionGCStats stats = gc(gc, maxAge, HOURS);
        assertStatsCountsZero(stats);

        //Remove property
        NodeBuilder b2 = store1.getRoot().builder();
        for (int i = 0; i < 5; i++) {
            b2.getChildNode("z"+i).removeProperty("prop");
        }
        store1.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        final AtomicReference<VersionGarbageCollector> gcRef = new AtomicReference<>();
        final VersionGCSupport gcSupport = new VersionGCSupport(store1.getDocumentStore()) {

            @Override
            public Iterable<NodeDocument> getModifiedDocs(long fromModified, long toModified, int limit, @NotNull String fromId,
                                                          final @NotNull Set<String> includePaths, final @NotNull Set<String> excludePaths) {
                // reset fullGC variables externally while GC is running
                if (resetFullGCOnly) {
                    gcRef.get().resetFullGC();
                } else {
                    gcRef.get().reset();
                }
                return super.getModifiedDocs(fromModified, toModified, limit, fromId, includePaths, excludePaths);
            }
        };

        gcRef.set(new VersionGarbageCollector(store1, gcSupport, true, false, false, 3, 0, DEFAULT_FGC_BATCH_SIZE, DEFAULT_FGC_PROGRESS_SIZE));

        //3. Check that deleted property does get collected post maxAge
        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);

        Document document = store1.getDocumentStore().find(SETTINGS, SETTINGS_COLLECTION_ID);
        assert document != null;
        assertNotNull(document.get(SETTINGS_COLLECTION_FULL_GC_TIMESTAMP_PROP));
        assertNotNull(document.get(SETTINGS_COLLECTION_FULL_GC_DOCUMENT_ID_PROP));

        stats = gcRef.get().gc(maxAge*2, HOURS);

        document = store1.getDocumentStore().find(SETTINGS, SETTINGS_COLLECTION_ID);
        assertEquals(5, stats.updatedFullGCDocsCount);
        assertEquals(5, stats.deletedPropsCount);
        assertEquals(MIN_ID_VALUE, stats.oldestModifiedDocId);

        // 4. verify that fullGC variables are not updated after resetting them
        assert document != null;
        assertNull(document.get(SETTINGS_COLLECTION_FULL_GC_TIMESTAMP_PROP));
        assertNull(document.get(SETTINGS_COLLECTION_FULL_GC_DOCUMENT_ID_PROP));
    }

    // OAK-10921 END

    /**
     * Test when a revision on a parent is becoming garbage on a property
     * but not on "_revision" as it is (potentially but in this case indeed)
     * still required by a child (2:/parent/child "ck").
     * Note that this is not involving and branch commits.
     */
    @Test
    @SuppressWarnings("unchecked")
    public void parentWithGarbageGCChildIndependent() throws Exception { // TODO rename me
        assumeTrue(fixture.hasSinglePersistence());
        NodeBuilder nb = store1.getRoot().builder();
        NodeBuilder parent = nb.child("parent");
        parent.setProperty("pk", "pv");
        parent.child("child").setProperty("ck", "cv");
        store1.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();
        // now make a change that later can ge GCed on parent but not on child
        nb = store1.getRoot().builder();
        nb.child("parent").setProperty("pk", "pv2");
        nb.child("parent").child("child").setProperty("ck", "cv2");
        store1.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        nb = store1.getRoot().builder();
        nb.child("parent").setProperty("pk", "pv3");
        store1.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        // not doing a bgops == sweep here to cause "cv2" revision
        // not being considered as commited by sweep
        // (the test goal is to cause that resolution to fail in case
        // we would delete the parent's _revisions entry)

        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));

        // check state of 2:/parent/child.ck before GC
        // invalidate a bunch of caches, in particular nodecache and commitvaluecache
        store1.getNodeCache().invalidateAll();
        store1.getNodeChildrenCache().invalidateAll();
        CachingCommitValueResolver cvr = (CachingCommitValueResolver) readField(store1, "commitValueResolver", true);
        Cache<Revision, String> commitValueCache = (Cache<Revision, String>) readField( cvr, "commitValueCache", true);
        commitValueCache.invalidateAll();
        PropertyState ckBefore = store1.getRoot().getChildNode("parent").getChildNode("child").getProperty("ck");
        assertNotNull(ckBefore);

        // now the GC
        VersionGCStats stats = gc(gc, 1, HOURS);

        // check state of 2:/parent/child.ck after GC - should match before GC state
        // invalidate a bunch of caches, in particular nodecache and commitvaluecache
        store1.getNodeCache().invalidateAll();
        store1.getNodeChildrenCache().invalidateAll();
        cvr = (CachingCommitValueResolver) readField(store1, "commitValueResolver", true);
        commitValueCache = (Cache<Revision, String>) readField( cvr, "commitValueCache", true);
        commitValueCache.invalidateAll();
        PropertyState ckAfter = store1.getRoot().getChildNode("parent").getChildNode("child").getProperty("ck");
        assertNotNull(ckAfter);
        assertEquals(ckBefore.getValue(Type.STRING), ckAfter.getValue(Type.STRING));
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(),
                gapOrphProp(),
                allOrphProp(),
                keepOneFull(0, 0, 0, 3, 0, 0, 2),
                keepOneUser(0, 0, 0, 3, 0, 0, 2),
                unmergedBcs(),
                betweenChkp(0, 0, 0, 1, 1, 0, 2),
                btwnChkpUBC(0, 0, 0, 1, 1, 0, 2));
    }

    @Test
    public void parentGCChildIndependent() throws Exception {
        assumeTrue(fixture.hasSinglePersistence());
        NodeBuilder nb = store1.getRoot().builder();
        NodeBuilder parent = nb.child("parent");
        parent.setProperty("pk", "pv");
        parent.child("child").setProperty("ck", "cv");
        store1.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        nb = store1.getRoot().builder();
        nb.child("parent").setProperty("pk", "pv2");
        store1.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));

        // now make a child change so that only parent (and root) gets GCed
        nb = store1.getRoot().builder();
        nb.child("parent").child("child").setProperty("ck", "cv2");
        store1.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        // now unlike usually, DONT do a store1.runBackgroundOperations() here
        // this will leave the parent GC-able

        // now the GC
        VersionGCStats stats = gc(gc, 1, HOURS);
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(),
                gapOrphProp(),
                allOrphProp(),
                keepOneFull(0, 0, 0, 1, 0, 0, 1),
                keepOneUser(0, 0, 0, 1, 0, 0, 1),
                unmergedBcs(),
                betweenChkp(0, 0, 0, 0, 1, 0, 1),
                btwnChkpUBC(0, 0, 0, 0, 1, 0, 1));
    }

    @Test
    @Ignore(value = "OAK-10846 requires cleanup of 2nd thread")
    public void testPartialMergeRootCleanup() throws Exception {
        assumeTrue(fixture.hasSinglePersistence());

        createSecondaryStore(LeaseCheckMode.LENIENT, true);

        NodeBuilder nb = store2.getRoot().builder();
        nb.child("node1").setProperty("a", "1");
        store2.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store2.runBackgroundOperations();

        // create the orphaned paths
        final FailingDocumentStore fds = (FailingDocumentStore) ds2;
        fds.fail().after(1).eternally();

        // create partial merge
        nb = store2.getRoot().builder();
        nb.child("node1").setProperty("a", "2");
        nb.setProperty("rootProp1", "rootValue1");
        try {
            store2.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fail("expected 'OakOak0001: write operation failed'");
        } catch(CommitFailedException cfe) {
            assertEquals("OakOak0001: write operation failed", cfe.getMessage());
        }

        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour

        // before the gc, 1h+ later, do a last-minute modification (only) on /node1 (without root update)
        nb = store1.getRoot().builder();
        nb.child("node1").setProperty("b", "4");
        try {
            store1.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
            fail("should fail");
        } catch(Exception e) {
            // expected to fail
        }
        VersionGCStats stats = gc(gc, 1, HOURS);
        store1.runBackgroundOperations();
        store1.runBackgroundOperations();
        createSecondaryStore(LeaseCheckMode.LENIENT);
        NodeState node1 = store2.getRoot().getChildNode("node1");
        assertEquals("1", node1.getProperty("a").getValue(Type.STRING));
        assertFalse(node1.hasProperty("b"));
        assertStatsCountsZero(stats);
    }

    @Test
    public void testUnmergedBCRootCleanup() throws Exception {
        assumeTrue(fixture.hasSinglePersistence());
        NodeBuilder nb = store1.getRoot().builder();
        nb.child("node1").setProperty("a", "1");
        store1.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        // create branch commits
        RevisionVector br1 = unmergedBranchCommit(store1, b -> b.child("node1").setProperty("a", "2"));
        store1.runBackgroundOperations();
        invalidateCaches(store1);
        assertEquals("1", store1.getRoot().getChildNode("node1").getProperty("a").getValue(Type.STRING));

        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));

        invalidateCaches(store1);
        assertEquals("1", store1.getRoot().getChildNode("node1").getProperty("a").getValue(Type.STRING));

        // clean everything older than one hour

        // before the gc, 1h+ later, do a last-minute modification (only) on /node1 (without root update)
        nb = store1.getRoot().builder();
        nb.child("node1").setProperty("b", "4");
        store1.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        VersionGCStats stats = gc(gc, 1, HOURS);
        invalidateCaches(store1);
        assertEquals("1", store1.getRoot().getChildNode("node1").getProperty("a").getValue(Type.STRING));
        store1.runBackgroundOperations();
        invalidateCaches(store1);
        assertEquals("1", store1.getRoot().getChildNode("node1").getProperty("a").getValue(Type.STRING));
        store1.runBackgroundOperations();
        invalidateCaches(store1);
        assertEquals("1", store1.getRoot().getChildNode("node1").getProperty("a").getValue(Type.STRING));
        createSecondaryStore(LeaseCheckMode.LENIENT);

        // while "2" was written to node1/a via an unmerged branch commit,
        // it should not have been made visible through FGC/sweep combo
        invalidateCaches(store2);
        assertEquals("1", store2.getRoot().getChildNode("node1").getProperty("a").getValue(Type.STRING));
        assertEquals("4", store2.getRoot().getChildNode("node1").getProperty("b").getValue(Type.STRING));
        invalidateCaches(store2);
        assertEquals("1", store2.getRoot().getChildNode("node1").getProperty("a").getValue(Type.STRING));
        assertEquals("4", store2.getRoot().getChildNode("node1").getProperty("b").getValue(Type.STRING));

        // deletedPropsCount=0 : _bc on /node1 and / CANNOT be removed
        // deletedPropRevsCount=1 : (nothing on /node1[a, _commitRoot), /[_revisions]
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(),
                gapOrphProp(),
                allOrphProp(),
                keepOneFull(0, 0, 1, 0, 1, 0, 1),
                keepOneUser(),
                unmergedBcs(0, 0, 1, 0, 1, 1, 1),
                betweenChkp(0, 0, 0, 0, 1, 0, 1),
                btwnChkpUBC(0, 0, 1, 0, 2, 1, 1));
        // checking for br1 revisino to have disappeared doesn't really make much sense,
        // since 1:/node1 isn't GCed as it is young, and 0:/ being root cannot guarantee full removal
        // (if br1 is deleted form 0:/ _bc, then the commit value resolution flips it to committed)
        assertBranchRevisionRemovedFromAllDocuments(store1, br1, "1:/node1");
    }

    // OAK-8646
    @Test
    public void testDeletedPropsAndUnmergedBCWithoutCollision() throws Exception {
        // OAK-10974:
        assumeTrue(fullGcMode != FullGCMode.ORPHANS_EMPTYPROPS_KEEP_ONE_ALL_PROPS);
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

        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGCStats stats = gc(gc, 1, HOURS);

        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(0, 3, 0, 0, 0, 0, 2),
                gapOrphProp(0, 3, 0, 0, 0, 0, 2),
                allOrphProp(0, 3, 0, 0, 0, 0, 2),
                keepOneFull(0, 3, 1, 1, 9, 0, 3),
                keepOneUser(0, 3, 0, 1, 0, 0, 2),
                unmergedBcs(0, 3, 1, 1, 7, 2, 3),
                betweenChkp(0, 3, 0, 0, 1, 0, 3),
                btwnChkpUBC(0, 3, 1, 1, 8, 2, 3));
        if (!isModeOneOf(FullGCMode.NONE, FullGCMode.GAP_ORPHANS, FullGCMode.GAP_ORPHANS_EMPTYPROPS)) {
            assertBranchRevisionRemovedFromAllDocuments(store1, br1);
            assertBranchRevisionRemovedFromAllDocuments(store1, br4);
        }
    }

    @Test
    public void testDeletedPropsAndUnmergedBCWithCollision() throws Exception {
        assumeTrue(fullGcMode != FullGCMode.ORPHANS_EMPTYPROPS_KEEP_ONE_ALL_PROPS);
        assumeTrue(fullGcMode != FullGCMode.ORPHANS_EMPTYPROPS_UNMERGED_BC);
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

        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour

        VersionGCStats stats = gc(gc, 1, HOURS);
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(0, 3, 0, 0,  0, 0, 2),
                gapOrphProp(0, 3, 0, 0,  0, 0, 2),
                allOrphProp(0, 3, 0, 0,  0, 0, 2),
                keepOneFull(0, 3, 2, 1, 17, 0, 3),
                keepOneUser(0, 3, 0, 1,  0, 0, 2),
                unmergedBcs(0, 3, 2, 1, 15, 4, 3),
                betweenChkp(0, 3, 0, 0,  1, 0, 3),
                btwnChkpUBC(0, 3, 2, 1, 16, 4, 3));
        if (!isModeOneOf(FullGCMode.NONE, FullGCMode.GAP_ORPHANS, FullGCMode.GAP_ORPHANS_EMPTYPROPS)) {
            assertBranchRevisionRemovedFromAllDocuments(store1, br1);
            assertBranchRevisionRemovedFromAllDocuments(store1, br2);
            assertBranchRevisionRemovedFromAllDocuments(store1, br3);
            assertBranchRevisionRemovedFromAllDocuments(store1, br4);
        }
    }

    static boolean isModeOneOf(FullGCMode... modes) {
        for (FullGCMode rdgcType : modes) {
            if (VersionGarbageCollector.getFullGcMode() == rdgcType) {
                return true;
            }
        }
        return false;
    }

    // OAK-8646 END

    /**
     * Tests whether FullGC properly deletes a late-written addChild "/grand/parent/a"
     */
    @Test
    public void lateWriteCreateChildGC() throws Exception {
        doLateWriteCreateChildrenGC(of("/grand/parent"), of("/grand/parent/a"), "/d",
            gapOrphOnly(),
            empPropOnly(),
            gapOrphProp(),
            allOrphProp(1, 0, 0, 0, 0, 0, 1),
            keepOneFull(1, 0, 0, 0, 0, 0, 1),
            keepOneUser(1, 0, 0, 0, 0, 0, 1),
            unmergedBcs(1, 0, 0, 0, 0, 0, 1),
            betweenChkp(1, 0, 0, 0, 2, 0, 2),
            btwnChkpUBC(1, 0, 0, 0, 2, 0, 2));
    }

    /**
     * Tests whether FullGC can delete a whole subtree "/a/b/c/d/**" that was
     * added via late-writes.
     */
    @Test
    public void lateWriteCreateChildTreeGC() throws Exception {
        doLateWriteCreateChildrenGC(of("/a", "/a/b/c"), of("/a/b/c/d", "/a/b/c/d/e/f"), "/d",
            gapOrphOnly(),
            empPropOnly(),
            gapOrphProp(),
            allOrphProp(3, 0, 0, 0, 0, 0, 3),
            keepOneFull(3, 0, 0, 0, 0, 0, 3),
            keepOneUser(3, 0, 0, 0, 0, 0, 3),
            unmergedBcs(3, 0, 0, 0, 0, 0, 3),
            betweenChkp(3, 0, 0, 0, 3/*4*/, 0, 4),
            btwnChkpUBC(3, 0, 0, 0, 3/*4*/, 0, 4));
    }

    /**
     * Same as {@link #lateWriteCreateChildGC()} except with a long path
     * (where the id becomes a hash)
     */
    @Test
    public void lateWriteCreateChildGCLargePath() throws Exception {
        String longPath = "p".repeat(PATH_LONG + 1);
        String path = "/grand/parent/" + longPath + "/longPathChild";
        doLateWriteCreateChildrenGC(of("/grand/parent"), of(path), "/d",
              gapOrphOnly(),
              empPropOnly(),
              gapOrphProp(),
              allOrphProp(2, 0, 0, 0, 0, 0, 2),
              keepOneFull(2, 0, 0, 0, 0, 0, 2),
              keepOneUser(2, 0, 0, 0, 0, 0, 2),
              unmergedBcs(2, 0, 0, 0, 0, 0, 2),
              betweenChkp(2, 0, 0, 0, 2/*3*/, 0, 3),
              btwnChkpUBC(2, 0, 0, 0, 2/*3*/, 0, 3));
    }

    /**
     * Tests whether FullGC can delete a large amount of randomly
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
        int expectedNumOrphanedDocs = orphans.size() + commonOrphanParents.size();
        int expectedNumInternalPropRevsGCed = 4;//904=expectedNumOrphanedDocs + 1;
        int expectedNumDocsUpdatedGCed = expectedNumOrphanedDocs + 1;
        doLateWriteCreateChildrenGC(nonOrphans, orphans, "/d",
              gapOrphOnly(),
              empPropOnly(),
              gapOrphProp(),
              allOrphProp(expectedNumOrphanedDocs, 0, 0, 0, 0, 0, expectedNumOrphanedDocs),
              keepOneFull(expectedNumOrphanedDocs, 0, 0, 0, 0, 0, expectedNumOrphanedDocs),
              keepOneUser(expectedNumOrphanedDocs, 0, 0, 0, 0, 0, expectedNumOrphanedDocs),
              unmergedBcs(expectedNumOrphanedDocs, 0, 0, 0, 0, 0, expectedNumOrphanedDocs),
              betweenChkp(expectedNumOrphanedDocs, 0, 0, 0, expectedNumInternalPropRevsGCed, 0, expectedNumDocsUpdatedGCed),
              btwnChkpUBC(expectedNumOrphanedDocs, 0, 0, 0, expectedNumInternalPropRevsGCed, 0, expectedNumDocsUpdatedGCed));
    }

    @Test
    public void lateWriteRemoveChildGC_noSweep() throws Exception {
        assumeTrue(fixture.hasSinglePersistence());
        enableFullGC(store1.getVersionGarbageCollector());
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
        // should be 3 as it should clean up the _deleted from /a/b, /a/b/c and /a/b/c/d
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(),
                gapOrphProp(),
                allOrphProp(),
                keepOneFull(0, 0, 0, 3, 3, 0, 3),
                keepOneUser(0, 0, 0, 3, 0, 0, 3),
                unmergedBcs(),
                betweenChkp(0, 0, 0, 0, 1, 0, 1),
                btwnChkpUBC(0, 0, 0, 0, 1, 0, 1));
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
        enableFullGC(store1.getVersionGarbageCollector());
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
        enableFullGC(store1.getVersionGarbageCollector());
        VersionGCStats stats = gc(store1.getVersionGarbageCollector(), 1, HOURS);
        assertNotNull(stats);
        // expected 2 updated (deletions): /a/b/c/d and /a/b/c/d/e
        assertStatsCountsEqual(stats,
                empPropOnly(),
                gapOrphOnly(2, 0, 0, 0, 0, 0, 2),
                gapOrphProp(2, 0, 0, 0, 0, 0, 2),
                allOrphProp(2, 0, 0, 0, 0, 0, 2),
                keepOneFull(2, 0, 0, 0, 0, 0, 2),
                keepOneUser(2, 0, 0, 0, 0, 0, 2),
                unmergedBcs(2, 0, 0, 0, 0, 0, 2),
                betweenChkp(2, 0, 0, 0, 3, 0, 4),
                btwnChkpUBC(2, 0, 0, 0, 3, 0, 4));

        if (isModeOneOf(FullGCMode.NONE, FullGCMode.EMPTYPROPS)) {
            // in these modes the inconsistency isn't cleaned up
            // which means there will be a OakMerge0004 exception upon
            // trying to create the node(s) again.
            // hence we can't really do this in thsse modes
        } else {
            createNodes("/a/b/c/d/e");
        }
    }

    @Ignore(value="this is a reminder to add bundling-fullGC tests in general, plus some of those cases combined with OAK-10542")
    @Test
    public void testBundlingAndLatestSplit() throws Exception {
        fail("yet to be implemented");
    }

    @Test
    public void testBundledPropUnmergedBCGC() throws Exception {
        assumeTrue(fullGcMode != FullGCMode.ORPHANS_EMPTYPROPS_KEEP_ONE_ALL_PROPS);
        assumeTrue(fullGcMode != FullGCMode.ORPHANS_EMPTYPROPS_UNMERGED_BC);

        //0. Initialize bundling configs
        final NodeBuilder builder = store1.getRoot().builder();
        new InitialContent().initialize(builder);
        BundlingConfigInitializer.INSTANCE.initialize(builder);
        merge(store1, builder);
        store1.runBackgroundOperations();

        //1. Create nodes with properties
        NodeBuilder b1 = store1.getRoot().builder();
        b1.child("x").setProperty("jcr:primaryType", "nt:file", NAME);
        String nasty_key1 = "nas.ty_key$%^";
        String nasty_key2 = "nas.ty_key$%^2";
        b1.child("x").setProperty(nasty_key1, "v", STRING);
        b1.child("x").child("jcr:content").setProperty(nasty_key2, "v2", STRING);
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeBuilder nb = store1.getRoot().builder();
        nb.child("x").setProperty(nasty_key1, "v3", STRING);
        nb.child("x").child("jcr:content").setProperty("prop", "value");
        store1.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        // create branch commits
        mergedBranchCommit(store1, b -> b.child("x").child("jcr:content").setProperty("p", "prop"));
        unmergedBranchCommit(store1, b -> b.child("x").child("jcr:content").setProperty("a", "b"));
        unmergedBranchCommit(store1, b -> b.child("x").child("jcr:content").setProperty(nasty_key2, "c"));
        unmergedBranchCommit(store1, b -> b.child("x").child("jcr:content").setProperty(nasty_key2, "d"));
        mergedBranchCommit(store1, b -> b.child("x").child("jcr:content").removeProperty("p"));
        store1.runBackgroundOperations();

        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);
        long maxAgeHours = 1;
        long maxAgeMillis = TimeUnit.HOURS.toMillis(maxAgeHours);
        //1. Go past GC age and check no GC done as nothing deleted
        clock.waitUntil(getCurrentTimestamp() + maxAgeMillis + 1);

        VersionGCStats stats = gc(gc, maxAgeHours, HOURS);
        // below is an example of how the different modes result in different cleanups
        // this might help us narrow down differences in the modes
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(0, 2, 0, 0,  0, 0, 1),
                gapOrphProp(0, 2, 0, 0,  0, 0, 1),
                allOrphProp(0, 2, 0, 0,  0, 0, 1),
                keepOneFull(0, 2, 2, 3, 11, 0, 2),
                keepOneUser(0, 2, 0, 3,  0, 0, 1),
                unmergedBcs(0, 2, 1, 2, 12, 3, 2),
                betweenChkp(0, 2, 0, 0,  1, 0, 2),
                btwnChkpUBC(0, 2, 1, 2, 13, 3, 2));
    }

    static GCCounts empPropOnly() {
        return new GCCounts(FullGCMode.EMPTYPROPS);
    }

    static GCCounts empPropOnly(int deletedDocGCCount, int deletedPropsCount,
                                int deletedInternalPropsCount, int deletedPropRevsCount,
                                int deletedInternalPropRevsCount, int deletedUnmergedBCCount,
                                int updatedFullGCDocsCount) {
        assertEquals(0, deletedInternalPropsCount);
        assertEquals(0, deletedPropRevsCount);
        assertEquals(0, deletedInternalPropRevsCount);
        assertEquals(0, deletedUnmergedBCCount);
        return new GCCounts(FullGCMode.EMPTYPROPS, deletedDocGCCount,
                deletedPropsCount, deletedInternalPropsCount, deletedPropRevsCount,
                deletedInternalPropRevsCount, deletedUnmergedBCCount,
                updatedFullGCDocsCount);
    }

    static GCCounts gapOrphOnly() {
        return new GCCounts(FullGCMode.GAP_ORPHANS);
    }

    static GCCounts gapOrphOnly(int deletedDocGCCount, int deletedPropsCount,
            int deletedInternalPropsCount, int deletedPropRevsCount,
            int deletedInternalPropRevsCount, int deletedUnmergedBCCount,
            int updatedFullGCDocsCount) {
        assertEquals(0, deletedInternalPropsCount);
        assertEquals(0, deletedPropRevsCount);
        assertEquals(0, deletedInternalPropRevsCount);
        assertEquals(0, deletedUnmergedBCCount);
        return new GCCounts(FullGCMode.GAP_ORPHANS, deletedDocGCCount,
                deletedPropsCount, deletedInternalPropsCount, deletedPropRevsCount,
                deletedInternalPropRevsCount, deletedUnmergedBCCount,
                updatedFullGCDocsCount);
    }

    static GCCounts gapOrphProp() {
        return new GCCounts(FullGCMode.GAP_ORPHANS_EMPTYPROPS);
    }

    static GCCounts gapOrphProp(int deletedDocGCCount, int deletedPropsCount,
            int deletedInternalPropsCount, int deletedPropRevsCount,
            int deletedInternalPropRevsCount, int deletedUnmergedBCCount,
            int updatedFullGCDocsCount) {
        assertEquals(0, deletedInternalPropsCount);
        assertEquals(0, deletedPropRevsCount);
        assertEquals(0, deletedInternalPropRevsCount);
        assertEquals(0, deletedUnmergedBCCount);
        return new GCCounts(FullGCMode.GAP_ORPHANS_EMPTYPROPS, deletedDocGCCount,
                deletedPropsCount, deletedInternalPropsCount, deletedPropRevsCount,
                deletedInternalPropRevsCount, deletedUnmergedBCCount,
                updatedFullGCDocsCount);
    }

    static GCCounts allOrphProp() {
        return new GCCounts(FullGCMode.ALL_ORPHANS_EMPTYPROPS);
    }

    static GCCounts allOrphProp(int deletedDocGCCount, int deletedPropsCount,
            int deletedInternalPropsCount, int deletedPropRevsCount,
            int deletedInternalPropRevsCount, int deletedUnmergedBCCount,
            int updatedFullGCDocsCount) {
        assertEquals(0, deletedInternalPropsCount);
        assertEquals(0, deletedPropRevsCount);
        assertEquals(0, deletedInternalPropRevsCount);
        assertEquals(0, deletedUnmergedBCCount);
        return new GCCounts(FullGCMode.ALL_ORPHANS_EMPTYPROPS, deletedDocGCCount,
                deletedPropsCount, deletedInternalPropsCount, deletedPropRevsCount,
                deletedInternalPropRevsCount, deletedUnmergedBCCount,
                updatedFullGCDocsCount);
    }

    static GCCounts keepOneFull() {
        return new GCCounts(FullGCMode.ORPHANS_EMPTYPROPS_KEEP_ONE_ALL_PROPS);
    }

    static GCCounts keepOneFull(int deletedDocGCCount, int deletedPropsCount,
            int deletedInternalPropsCount, int deletedPropRevsCount,
            int deletedInternalPropRevsCount, int deletedUnmergedBCCount,
            int updatedFullGCDocsCount) {
        return new GCCounts(FullGCMode.ORPHANS_EMPTYPROPS_KEEP_ONE_ALL_PROPS, deletedDocGCCount,
                deletedPropsCount, deletedInternalPropsCount, deletedPropRevsCount,
                deletedInternalPropRevsCount, deletedUnmergedBCCount,
                updatedFullGCDocsCount);
    }

    static GCCounts keepOneUser() {
        return new GCCounts(FullGCMode.ORPHANS_EMPTYPROPS_KEEP_ONE_USER_PROPS);
    }

    static GCCounts keepOneUser(int deletedDocGCCount, int deletedPropsCount,
            int deletedInternalPropsCount, int deletedPropRevsCount,
            int deletedInternalPropRevsCount, int deletedUnmergedBCCount,
            int updatedFullGCDocsCount) {
        return new GCCounts(FullGCMode.ORPHANS_EMPTYPROPS_KEEP_ONE_USER_PROPS,
                deletedDocGCCount, deletedPropsCount, deletedInternalPropsCount,
                deletedPropRevsCount, deletedInternalPropRevsCount,
                deletedUnmergedBCCount, updatedFullGCDocsCount);
    }

    static GCCounts unmergedBcs() {
        return new GCCounts(FullGCMode.ORPHANS_EMPTYPROPS_UNMERGED_BC);
    }

    static GCCounts unmergedBcs(int deletedDocGCCount, int deletedPropsCount,
            int deletedInternalPropsCount, int deletedPropRevsCount,
            int deletedInternalPropRevsCount, int deletedUnmergedBCCount,
            int updatedFullGCDocsCount) {
        return new GCCounts(FullGCMode.ORPHANS_EMPTYPROPS_UNMERGED_BC,
                deletedDocGCCount, deletedPropsCount, deletedInternalPropsCount,
                deletedPropRevsCount, deletedInternalPropRevsCount,
                deletedUnmergedBCCount, updatedFullGCDocsCount);
    }

    static GCCounts betweenChkp() {
        return new GCCounts(FullGCMode.ORPHANS_EMPTYPROPS_BETWEEN_CHECKPOINTS_NO_UNMERGED_BC);
    }

    static GCCounts betweenChkp(int deletedDocGCCount, int deletedPropsCount,
            int deletedInternalPropsCount, int deletedPropRevsCount,
            int deletedInternalPropRevsCount, int deletedUnmergedBCCount,
            int updatedFullGCDocsCount) {
        return new GCCounts(FullGCMode.ORPHANS_EMPTYPROPS_BETWEEN_CHECKPOINTS_NO_UNMERGED_BC,
                deletedDocGCCount, deletedPropsCount, deletedInternalPropsCount,
                deletedPropRevsCount, deletedInternalPropRevsCount,
                deletedUnmergedBCCount, updatedFullGCDocsCount);
    }

    static GCCounts btwnChkpUBC() {
        return new GCCounts(FullGCMode.ORPHANS_EMPTYPROPS_BETWEEN_CHECKPOINTS_WITH_UNMERGED_BC);
    }

    static GCCounts btwnChkpUBC(int deletedDocGCCount, int deletedPropsCount,
            int deletedInternalPropsCount, int deletedPropRevsCount,
            int deletedInternalPropRevsCount, int deletedUnmergedBCCount,
            int updatedFullGCDocsCount) {
        return new GCCounts(FullGCMode.ORPHANS_EMPTYPROPS_BETWEEN_CHECKPOINTS_WITH_UNMERGED_BC,
                deletedDocGCCount, deletedPropsCount, deletedInternalPropsCount,
                deletedPropRevsCount, deletedInternalPropRevsCount,
                deletedUnmergedBCCount, updatedFullGCDocsCount);
    }

    @Test
    public void testBundledPropRevGC() throws Exception {
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
            b1.child("x").child("jcr:content").setProperty("bprop"+i, "t", STRING);
            b1.child("x").setProperty(META_PROP_PATTERN, of("jcr:content"), STRINGS);
            b1.child("x").setProperty("prop"+i, "bar", STRING);
        }
        String nasty_key1 = "nas.ty_key$%^";
        String nasty_key2 = "nas.ty_key$%^2";
        b1.child("x").setProperty(nasty_key1, "v", STRING);
        b1.child("x").child("jcr:content").setProperty(nasty_key2, "v2", STRING);
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // make some overwrites for FullGC to cleanup
        b1 = store1.getRoot().builder();
        for (int i = 0; i < 6; i++) {
            b1.child("x").child("jcr:content").setProperty("bprop"+i, "t2", STRING);
        }
        for (int i = 0; i < 3; i++) {
            b1.child("x").setProperty("prop"+i, "bar2", STRING);
        }
        b1.child("x").setProperty(nasty_key1, "bv", STRING);
        b1.child("x").child("jcr:content").setProperty(nasty_key2, "bv2", STRING);
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);
        long maxAgeHours = 1;
        long maxAgeMillis = TimeUnit.HOURS.toMillis(maxAgeHours);
        //1. Go past GC age and check no GC done as nothing deleted
        clock.waitUntil(getCurrentTimestamp() + maxAgeMillis + 1);
        VersionGCStats stats = gc(gc, maxAgeHours, HOURS);
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(),
                gapOrphProp(),
                allOrphProp(),
                keepOneFull(0, 0, 0, 11, 0, 0, 1),
                keepOneUser(0, 0, 0, 11, 0, 0, 1),
                unmergedBcs(),
                betweenChkp(0, 0, 0, 0, 1, 0, 1),
                btwnChkpUBC(0, 0, 0, 0, 1, 0, 1));

        NodeState x = store1.getRoot().getChildNode("x");
        assertTrue(x.exists());
        assertEquals(x.getProperty("prop0").getValue(Type.STRING), "bar2");
        assertEquals(x.getProperty("prop9").getValue(Type.STRING), "bar");
        NodeState jcrContent = x.getChildNode("jcr:content");
        assertTrue(jcrContent.exists());
        assertEquals(jcrContent.getProperty("bprop0").getValue(Type.STRING), "t2");
        assertEquals(jcrContent.getProperty("bprop9").getValue(Type.STRING), "t");

        NodeDocument doc = store1.getDocumentStore().find(NODES, "1:/x", -1);
        assertNotNull(doc);
        if (VersionGarbageCollector.getFullGcMode() == FullGCMode.ORPHANS_EMPTYPROPS_BETWEEN_CHECKPOINTS_WITH_UNMERGED_BC
                || VersionGarbageCollector.getFullGcMode() == FullGCMode.NONE || VersionGarbageCollector.getFullGcMode() == FullGCMode.GAP_ORPHANS
                || VersionGarbageCollector.getFullGcMode() == FullGCMode.GAP_ORPHANS_EMPTYPROPS
                || VersionGarbageCollector.getFullGcMode() == FullGCMode.ALL_ORPHANS_EMPTYPROPS
                || VersionGarbageCollector.getFullGcMode() == FullGCMode.ORPHANS_EMPTYPROPS_UNMERGED_BC
                || VersionGarbageCollector.getFullGcMode() == FullGCMode.ORPHANS_EMPTYPROPS_BETWEEN_CHECKPOINTS_NO_UNMERGED_BC
                || VersionGarbageCollector.getFullGcMode() == FullGCMode.EMPTYPROPS) {
            // this mode doesn't currently delete all revisions,
            // thus would fail below assert.
            return;
        }
        for (Entry<String, Object> e : doc.entrySet()) {
            Object v = e.getValue();
            if (v instanceof Map) {
                @SuppressWarnings("rawtypes")
                Map m = (Map)v;
                assertEquals("more than 1 entry for " + e.getKey(), 1, m.size());
            }
        }
    }

    // OAK-10370
    @Test
    public void testGCDeletedPropsWithDryRunMode() throws Exception {
        //1. Create nodes with properties
        NodeBuilder b1 = store1.getRoot().builder();
        b1.child("x").setProperty("test", "t", STRING);
        b1.child("z").setProperty("prop", "foo", STRING);
        store1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // enable the full gc flag
        enableFullGC(gc);
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
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(0, 1, 0, 0, 0, 0, 1),
                gapOrphProp(0, 1, 0, 0, 0, 0, 1),
                allOrphProp(0, 1, 0, 0, 0, 0, 1),
                keepOneFull(0, 1, 0, 0, 0, 0, 1),
                keepOneUser(0, 1, 0, 0, 0, 0, 1),
                unmergedBcs(0, 1, 0, 0, 0, 0, 1),
                betweenChkp(0, 1, 0, 0, 1, 0, 2),
                btwnChkpUBC(0, 1, 0, 0, 1, 0, 2));
        assertEquals(MIN_ID_VALUE, stats.oldestModifiedDocId);

        // 4. Save values of fullGC settings collection fields
        final String oldestModifiedDocId = stats.oldestModifiedDocId;
        final long oldestModifiedDocTimeStamp = stats.oldestModifiedDocTimeStamp;

        final Document documentBefore = store1.getDocumentStore().find(SETTINGS, SETTINGS_COLLECTION_ID);
        assert documentBefore != null;
        assertEquals(documentBefore.get(SETTINGS_COLLECTION_FULL_GC_TIMESTAMP_PROP), oldestModifiedDocTimeStamp);
        assertEquals(documentBefore.get(SETTINGS_COLLECTION_FULL_GC_DOCUMENT_ID_PROP), oldestModifiedDocId);

        //5. Verify that in dryRun mode property does not get gc and fullGC fields remain the same
        NodeBuilder b3 = store1.getRoot().builder();
        b3.child("x").removeProperty("test");
        store1.merge(b3, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        clock.waitUntil(clock.getTime() + HOURS.toMillis(maxAge*2) + delta);
        // enabled dryRun mode
        enableFullGCDryRun(gc);
        stats = gc(gc, maxAge*2, HOURS);

        final String oldestModifiedDryRunDocId = stats.oldestModifiedDocId;
        final long oldestModifiedDocDryRunTimeStamp = stats.oldestModifiedDocTimeStamp;

        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(0, 1, 0, 0, 0, 0, 1),
                gapOrphProp(0, 1, 0, 0, 0, 0, 1),
                allOrphProp(0, 1, 0, 0, 0, 0, 1),
                keepOneFull(0, 1, 0, 0, 0, 0, 1),
                keepOneUser(0, 1, 0, 0, 0, 0, 1),
                unmergedBcs(0, 1, 0, 0, 0, 0, 1),
                betweenChkp(0, 1, 0, 0, 1, 0, 2),
                btwnChkpUBC(0, 1, 0, 0, 1, 0, 2));
        assertEquals(MIN_ID_VALUE, stats.oldestModifiedDocId);
        assertTrue(stats.fullGCDryRunMode);

        final Document documentAfter = store1.getDocumentStore().find(SETTINGS, SETTINGS_COLLECTION_ID);
        assert documentAfter != null;
        assertEquals(documentAfter.get(SETTINGS_COLLECTION_FULL_GC_TIMESTAMP_PROP), oldestModifiedDocTimeStamp);
        assertEquals(documentAfter.get(SETTINGS_COLLECTION_FULL_GC_DOCUMENT_ID_PROP), oldestModifiedDocId);

        assertEquals(documentAfter.get(SETTINGS_COLLECTION_FULL_GC_DRY_RUN_TIMESTAMP_PROP), oldestModifiedDocDryRunTimeStamp);
        assertEquals(documentAfter.get(SETTINGS_COLLECTION_FULL_GC_DRY_RUN_DOCUMENT_ID_PROP), oldestModifiedDryRunDocId);
    }

    @Test
    public void testDeletedPropsAndUnmergedBCWithCollisionWithDryRunMode() throws Exception {
        // OAK-10869:
        assumeTrue(fullGcMode != FullGCMode.ORPHANS_EMPTYPROPS_KEEP_ONE_ALL_PROPS);
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

        // enable the full gc flag
        enableFullGC(gc);
        enableFullGCDryRun(gc);

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGCStats stats = gc(gc, 1, HOURS);
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(0, 3, 0, 0, 0, 0, 2),
                gapOrphProp(0, 3, 0, 0, 0, 0, 2),
                allOrphProp(0, 3, 0, 0, 0, 0, 2),
                keepOneFull(0, 3, 2, 1,17, 0, 3),
                keepOneUser(0, 3, 0, 1, 0, 0, 2),
                unmergedBcs(0, 3, 2, 1,15, 4, 3),
                betweenChkp(0, 3, 0, 0, 1, 0, 3),
                btwnChkpUBC(0, 3, 2, 1,16, 4, 3));
        assertTrue(stats.fullGCDryRunMode);

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

        enableFullGC(store1.getVersionGarbageCollector());

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGCStats stats = gc(store1.getVersionGarbageCollector(), 1, HOURS);
        assertFalse(store1.getRoot().getChildNode("bar").hasProperty("prop"));
        assertNotNull(stats);
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(0, 1, 0, 0, 0, 0, 1),
                gapOrphProp(0, 1, 0, 0, 0, 0, 1),
                allOrphProp(0, 1, 0, 0, 0, 0, 1),
                keepOneFull(0, 1, 0, 0, 0, 0, 1),
                keepOneUser(0, 1, 0, 0, 0, 0, 1),
                unmergedBcs(0, 1, 0, 0, 0, 0, 1),
                betweenChkp(0, 1, 0, 0, 1, 0, 2),
                btwnChkpUBC(0, 1, 0, 0, 1, 0, 2));
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

        enableFullGC(store1.getVersionGarbageCollector());

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGCStats stats = gc(store1.getVersionGarbageCollector(), 1, HOURS);
        assertFalse(store1.getRoot().getChildNode("bar").hasProperty("prop"));
        assertNotNull(stats);
        // since we have updated a totally unrelated path i.e. "/a",
        // we should still be seeing the garbage from late write and
        // thus it will be collected.
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(0, 1, 0, 0, 0, 0, 1),
                gapOrphProp(0, 1, 0, 0, 0, 0, 1),
                allOrphProp(0, 1, 0, 0, 0, 0, 1),
                keepOneFull(0, 1, 0, 0, 0, 0, 1),
                keepOneUser(0, 1, 0, 0, 0, 0, 1),
                unmergedBcs(0, 1, 0, 0, 0, 0, 1),
                betweenChkp(0, 1, 0, 0, 2, 0, 2),
                btwnChkpUBC(0, 1, 0, 0, 2, 0, 2));

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

        enableFullGC(store1.getVersionGarbageCollector());

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGCStats stats = gc(store1.getVersionGarbageCollector(), 1, HOURS);
        assertEquals("value2", store1.getRoot().getChildNode("bar").getProperty("prop").getValue(Type.STRING));
        // deletedPropsCount : we shouldn't be able to remove the property since we have
        // updated an related path that has lead to an update of common ancestor and
        // this would make late write visible
        // deletedPropRevsCount : 2 prop-revs GCed : the original prop=value, plus the
        // removeProperty(prop) plus 1 _commitRoot entry
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(),
                gapOrphProp(),
                allOrphProp(),
                keepOneFull(0, 0, 0, 2, 0, 0, 1),
                keepOneUser(0, 0, 0, 2, 0, 0, 1),
                unmergedBcs(),
                betweenChkp(0, 0, 0, 0, 2, 0, 1),
                btwnChkpUBC(0, 0, 0, 0, 2, 0, 1));
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
        lateWriteRemovePropertiesNodes(of("/bar"), null, false, "p");

        assertDocumentsExist(of("/bar"));
        assertPropertyNotExist("/bar", "p");

        enableFullGC(store1.getVersionGarbageCollector());

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGCStats stats = gc(store1.getVersionGarbageCollector(), 1, HOURS);
        assertNotNull(stats);
        // 1 prop-rev removal : the late-write null
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(),
                gapOrphProp(),
                allOrphProp(),
                keepOneFull(0, 0, 0, 1, 0, 0, 1),
                keepOneUser(0, 0, 0, 1, 0, 0, 1),
                unmergedBcs(),
                betweenChkp(0, 0, 0, 0, 1, 0, 1),
                btwnChkpUBC(0, 0, 0, 0, 1, 0, 1));
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
        lateWriteRemovePropertiesNodes(of("/foo/bar/baz"), "/a", false, "prop");

        assertDocumentsExist(of("/foo/bar/baz"));
        assertPropertyNotExist("/foo/bar/baz", "prop");

        enableFullGC(store1.getVersionGarbageCollector());

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGCStats stats = gc(store1.getVersionGarbageCollector(), 1, HOURS);
        assertNotNull(stats);
        // since we have updated an totally unrelated path i.e. "/a", we should still be seeing the garbage from late write and
        // thus it will be collected.
        // removes 1 prop-rev : the late-write null
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(),
                gapOrphProp(),
                allOrphProp(),
                keepOneFull(0, 0, 0, 1, 0, 0, 1),
                keepOneUser(0, 0, 0, 1, 0, 0, 1),
                unmergedBcs(),
                betweenChkp(0, 0, 0, 0, 2, 0, 1),
                btwnChkpUBC(0, 0, 0, 0, 2, 0, 1));
        assertDocumentsExist(of("/foo/bar/baz"));
        invalidateCaches(store1);
        assertEquals("value", store1.getRoot().getChildNode("foo").getChildNode("bar")
                .getChildNode("baz").getProperty("prop").getValue(Type.STRING));
    }

    @SuppressWarnings("unchecked")
    private void invalidateCaches(DocumentNodeStore dns) throws IllegalAccessException {
        dns.invalidateNodeChildrenCache();
        dns.getNodeCache().invalidateAll();
        dns.getNodeChildrenCache().invalidateAll();
        CachingCommitValueResolver cvr = (CachingCommitValueResolver) readField(dns, "commitValueResolver", true);
        Cache<Revision, String> commitValueCache = (Cache<Revision, String>) readField( cvr, "commitValueCache", true);
        commitValueCache.invalidateAll();
    }

    @Test
    public void skipPropertyRemovedByLateWriteWithRelatedPath_normal() throws Exception {
        doSkipPropertyRemovedByLateWriteWithRelatedPath(false);
    }

    @Test
    public void skipPropertyRemovedByLateWriteWithRelatedPath_branch() throws Exception {
        doSkipPropertyRemovedByLateWriteWithRelatedPath(true);
    }

    private void doSkipPropertyRemovedByLateWriteWithRelatedPath(boolean useBranchForUnrelatedPath) throws Exception {
        // create a node with property.
        assumeTrue(fixture.hasSinglePersistence());
        NodeBuilder nb = store1.getRoot().builder();
        nb.child("bar").setProperty("prop", "value");
        store1.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store1.runBackgroundOperations();

        // unrelated path should be such that the paths and unrelated path shouldn't have common parent
        // for e.g. if path is /bar & unrelated is /d then there common ancestor is "/" i.e. root.
        lateWriteRemovePropertiesNodes(of("/bar"), "/d", useBranchForUnrelatedPath, "prop");

        assertDocumentsExist(of("/bar"));
        assertPropertyNotExist("/bar", "prop");

        enableFullGC(store1.getVersionGarbageCollector());

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGCStats stats = gc(store1.getVersionGarbageCollector(), 1, HOURS);
        assertTrue(store1.getRoot().getChildNode("d").exists());
        invalidateCaches(store1);
        assertTrue(store1.getRoot().getChildNode("d").exists());
        assertNotNull(stats);
        // we should be able to remove the property since we have updated an related path that has lead to an update
        // of common ancestor and this would make late write visible
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(0, 1, 0, 0, 0, 0, 1),
                gapOrphProp(0, 1, 0, 0, 0, 0, 1),
                allOrphProp(0, 1, 0, 0, 0, 0, 1),
                keepOneFull(0, 1, 0, 0, 0, 0, 1),
                keepOneUser(0, 1, 0, 0, 0, 0, 1),
                unmergedBcs(0, 1, 0, 0, 0, 0, 1),
                useBranchForUnrelatedPath ?
                betweenChkp(0, 1, 0, 0, 1, 0, 2) :
                betweenChkp(0, 1, 0, 0, 2, 0, 2),
                useBranchForUnrelatedPath ?
                btwnChkpUBC(0, 1, 0, 0, 1, 0, 2) :
                btwnChkpUBC(0, 1, 0, 0, 2, 0, 2));
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

        Set<String> names = new HashSet<>();
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

        Set<String> children = new HashSet<>();
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
                        input -> {
                                try {
                                    docs.put(input);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                                return true;
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
        List<String> names = new ArrayList<>();
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
        List<String> prevIds = CollectionUtils.toList(Iterators.transform(
                foo.getPreviousDocLeaves(), input -> input.getId()));

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

        final AtomicReference<VersionGarbageCollector> gcRef = new AtomicReference<>();
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

        final AtomicReference<VersionGarbageCollector> gcRef = new AtomicReference<>();
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
                        input -> {
                                docCounter.incrementAndGet();
                                // don't report any doc to be GC'able
                                return false;
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
        lateWrite(orphanedPaths, TestUtils::createChild, unrelatedPathOrNull, false,
                ADD_NODE_OPS, (ds, ops) -> ds.createOrUpdate(NODES, ops));
    }

    private void lateWriteRemoveNodes(Collection<String> orphanedPaths,
                                      String unrelatedPathOrNull) throws Exception {
        lateWrite(orphanedPaths, (rb, path) -> childBuilder(rb, path).remove(),
                unrelatedPathOrNull, false, REMOVE_NODE_OPS,
                (ds, ops) -> ds.createOrUpdate(NODES, ops));
    }

    private void lateWriteAddPropertiesNodes(Collection<String> paths, String unrelatedPath, String propertyName,
                                             String propertyValue) throws Exception {
        lateWrite(paths,
                (rb, path) -> childBuilder(rb, path).setProperty(propertyName,
                        propertyValue),
                unrelatedPath, false, setPropertyOps(propertyName),
                (ds, ops) -> ds.findAndUpdate(NODES, ops));
    }

    private void lateWriteRemovePropertiesNodes(Collection<String> paths, String unrelatedPath, boolean bc4Unrelated, String propertyName)
            throws Exception {
        lateWrite(paths,
                (rb, path) -> childBuilder(rb, path).removeProperty(propertyName),
                unrelatedPath, bc4Unrelated, setPropertyOps(propertyName),
                (ds, ops) -> ds.findAndUpdate(NODES, ops));
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
    private void lateWrite(Collection<String> paths, LateWriteChangesBuilder lateWriteChangesBuilder, String unrelatedPath, boolean bc4Unrealted,
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
                assertEquals("OakOak0001: write operation failed", e.getMessage());
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
        if (bc4Unrealted) {
            mergedBranchCommit(store2, nb -> createChild(nb, unrelatedPath));
        } else {
            merge(store2, createChild(store2.getRoot().builder(), unrelatedPath));
        }
        store2.runBackgroundOperations();
        store2.dispose();
        store1.runBackgroundOperations();
    }

    /**
     * Creates a bunch of parents properly, then creates a bunch of orphans in
     * late-write manner (i.e. not properly), then runs FullGC and assets that
     * everything was deleted as expected
     *
     * @param parents                 the nodes that should be created properly -
     *                                each one in a separate merge
     * @param orphans                 the nodes that should be created inproperly -
     *                                each one in a separate late-write way
     * @param expectedNumOrphanedDocs the expected number of orphan documents that
     *                                FullGC should cleanup
     * @param unrelatedPath           an unrelated path that should be merged after
     *                                late-write - ensures lastRev is updated on
     *                                root to allow detecting late-writes as such
     */
    private void doLateWriteCreateChildrenGC(Collection<String> parents,
            Collection<String> orphans, String unrelatedPath, GCCounts... counts)
            throws Exception {
        assumeTrue(fixture.hasSinglePersistence());
        createNodes(parents);
        lateWriteCreateNodes(orphans, unrelatedPath);

        assertDocumentsExist(parents);
        assertDocumentsExist(orphans);
        assertNodesDontExist(parents, orphans);

        enableFullGC(store1.getVersionGarbageCollector());

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGCStats stats = gc(store1.getVersionGarbageCollector(), 1, HOURS);
        assertNotNull(stats);
        assertStatsCountsEqual(stats, counts);
        assertDocumentsExist(parents);
        // and the main assert being: have those lateCreated (orphans) docs been deleted
        assertNodesDontExist(parents, orphans);
        if (!isModeOneOf(FullGCMode.NONE, FullGCMode.GAP_ORPHANS, FullGCMode.GAP_ORPHANS_EMPTYPROPS, FullGCMode.EMPTYPROPS)) {
            assertDocumentsDontExist(orphans);
        }
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
