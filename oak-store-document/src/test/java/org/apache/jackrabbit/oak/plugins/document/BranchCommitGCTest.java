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

import static java.util.concurrent.TimeUnit.HOURS;
import static org.apache.commons.lang3.reflect.FieldUtils.writeField;
import static org.apache.commons.lang3.reflect.FieldUtils.writeStaticField;
import static org.apache.jackrabbit.oak.plugins.document.FullGCHelper.assertBranchRevisionRemovedFromAllDocuments;
import static org.apache.jackrabbit.oak.plugins.document.FullGCHelper.build;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollectorIT.allOrphProp;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollectorIT.assertStatsCountsEqual;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollectorIT.assertStatsCountsZero;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollectorIT.betweenChkp;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollectorIT.btwnChkpUBC;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollectorIT.empPropOnly;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollectorIT.gapOrphOnly;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollectorIT.gapOrphProp;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollectorIT.isModeOneOf;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollectorIT.keepOneFull;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollectorIT.keepOneUser;
import static org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollectorIT.unmergedBcs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.SortedMap;
import java.util.function.Consumer;

import org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.FullGCMode;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollectorIT.GCCounts;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;

@RunWith(Parameterized.class)
public class BranchCommitGCTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();
    private Clock clock;
    private DocumentNodeStore store;
    private VersionGarbageCollector gc;
    private VersionGarbageCollector.VersionGCStats stats;

    @Parameterized.Parameters(name="{index}: {0} with {1}")
    public static java.util.Collection<Object[]> params() throws IOException {
        java.util.Collection<Object[]> params = new LinkedList<>();
        for (Object[] fixture : AbstractDocumentStoreTest.fixtures()) {
            DocumentStoreFixture f = (DocumentStoreFixture)fixture[0];
            for (FullGCMode gcType : FullGCMode.values()) {
                params.add(new Object[] {f, gcType});
            }
        }
        return params;
    }

    @Parameter(0)
    public DocumentStoreFixture fixture;

    @Parameter(1)
    public FullGCMode fullGcMode;

    private FullGCMode originalFullGcMode;

    @Before
    public void setUp() throws Exception {
        clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        store = builderProvider.newBuilder().clock(clock)
                .setLeaseCheckMode(LeaseCheckMode.DISABLED).setFullGCEnabled(true)
                .setAsyncDelay(0).getNodeStore();
        gc = store.getVersionGarbageCollector();
        VersionGarbageCollectorIT.staticStore = store;
        originalFullGcMode = VersionGarbageCollector.getFullGcMode();
        writeStaticField(VersionGarbageCollector.class, "fullGcMode", fullGcMode, true);
        // OAK-11254 : adding a temporary sleep to reduce likelyhood of
        // backgroundPurge to interfere with test
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            fail("got interrupted");
        }
    }

    @After
    public void tearDown() throws Exception {
        writeStaticField(VersionGarbageCollector.class, "fullGcMode", originalFullGcMode, true);
        assertNoEmptyProperties();
        if (store != null) {
            store.dispose();
        }
        VersionGarbageCollectorIT.staticStore = null;
        Revision.resetClockToDefault();
    }

    @Test
    public void unmergedAddChildren() throws Exception {
        RevisionVector br = unmergedBranchCommit(b -> {
            b.child("a");
            b.child("b");
        });
        assertExists("1:/a");
        assertExists("1:/b");
        assertFalse(store.getDocumentStore().find(Collection.NODES, "1:/a").wasDeletedOnce());
        assertFalse(store.getDocumentStore().find(Collection.NODES, "1:/b").wasDeletedOnce());

        store.runBackgroundOperations();

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGarbageCollector.VersionGCStats stats = gc.gc(1, HOURS);

        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(),
                gapOrphProp(),
                allOrphProp(2, 0, 0, 0, 0, 0, 2),
                keepOneFull(2, 0, 1, 0, 1, 0, 3),
                keepOneUser(2, 0, 0, 0, 0, 0, 2),
                betweenChkp(2, 0, 0, 0, 0, 0, 2),
                unmergedBcs(2, 0, 1, 0, 1, 1, 3),
                btwnChkpUBC(2, 0, 1, 0, 1, 1, 3));

        // now do another gc - should not have anything left to clean up though
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        stats = gc.gc(1, HOURS);
        assertStatsCountsZero(stats);
        if (!isModeOneOf(FullGCMode.NONE, FullGCMode.GAP_ORPHANS, FullGCMode.GAP_ORPHANS_EMPTYPROPS, FullGCMode.EMPTYPROPS)) {
            assertNotExists("1:/a");
            assertNotExists("1:/b");
            assertBranchRevisionRemovedFromAllDocuments(store, br);
        }
    }

    @Test
    public void unmergedAddThenMergedAddAndRemoveChildren() throws Exception {
        assumeTrue(fullGcMode != FullGCMode.ORPHANS_EMPTYPROPS_KEEP_ONE_ALL_PROPS);
        assumeTrue(fullGcMode != FullGCMode.ORPHANS_EMPTYPROPS_KEEP_ONE_USER_PROPS);
        RevisionVector br = unmergedBranchCommit(b -> {
            b.child("a");
            b.child("b");
        });
        assertExists("1:/a");
        assertExists("1:/b");
        assertFalse(store.getDocumentStore().find(Collection.NODES, "1:/a").wasDeletedOnce());
        assertFalse(store.getDocumentStore().find(Collection.NODES, "1:/b").wasDeletedOnce());

        store.runBackgroundOperations();

        mergedBranchCommit(b -> {
            b.child("a");
            b.child("b");
        });

        assertEquals(1, countCollisionsOnRoot());

        store.runBackgroundOperations();

        mergedBranchCommit(b -> {
            b.child("a").remove();
            b.child("b").remove();
        });

        store.runBackgroundOperations();

        int collisionsBeforeGC = countCollisionsOnRoot();
        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGarbageCollector.VersionGCStats stats = gc.gc(1, HOURS);

        if (collisionsBeforeGC == 1) {
            // expects a collision to have happened - which was cleaned up - hence a _bc (but not the _revision I guess)
            // the collisions cleaned up - with the 1 collision and a _bc
            assertStatsCountsEqual(stats,
                    new GCCounts(FullGCMode.NONE,
                                2, 0, 0, 0, 0, 0, 0),
                    gapOrphOnly(2, 0, 0, 0, 0, 0, 0),
                    empPropOnly(2, 0, 0, 0, 0, 0, 0),
                    gapOrphProp(2, 0, 0, 0, 0, 0, 0),
                    allOrphProp(2, 0, 0, 0, 0, 0, 0),
                    keepOneFull(2, 0, 1, 0, 2, 0, 1),
                    keepOneUser(2, 0, 0, 0, 0, 0, 0),
                    betweenChkp(2, 0, 0, 0, 0, 0, 0),
                    unmergedBcs(2, 0, 1, 0, 2, 1, 1),
                    btwnChkpUBC(2, 0, 1, 0, 2, 1, 1));
        } else {
            // in this case classic collision cleanup already took care of everything, nothing left
            assertStatsCountsEqual(stats,
                    new GCCounts(FullGCMode.NONE,
                                2, 0, 0, 0, 0, 0, 0),
                    gapOrphOnly(2, 0, 0, 0, 0, 0, 0),
                    empPropOnly(2, 0, 0, 0, 0, 0, 0),
                    gapOrphProp(2, 0, 0, 0, 0, 0, 0),
                    allOrphProp(2, 0, 0, 0, 0, 0, 0),
                    keepOneFull(2, 1, 0, 0, 0, 0, 0),
                    keepOneUser(2, 1, 0, 0, 0, 0, 0),
                    betweenChkp(2, 0, 0, 0, 0, 0, 0),
                    unmergedBcs(2, 0, 0, 0, 0, 0, 0),
                    btwnChkpUBC(2, 0, 0, 0, 0, 0, 0));
        }

        assertNotExists("1:/a");
        assertNotExists("1:/b");

        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        stats = gc.gc(1, HOURS);
        assertEquals(0, stats.updatedFullGCDocsCount);
        assertEquals(0, stats.deletedDocGCCount);
        assertEquals(0, stats.deletedUnmergedBCCount);
        assertBranchRevisionRemovedFromAllDocuments(store, br);
    }

    private int countCollisionsOnRoot() {
        NodeDocument r = store.getDocumentStore().find(Collection.NODES, "0:/");
        SortedMap<Revision, String> colls = r.getLocalMap(NodeDocument.COLLISIONS);
        int countCollisions = colls.size();
        return countCollisions;
    }

    @Test
    public void testDeletedPropsAndUnmergedBC() throws Exception {
        assumeTrue(fullGcMode != FullGCMode.ORPHANS_EMPTYPROPS_KEEP_ONE_ALL_PROPS);
        assumeTrue(fullGcMode != FullGCMode.ORPHANS_EMPTYPROPS_BETWEEN_CHECKPOINTS_WITH_UNMERGED_BC);

        // create a node with property.
        NodeBuilder nb = store.getRoot().builder();
        nb.child("bar").setProperty("prop", "value");
        nb.child("bar").setProperty("x", "y");
        store.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store.runBackgroundOperations();

        // remove the property
        nb = store.getRoot().builder();
        nb.child("bar").removeProperty("prop");
        store.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        store.runBackgroundOperations();

        // create branch commits
        mergedBranchCommit(b -> b.child("foo").setProperty("p", "prop"));
        RevisionVector br1 = unmergedBranchCommit(b -> b.child("foo").setProperty("a", "b"));
        RevisionVector br2 = unmergedBranchCommit(b -> b.child("foo").setProperty("a", "c"));
        RevisionVector br3 = unmergedBranchCommit(b -> b.child("foo").setProperty("a", "d"));
        RevisionVector br4 = unmergedBranchCommit(b -> b.child("bar").setProperty("x", "z"));
        mergedBranchCommit(b -> b.child("foo").removeProperty("p"));
        store.runBackgroundOperations();

        // enable the full gc flag
        writeField(gc, "fullGCEnabled", true, true);

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGarbageCollector.VersionGCStats stats = gc.gc(1, HOURS);

        // 6 deleted props: 0:/[_collisions], 1:/foo[p, a], 1:/bar[_bc,prop,_revisions]
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(0, 3, 0, 0, 0, 0, 2),
                gapOrphProp(0, 3, 0, 0, 0, 0, 2),
                allOrphProp(0, 3, 0, 0, 0, 0, 2),
                keepOneFull(0, 3, 2, 1,17, 0, 3),
                keepOneUser(0, 3, 0, 1, 0, 0, 2),
                betweenChkp(0, 3, 0, 0, 1, 0, 3),
                unmergedBcs(0, 3, 2, 1,15, 4, 3),
                btwnChkpUBC(0, 3, 2, 1,16, 4, 3));
        if (!isModeOneOf(FullGCMode.NONE, FullGCMode.GAP_ORPHANS)) {
            assertBranchRevisionRemovedFromAllDocuments(store, br1);
            assertBranchRevisionRemovedFromAllDocuments(store, br2);
            assertBranchRevisionRemovedFromAllDocuments(store, br3);
            assertBranchRevisionRemovedFromAllDocuments(store, br4);
        }
    }

    @Test
    public void unmergedAddTwoChildren() throws Exception {
        assumeTrue(fullGcMode != FullGCMode.ORPHANS_EMPTYPROPS_KEEP_ONE_ALL_PROPS);
        assumeTrue(fullGcMode != FullGCMode.ORPHANS_EMPTYPROPS_UNMERGED_BC);
        RevisionVector br1 = unmergedBranchCommit(b -> {
            b.child("a");
            b.child("b");
        });
        RevisionVector br2 = unmergedBranchCommit(b -> {
            b.child("a");
            b.child("b");
        });
        assertExists("1:/a");
        assertExists("1:/b");
        assertFalse(store.getDocumentStore().find(Collection.NODES, "1:/a").wasDeletedOnce());
        assertFalse(store.getDocumentStore().find(Collection.NODES, "1:/b").wasDeletedOnce());

        store.runBackgroundOperations();

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGarbageCollector.VersionGCStats stats = gc.gc(1, HOURS);

        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(),
                gapOrphProp(),
                allOrphProp(2, 0, 0, 0, 0, 0, 2),
                keepOneFull(2, 0, 2, 0, 2, 0, 3),
                keepOneUser(2, 0, 0, 0, 0, 0, 2),
                betweenChkp(2, 0, 0, 0, 0, 0, 2),
                unmergedBcs(2, 0, 2, 0, 2, 2, 3),
                btwnChkpUBC(2, 0, 2, 0, 2, 2, 3));

        if (!isModeOneOf(FullGCMode.NONE, FullGCMode.GAP_ORPHANS, FullGCMode.GAP_ORPHANS_EMPTYPROPS, FullGCMode.EMPTYPROPS)) {
            assertNotExists("1:/a");
            assertNotExists("1:/b");
        }

        // now do another gc - should not have anything left to clean up though
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        stats = gc.gc(1, HOURS);
        assertStatsCountsZero(stats);
        if (!isModeOneOf(FullGCMode.NONE)) {
            assertBranchRevisionRemovedFromAllDocuments(store, br1);
            assertBranchRevisionRemovedFromAllDocuments(store, br2);
        }
    }

    @Test
    public void unmergedAddsThenMergedAddsChildren() throws Exception {
        assumeTrue(fullGcMode != FullGCMode.ORPHANS_EMPTYPROPS_BETWEEN_CHECKPOINTS_WITH_UNMERGED_BC);
        RevisionVector br1 = unmergedBranchCommit(b -> {
            b.child("a");
            b.child("b");
        });
        RevisionVector br2 = unmergedBranchCommit(b -> {
            b.child("a");
            b.child("b");
        });
        assertExists("1:/a");
        assertExists("1:/b");
        assertFalse(store.getDocumentStore().find(Collection.NODES, "1:/a").wasDeletedOnce());
        assertFalse(store.getDocumentStore().find(Collection.NODES, "1:/b").wasDeletedOnce());

        store.runBackgroundOperations();

        mergedBranchCommit(b -> {
            b.child("a");
            b.child("b");
        });

        store.runBackgroundOperations();

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGarbageCollector.VersionGCStats stats = gc.gc(1, HOURS);

        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(),
                gapOrphProp(),
                allOrphProp(),
                keepOneFull(0, 0, 1, 4,12, 0, 3),
                keepOneUser(0, 0, 0, 4, 0, 0, 2),
                betweenChkp(),
                unmergedBcs(0, 0, 1, 0,16, 2, 3),
                btwnChkpUBC(0, 0, 1, 0,16, 2, 3));
        assertExists("1:/a");
        assertExists("1:/b");

        // now do another gc to get those documents actually deleted
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        stats = gc.gc(1, HOURS);
        assertStatsCountsZero(stats);
        if (!isModeOneOf(FullGCMode.NONE)) {
            assertBranchRevisionRemovedFromAllDocuments(store, br1);
            assertBranchRevisionRemovedFromAllDocuments(store, br2);
        }
    }

    @Test
    public void unmergedAddsThenMergedAddThenUnmergedRemovesChildren() throws Exception {
        assumeTrue(fullGcMode != FullGCMode.ORPHANS_EMPTYPROPS_UNMERGED_BC); // see OAK-10852
        assumeTrue(fullGcMode != FullGCMode.ORPHANS_EMPTYPROPS_KEEP_ONE_ALL_PROPS); // see OAK-10852
        RevisionVector br1 = unmergedBranchCommit(b -> {
            b.child("a");
            b.child("b");
        });
        RevisionVector br2 = unmergedBranchCommit(b -> {
            b.child("a");
            b.child("b");
        });
        assertExists("1:/a");
        assertExists("1:/b");
        assertFalse(store.getDocumentStore().find(Collection.NODES, "1:/a").wasDeletedOnce());
        assertFalse(store.getDocumentStore().find(Collection.NODES, "1:/b").wasDeletedOnce());

        store.runBackgroundOperations();

        mergedBranchCommit(b -> {
            b.child("a");
            b.child("b");
        });

        store.runBackgroundOperations();

        RevisionVector br3 = unmergedBranchCommit(b -> {
            b.child("a").remove();
            b.child("b").remove();
        });
        RevisionVector br4 = unmergedBranchCommit(b -> {
            b.child("a").remove();
            b.child("b").remove();
        });

        store.runBackgroundOperations();

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGarbageCollector.VersionGCStats stats = gc.gc(1, HOURS);

        assertStatsCountsEqual(stats,
                empPropOnly(0, 0, 0, 0, 0, 0, 0),
                gapOrphOnly(0, 0, 0, 0, 0, 0, 0),
                gapOrphProp(0, 0, 0, 0, 0, 0, 0),
                allOrphProp(0, 0, 0, 0, 0, 0, 0),
                keepOneFull(0, 0, 1, 8,24, 0, 3),
                keepOneUser(0, 0, 0, 8, 0, 0, 2),
                betweenChkp(),
                unmergedBcs(0, 0, 1, 0,32, 4, 3),
                btwnChkpUBC(0, 0, 1, 0,32, 4, 3));
        assertExists("1:/a");
        assertExists("1:/b");

        // now do another gc to get those documents actually deleted
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        stats = gc.gc(1, HOURS);
        assertStatsCountsZero(stats);
        if (!isModeOneOf(FullGCMode.NONE)) {
            assertBranchRevisionRemovedFromAllDocuments(store, br1);
            assertBranchRevisionRemovedFromAllDocuments(store, br2);
            assertBranchRevisionRemovedFromAllDocuments(store, br3);
            assertBranchRevisionRemovedFromAllDocuments(store, br4);
        }
    }

    @Test
    public void unmergedAddAndRemoveChild() throws Exception {
        mergedBranchCommit(b -> {
            b.child("foo");
            b.child("test");
        });
        RevisionVector br = unmergedBranchCommit(b -> {
            b.child("test").remove();
            b.getChildNode("foo").child("childFoo");
        });
        store.runBackgroundOperations();

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGarbageCollector.VersionGCStats stats = gc.gc(1, HOURS);

        // first gc round now deletes it, via orphaned node deletion
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(),
                gapOrphProp(),
                allOrphProp(1, 0, 0, 0, 0, 0, 1),
                keepOneFull(1, 0, 0, 1, 6, 0, 4),
                keepOneUser(1, 0, 0, 1, 0, 0, 2),
                betweenChkp(1, 0, 0, 0, 0, 0, 1),
                unmergedBcs(1, 0, 0, 0, 7, 1, 4),
                btwnChkpUBC(1, 0, 0, 0, 7, 1, 4));

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // now do second gc round - should not have anything left to clean up though
        stats = gc.gc(1, HOURS);
        assertStatsCountsZero(stats);
        if (!isModeOneOf(FullGCMode.NONE)) {
            assertBranchRevisionRemovedFromAllDocuments(store, br);
        }
    }

    @Test
    public void unmergedRemoveProperty() throws Exception {
        mergedBranchCommit(b -> b.child("foo"));
        mergedBranchCommit(b -> b.child("foo").setProperty("a", "b"));
        // do a gc without waiting, to check that works fine
        store.runBackgroundOperations();
        stats = gc.gc(1, HOURS);
        assertEquals(0, stats.updatedFullGCDocsCount);

        RevisionVector br = unmergedBranchCommit(b -> b.child("foo").removeProperty("a"));
        mergedBranchCommit(b -> b.child("foo").setProperty("c", "d"));
        store.runBackgroundOperations();

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        stats = gc.gc(1, HOURS);

        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(),
                gapOrphProp(),
                allOrphProp(),
                keepOneFull(0, 0, 0, 1, 4, 0, 2),
                keepOneUser(0, 0, 0, 1, 0, 0, 1),
                betweenChkp(),
                unmergedBcs(0, 0, 0, 1, 4, 1, 2),
                btwnChkpUBC(0, 0, 0, 1, 4, 1, 2));
        if (!isModeOneOf(FullGCMode.NONE)) {
            assertBranchRevisionRemovedFromAllDocuments(store, br);
        }
    }

    @Test
    public void unmergedAddProperty() throws Exception {
        mergedBranchCommit(b -> b.child("foo"));
        RevisionVector br = unmergedBranchCommit(b -> b.child("foo").setProperty("a", "b"));
        store.runBackgroundOperations();

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        stats = gc.gc(1, HOURS);

        // 1 deleted prop: 1:/foo[a]
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(0, 1, 0, 0, 0, 0, 1),
                gapOrphProp(0, 1, 0, 0, 0, 0, 1),
                allOrphProp(0, 1, 0, 0, 0, 0, 1),
                keepOneFull(0, 1, 0, 0, 4, 0, 2),
                keepOneUser(0, 1, 0, 0, 0, 0, 1),
                betweenChkp(0, 1, 0, 0, 0, 0, 1),
                unmergedBcs(0, 1, 0, 0, 4, 1, 2),
                btwnChkpUBC(0, 1, 0, 0, 4, 1, 2));
        if (!isModeOneOf(FullGCMode.NONE)) {
            assertBranchRevisionRemovedFromAllDocuments(store, br);
        }
    }

    @Test
    public void unmergedRemoveChild() throws Exception {
        assumeTrue(fullGcMode != FullGCMode.ORPHANS_EMPTYPROPS_KEEP_ONE_ALL_PROPS);
        assumeTrue(fullGcMode != FullGCMode.ORPHANS_EMPTYPROPS_BETWEEN_CHECKPOINTS_WITH_UNMERGED_BC);
        assumeTrue(fullGcMode != FullGCMode.ORPHANS_EMPTYPROPS_UNMERGED_BC); // OAK-11252
        mergedBranchCommit(b -> {
            b.child("foo");
            b.child("bar");
        });
        // do a gc without waiting, to check that works fine
        store.runBackgroundOperations();
        stats = gc.gc(1, HOURS);
        assertEquals(0, stats.updatedFullGCDocsCount);
        assertEquals(0, stats.deletedUnmergedBCCount);

        final List<RevisionVector> brs = new LinkedList<>();
        for (int j = 0; j < 10; j++) {
            brs.add(unmergedBranchCommit(b -> b.child("foo").remove()));
        }
        store.runBackgroundOperations();

        NodeDocument doc = store.getDocumentStore().find(Collection.NODES, "1:/foo");
        Long originalModified = doc.getModified();

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        stats = gc.gc(1, HOURS);

        assertStatsCountsEqual(stats,
                gapOrphOnly(0, 0, 0, 0, 0, 0, 0),
                empPropOnly(0, 0, 0, 0, 0, 0, 0),
                gapOrphProp(0, 0, 0, 0, 0, 0, 0),
                allOrphProp(0, 0, 0, 0, 0, 0, 0),
                keepOneFull(0, 0, 1,10,40, 0, 2),
                keepOneUser(0, 0, 0,10, 0, 0, 1),
                betweenChkp(),
                unmergedBcs(0, 0, 1, 0,50,10, 2),
                btwnChkpUBC(0, 0, 1, 0,50,10, 2));

        doc = store.getDocumentStore().find(Collection.NODES, "1:/foo");
        Long finalModified = doc.getModified();

        assertEquals(originalModified, finalModified);

        if (!isModeOneOf(FullGCMode.NONE)) {
            for (RevisionVector br : brs) {
                assertBranchRevisionRemovedFromAllDocuments(store, br);
            }
        }
    }

    @Test @Ignore("OAK-10845")
    public void unmergedMergedRemoveChild() throws Exception {
        mergedBranchCommit(b -> {
            b.child("foo");
            b.child("bar");
        });
        // do a gc without waiting, to check that works fine
        store.runBackgroundOperations();
        stats = gc.gc(1, HOURS);
        assertEquals(0, stats.updatedFullGCDocsCount);
        assertEquals(0, stats.deletedUnmergedBCCount);

        for (int i = 0; i < 50; i++) {
            mergedBranchCommit(b -> b.child("foo").remove());
            mergedBranchCommit(b -> b.child("foo"));
        }
        final List<RevisionVector> brs = new LinkedList<>();
        for (int j = 0; j < 10; j++) {
            brs.add(unmergedBranchCommit(b -> b.child("foo").remove()));
        }
        store.runBackgroundOperations();

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        stats = gc.gc(1, HOURS);

        assertStatsCountsEqual(stats,
                gapOrphOnly(0, 0, 0, 0, 0, 0, 0),
                empPropOnly(0, 0, 0, 0, 0, 0, 0),
                gapOrphProp(0, 0, 0, 0, 0, 0, 0),
                allOrphProp(0, 0, 0, 0, 0, 0, 0),
                keepOneFull(0, 0, 2,10,30, 0, 2),
                keepOneUser(0, 0, 0,10, 0, 0, 1),
                betweenChkp(),
                unmergedBcs(0, 0, 2, 0,40,10, 2),
                btwnChkpUBC(0, 0, 2, 0,40,10, 2));
        if (!isModeOneOf(FullGCMode.NONE)) {
            for (RevisionVector br : brs) {
                assertBranchRevisionRemovedFromAllDocuments(store, br);
            }
        }
    }

    @Test @Ignore("OAK-10852")
    public void unmergedThenMergedRemoveProperty() throws Exception {
        mergedBranchCommit(b -> b.child("foo"));
        mergedBranchCommit(b -> b.child("foo").setProperty("a", "b"));
        mergedBranchCommit(b -> b.child("foo").setProperty("c", "d"));
        // do a gc without waiting, to check that works fine
        store.runBackgroundOperations();
        stats = gc.gc(1, HOURS);
        assertEquals(0, stats.updatedFullGCDocsCount);
        assertEquals(0, stats.deletedUnmergedBCCount);
        assertEquals(0, stats.deletedPropsCount);
        assertEquals(0, stats.deletedPropRevsCount);

        RevisionVector br = unmergedBranchCommit(b -> {
            b.setProperty("rootProp", "v");
            b.child("foo").removeProperty("a");
        });
        store.runBackgroundOperations();
        DocumentNodeStore store2 = newStore(2);
        FullGCHelper.mergedBranchCommit(store2, b -> b.child("foo").removeProperty("a"));
        store2.runBackgroundOperations();
        store2.dispose();
        store.runBackgroundOperations();

        {
            // some flakyness diagnostics
            NodeDocument d = store.getDocumentStore().find(Collection.NODES, "0:/", -1);
            assertNotNull(d);
            assertEquals(1, d.getLocalMap("rootProp").size());
            assertEquals(1, d.getLocalMap("_collisions").size());
            d = store.getDocumentStore().find(Collection.NODES, "1:/foo", -1);
            assertNotNull(d);
            assertEquals(3, d.getLocalMap("a").size());
        }

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        stats = gc.gc(1, HOURS);

        // deleted props: 0:/[rootProp], 1:/foo[a]
        // deleted internal prop : 0:/ _collision
        // deleted properties are 0:/ -> rootProp, _collisions & 1:/foo -> a
        assertStatsCountsEqual(stats,
                gapOrphOnly(),
                empPropOnly(0, 2, 0, 0, 0, 0, 2),
                gapOrphProp(0, 2, 0, 0, 0, 0, 2),
                allOrphProp(0, 2, 0, 0, 0, 0, 2),
                keepOneFull(0, 2, 1, 0, 8, 0, 2),
                keepOneUser(0, 2, 0, 0, 0, 0, 2),
                betweenChkp(0, 2, 0, 0, 0, 0, 2),
                unmergedBcs(0, 2, 1, 0, 4, 1, 2),
                btwnChkpUBC(0, 2, 1, 0, 4, 1, 2));
        if (isModeOneOf(FullGCMode.NONE, FullGCMode.GAP_ORPHANS)) {
            return;
        }

        {
            // some flakyness diagnostics
            NodeDocument d = store.getDocumentStore().find(Collection.NODES, "0:/", -1);
            assertNotNull(d);
            assertEquals(0, d.getLocalMap("rootProp").size());
            if (VersionGarbageCollector.getFullGcMode() == FullGCMode.ORPHANS_EMPTYPROPS_KEEP_ONE_ALL_PROPS
                    || VersionGarbageCollector.getFullGcMode() == FullGCMode.ORPHANS_EMPTYPROPS_UNMERGED_BC
                    || VersionGarbageCollector.getFullGcMode() == FullGCMode.ORPHANS_EMPTYPROPS_BETWEEN_CHECKPOINTS_WITH_UNMERGED_BC) {
                assertEquals(0, d.getLocalMap("_collisions").size());
            } else {
                assertEquals(1, d.getLocalMap("_collisions").size());
            }
            d = store.getDocumentStore().find(Collection.NODES, "1:/foo", -1);
            assertNotNull(d);
            assertEquals(0, d.getLocalMap("a").size());
        }
        assertBranchRevisionRemovedFromAllDocuments(store, br);
    }


    // helper methods

    private DocumentNodeStore newStore(int clusterId) {
        Builder builder = builderProvider.newBuilder().clock(clock)
                .setLeaseCheckMode(LeaseCheckMode.DISABLED).setFullGCEnabled(true)
                .setAsyncDelay(0).setDocumentStore(store.getDocumentStore());
        if (clusterId > 0) {
            builder.setClusterId(clusterId);
        }
        DocumentNodeStore nodeStore = builder.getNodeStore();
        // OAK-11254 : adding a temporary sleep to reduce likelyhood of
        // backgroundPurge to interfere with test
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            fail("got interrupted");
        }
        return nodeStore;
    }

    private RevisionVector mergedBranchCommit(Consumer<NodeBuilder> buildFunction) throws Exception {
        return build(store, true, true, buildFunction);
    }

    private RevisionVector unmergedBranchCommit(Consumer<NodeBuilder> buildFunction) throws Exception {
        RevisionVector result = build(store, true, false, buildFunction);
        assertTrue(result.isBranch());
        return result;
    }

    private void assertNotExists(String id) {
        NodeDocument doc = store.getDocumentStore().find(Collection.NODES, id);
        assertNull("doc exists but was expected not to : id=" + id, doc);
    }

    private void assertExists(String id) {
        NodeDocument doc = store.getDocumentStore().find(Collection.NODES, id);
        assertNotNull("doc does not exist : id=" + id, doc);
    }

    private void assertNoEmptyProperties() {
        for (NodeDocument nd : Utils.getAllDocuments(store.getDocumentStore())) {
            for (Entry<String, Object> e : nd.data.entrySet()) {
                Object v = e.getValue();
                if (v instanceof Map) {
                    @SuppressWarnings("rawtypes")
                    Map m = (Map) v;
                    if (m.isEmpty() && (e.getKey().equals("_commitRoot") || e.getKey().equals("_collisions"))
                            && Objects.equals(nd.getId(), "0:/")) {
                        // skip : root document apparently has an empty _commitRoot:{}
                        continue;
                    }
                    assertFalse("document has empty property : id=" + nd.getId() +
                            ", property=" + e.getKey() + ", document=" + nd.asString(), m.isEmpty());
                }
            }
        }
    }

}
