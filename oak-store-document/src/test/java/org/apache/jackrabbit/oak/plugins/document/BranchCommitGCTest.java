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

import org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Consumer;

import static java.util.concurrent.TimeUnit.HOURS;
import static org.apache.commons.lang3.reflect.FieldUtils.writeField;
import static org.apache.jackrabbit.oak.plugins.document.DetailGCHelper.assertBranchRevisionRemovedFromAllDocuments;
import static org.apache.jackrabbit.oak.plugins.document.DetailGCHelper.build;
import static org.apache.jackrabbit.oak.plugins.document.DetailGCHelper.mergedBranchCommit;
import static org.apache.jackrabbit.oak.plugins.document.DetailGCHelper.unmergedBranchCommit;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class BranchCommitGCTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();
    private Clock clock;
    private DocumentNodeStore store;
    private VersionGarbageCollector gc;
    private VersionGarbageCollector.VersionGCStats stats;

    @Before
    public void setUp() throws InterruptedException {
        clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        store = builderProvider.newBuilder().clock(clock)
                .setLeaseCheckMode(LeaseCheckMode.DISABLED).setDetailedGCEnabled(true)
                .setAsyncDelay(0).getNodeStore();
        gc = store.getVersionGarbageCollector();
    }

    @After
    public void tearDown() throws Exception {
        assertNoEmptyProperties();
        if (store != null) {
            store.dispose();
        }
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

        assertEquals(3, stats.updatedDetailedGCDocsCount);
        assertEquals(1, stats.deletedUnmergedBCCount);

        assertTrue(store.getDocumentStore().find(Collection.NODES, "1:/a").wasDeletedOnce());
        assertTrue(store.getDocumentStore().find(Collection.NODES, "1:/b").wasDeletedOnce());

        // now do another gc to get those documents actually deleted
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        stats = gc.gc(1, HOURS);
        assertEquals(0, stats.updatedDetailedGCDocsCount);
        assertEquals(2, stats.deletedDocGCCount);
        assertEquals(0, stats.deletedUnmergedBCCount);
        assertNotExists("1:/a");
        assertNotExists("1:/b");
        assertBranchRevisionRemovedFromAllDocuments(store, br);
    }

    @Test
    public void unmergedAddThenMergedAddAndRemoveChildren() throws Exception {
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

        store.runBackgroundOperations();

        mergedBranchCommit(b -> {
            b.child("a").remove();
            b.child("b").remove();
        });

        store.runBackgroundOperations();

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGarbageCollector.VersionGCStats stats = gc.gc(1, HOURS);

        assertTrue("stats.updatedDetailedGCDocsCount expected 1 or less, was: " + stats.updatedDetailedGCDocsCount,
                stats.updatedDetailedGCDocsCount <= 1);
        assertEquals(2, stats.deletedDocGCCount);
        assertEquals(1, stats.deletedUnmergedBCCount);

        assertNotExists("1:/a");
        assertNotExists("1:/b");

        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        stats = gc.gc(1, HOURS);
        assertEquals(0, stats.updatedDetailedGCDocsCount);
        assertEquals(0, stats.deletedDocGCCount);
        assertEquals(0, stats.deletedUnmergedBCCount);
        assertBranchRevisionRemovedFromAllDocuments(store, br);
    }

    @Test
    public void testDeletedPropsAndUnmergedBC() throws Exception {
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

        // enable the detailed gc flag
        writeField(gc, "detailedGCEnabled", true, true);

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGarbageCollector.VersionGCStats stats = gc.gc(1, HOURS);

        assertEquals(3, stats.updatedDetailedGCDocsCount);
        // deleted properties are : 1:/foo -> prop, a, _collisions & p && 1:/bar -> _bc
        assertEquals(5, stats.deletedPropsCount);
        assertEquals(4, stats.deletedUnmergedBCCount);
        assertBranchRevisionRemovedFromAllDocuments(store, br1);
        assertBranchRevisionRemovedFromAllDocuments(store, br2);
        assertBranchRevisionRemovedFromAllDocuments(store, br3);
        assertBranchRevisionRemovedFromAllDocuments(store, br4);
    }

    @Test
    public void unmergedAddTwoChildren() throws Exception {
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

        Long originalModified = store.getDocumentStore().find(Collection.NODES, "1:/a").getModified();

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGarbageCollector.VersionGCStats stats = gc.gc(1, HOURS);

        Long laterModified = store.getDocumentStore().find(Collection.NODES, "1:/a").getModified();
        assertNotEquals(originalModified, laterModified);

        assertEquals(3, stats.updatedDetailedGCDocsCount);
        assertEquals(0, stats.deletedDocGCCount);
        assertEquals(2, stats.deletedUnmergedBCCount);

        assertExists("1:/a");
        assertExists("1:/b");

        // now do another gc to get those documents actually deleted
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        stats = gc.gc(1, HOURS);
        assertEquals(0, stats.updatedDetailedGCDocsCount);
        assertEquals(2, stats.deletedDocGCCount);
        assertEquals(0, stats.deletedUnmergedBCCount);
        assertBranchRevisionRemovedFromAllDocuments(store, br1);
        assertBranchRevisionRemovedFromAllDocuments(store, br2);
    }

    @Test
    public void unmergedAddsThenMergedAddsChildren() throws Exception {
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

        assertTrue("should have been 2 or more, was: " + stats.updatedDetailedGCDocsCount,
                stats.updatedDetailedGCDocsCount >= 2);
        assertEquals(0, stats.deletedDocGCCount);
        assertEquals(2, stats.deletedUnmergedBCCount);

        assertExists("1:/a");
        assertExists("1:/b");

        // now do another gc to get those documents actually deleted
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        stats = gc.gc(1, HOURS);
        assertEquals(0, stats.updatedDetailedGCDocsCount);
        assertEquals(0, stats.deletedDocGCCount);
        assertEquals(0, stats.deletedUnmergedBCCount);
        assertBranchRevisionRemovedFromAllDocuments(store, br1);
        assertBranchRevisionRemovedFromAllDocuments(store, br2);
    }

    @Test
    public void unmergedAddsThenMergedAddThenUnmergedRemovesChildren() throws Exception {
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

        assertEquals(3, stats.updatedDetailedGCDocsCount);
        assertEquals(0, stats.deletedDocGCCount);
        assertEquals(4, stats.deletedUnmergedBCCount);

        assertExists("1:/a");
        assertExists("1:/b");

        // now do another gc to get those documents actually deleted
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        stats = gc.gc(1, HOURS);
        assertEquals(0, stats.updatedDetailedGCDocsCount);
        assertEquals(0, stats.deletedDocGCCount);
        assertEquals(0, stats.deletedUnmergedBCCount);
        assertBranchRevisionRemovedFromAllDocuments(store, br1);
        assertBranchRevisionRemovedFromAllDocuments(store, br2);
        assertBranchRevisionRemovedFromAllDocuments(store, br3);
        assertBranchRevisionRemovedFromAllDocuments(store, br4);
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

        // first gc round will only mark document for deleting by second round
        assertEquals(0, stats.deletedDocGCCount);
        assertEquals(4, stats.updatedDetailedGCDocsCount);
        assertEquals(1, stats.deletedUnmergedBCCount);

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // now do second gc round to get those documents actually deleted
        stats = gc.gc(1, HOURS);
        assertEquals(0, stats.updatedDetailedGCDocsCount);
        assertEquals(1, stats.deletedDocGCCount);
        assertEquals(0, stats.deletedUnmergedBCCount);
        assertBranchRevisionRemovedFromAllDocuments(store, br);
    }

    @Test
    public void unmergedRemoveProperty() throws Exception {
        mergedBranchCommit(b -> b.child("foo"));
        mergedBranchCommit(b -> b.child("foo").setProperty("a", "b"));
        // do a gc without waiting, to check that works fine
        store.runBackgroundOperations();
        stats = gc.gc(1, HOURS);
        assertEquals(0, stats.updatedDetailedGCDocsCount);

        RevisionVector br = unmergedBranchCommit(b -> b.child("foo").removeProperty("a"));
        mergedBranchCommit(b -> b.child("foo").setProperty("c", "d"));
        store.runBackgroundOperations();

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        stats = gc.gc(1, HOURS);

        assertEquals(2, stats.updatedDetailedGCDocsCount);
        assertEquals(0, stats.deletedPropsCount);
        assertEquals(1, stats.deletedUnmergedBCCount);
        assertBranchRevisionRemovedFromAllDocuments(store, br);
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

        assertEquals(2, stats.updatedDetailedGCDocsCount);
        assertEquals(1, stats.deletedPropsCount);
        assertEquals(1, stats.deletedUnmergedBCCount);
        assertBranchRevisionRemovedFromAllDocuments(store, br);
    }

    @Test
    public void unmergedRemoveChild() throws Exception {
        mergedBranchCommit(b -> {
            b.child("foo");
            b.child("bar");
        });
        // do a gc without waiting, to check that works fine
        store.runBackgroundOperations();
        stats = gc.gc(1, HOURS);
        assertEquals(0, stats.updatedDetailedGCDocsCount);
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

        assertEquals(2, stats.updatedDetailedGCDocsCount);
        assertEquals(10, stats.deletedUnmergedBCCount);

        doc = store.getDocumentStore().find(Collection.NODES, "1:/foo");
        Long finalModified = doc.getModified();

        assertEquals(originalModified, finalModified);

        for (RevisionVector br : brs) {
            assertBranchRevisionRemovedFromAllDocuments(store, br);
        }
    }

    @Test
    public void unmergedMergedRemoveChild() throws Exception {
        mergedBranchCommit(b -> {
            b.child("foo");
            b.child("bar");
        });
        // do a gc without waiting, to check that works fine
        store.runBackgroundOperations();
        stats = gc.gc(1, HOURS);
        assertEquals(0, stats.updatedDetailedGCDocsCount);
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

        assertEquals(2, stats.updatedDetailedGCDocsCount);
        assertEquals(10, stats.deletedUnmergedBCCount);
        for (RevisionVector br : brs) {
            assertBranchRevisionRemovedFromAllDocuments(store, br);
        }
    }

    @Test
    public void unmergedThenMergedRemoveProperty() throws Exception {
        mergedBranchCommit(b -> b.child("foo"));
        mergedBranchCommit(b -> b.child("foo").setProperty("a", "b"));
        mergedBranchCommit(b -> b.child("foo").setProperty("c", "d"));
        // do a gc without waiting, to check that works fine
        store.runBackgroundOperations();
        stats = gc.gc(1, HOURS);
        assertEquals(0, stats.updatedDetailedGCDocsCount);
        assertEquals(0, stats.deletedUnmergedBCCount);
        assertEquals(0, stats.deletedPropsCount);

        RevisionVector br = unmergedBranchCommit(b -> {
            b.setProperty("rootProp", "v");
            b.child("foo").removeProperty("a");
        });
        store.runBackgroundOperations();
        DocumentNodeStore store2 = newStore(2);
        DetailGCHelper.mergedBranchCommit(store2, b -> b.child("foo").removeProperty("a"));
        store2.runBackgroundOperations();
        store2.dispose();
        store.runBackgroundOperations();

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        stats = gc.gc(1, HOURS);

        assertEquals(2, stats.updatedDetailedGCDocsCount);
        assertEquals(1, stats.deletedUnmergedBCCount);
        // deleted properties are 0:/ -> rootProp, _collisions & 1:/foo -> a
        assertEquals(3, stats.deletedPropsCount);
        assertBranchRevisionRemovedFromAllDocuments(store, br);
    }


    // helper methods

    private DocumentNodeStore newStore(int clusterId) {
        Builder builder = builderProvider.newBuilder().clock(clock)
                .setLeaseCheckMode(LeaseCheckMode.DISABLED).setDetailedGCEnabled(true)
                .setAsyncDelay(0).setDocumentStore(store.getDocumentStore());
        if (clusterId > 0) {
            builder.setClusterId(clusterId);
        }
        return builder.getNodeStore();
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
