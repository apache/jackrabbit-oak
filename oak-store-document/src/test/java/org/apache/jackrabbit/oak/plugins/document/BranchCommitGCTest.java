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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;

import org.apache.jackrabbit.oak.plugins.document.DocumentMK.Builder;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

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

    private void assertNoEmptyProperties() {
        for (NodeDocument nd : Utils.getAllDocuments(store.getDocumentStore())) {
            for (Entry<String, Object> e : nd.data.entrySet()) {
                Object v = e.getValue();
                if (v instanceof Map) {
                    @SuppressWarnings("rawtypes")
                    Map m = (Map) v;
                    if (m.isEmpty()
                            && (e.getKey().equals("_commitRoot")
                                    || e.getKey().equals("_collisions"))
                            && nd.getId().equals("0:/")) {
                        // skip : root document apparently has an empty _commitRoot:{}
                        continue;
                    }
                    assertFalse("document has empty property : id=" + nd.getId()
                            + ", property=" + e.getKey() + ", document=" + nd.asString(),
                            m.isEmpty());
                }
            }
        }
    }

    @Test
    public void unmergedAddChildren() throws Exception {
        RevisionVector br = unmergedBranchCommit(b -> {
            b.child("a");
            b.child("b");
        });
        assertExists("1:/a");
        assertExists("1:/b");
        assertFalse(
                store.getDocumentStore().find(Collection.NODES, "1:/a").wasDeletedOnce());
        assertFalse(
                store.getDocumentStore().find(Collection.NODES, "1:/b").wasDeletedOnce());

        store.runBackgroundOperations();

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGarbageCollector.VersionGCStats stats = gc.gc(1, HOURS);

        assertEquals(3, stats.updatedDetailedGCDocsCount);

        assertTrue(
                store.getDocumentStore().find(Collection.NODES, "1:/a").wasDeletedOnce());
        assertTrue(
                store.getDocumentStore().find(Collection.NODES, "1:/b").wasDeletedOnce());

        // now do another gc to get those documents actually deleted
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        stats = gc.gc(1, HOURS);
        assertEquals(0, stats.updatedDetailedGCDocsCount);
        assertEquals(2, stats.deletedDocGCCount);
        assertNotExists("1:/a");
        assertNotExists("1:/b");
        assertBranchRevisionRemovedFromAllDocuments(br);
    }

    @Test
    public void unmergedAddThenMergedAddAndRemoveChildren() throws Exception {
        RevisionVector br = unmergedBranchCommit(b -> {
            b.child("a");
            b.child("b");
        });
        assertExists("1:/a");
        assertExists("1:/b");
        assertFalse(
                store.getDocumentStore().find(Collection.NODES, "1:/a").wasDeletedOnce());
        assertFalse(
                store.getDocumentStore().find(Collection.NODES, "1:/b").wasDeletedOnce());

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

        assertTrue(
                "stats.updatedDetailedGCDocsCount expected 1 or less, was: "
                        + stats.updatedDetailedGCDocsCount,
                stats.updatedDetailedGCDocsCount <= 1);
        assertEquals(2, stats.deletedDocGCCount);

        assertNotExists("1:/a");
        assertNotExists("1:/b");

        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        stats = gc.gc(1, HOURS);
        assertEquals(0, stats.updatedDetailedGCDocsCount);
        assertEquals(0, stats.deletedDocGCCount);
        assertBranchRevisionRemovedFromAllDocuments(br);
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
        assertFalse(
                store.getDocumentStore().find(Collection.NODES, "1:/a").wasDeletedOnce());
        assertFalse(
                store.getDocumentStore().find(Collection.NODES, "1:/b").wasDeletedOnce());

        store.runBackgroundOperations();

        Long originalModified = store.getDocumentStore().find(Collection.NODES, "1:/a")
                .getModified();

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hour
        VersionGarbageCollector.VersionGCStats stats = gc.gc(1, HOURS);

        Long laterModified = store.getDocumentStore().find(Collection.NODES, "1:/a")
                .getModified();
        assertNotEquals(originalModified, laterModified);

        assertEquals(3, stats.updatedDetailedGCDocsCount);
        assertEquals(0, stats.deletedDocGCCount);

        assertExists("1:/a");
        assertExists("1:/b");

        // now do another gc to get those documents actually deleted
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        stats = gc.gc(1, HOURS);
        assertEquals(0, stats.updatedDetailedGCDocsCount);
        assertEquals(2, stats.deletedDocGCCount);
        assertBranchRevisionRemovedFromAllDocuments(br1);
        assertBranchRevisionRemovedFromAllDocuments(br2);
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
        assertFalse(
                store.getDocumentStore().find(Collection.NODES, "1:/a").wasDeletedOnce());
        assertFalse(
                store.getDocumentStore().find(Collection.NODES, "1:/b").wasDeletedOnce());

        store.runBackgroundOperations();

        mergedBranchCommit(b -> {
            b.child("a");
            b.child("b");
        });

        store.runBackgroundOperations();

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hours
        VersionGarbageCollector.VersionGCStats stats = gc.gc(1, HOURS);

        assertTrue("should have been 2 or more, was: " + stats.updatedDetailedGCDocsCount,
                stats.updatedDetailedGCDocsCount >= 2);
        assertEquals(0, stats.deletedDocGCCount);

        assertExists("1:/a");
        assertExists("1:/b");

        // now do another gc to get those documents actually deleted
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        stats = gc.gc(1, HOURS);
        assertEquals(0, stats.updatedDetailedGCDocsCount);
        assertEquals(0, stats.deletedDocGCCount);
        assertBranchRevisionRemovedFromAllDocuments(br1);
        assertBranchRevisionRemovedFromAllDocuments(br2);
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
        assertFalse(
                store.getDocumentStore().find(Collection.NODES, "1:/a").wasDeletedOnce());
        assertFalse(
                store.getDocumentStore().find(Collection.NODES, "1:/b").wasDeletedOnce());

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

        assertExists("1:/a");
        assertExists("1:/b");

        // now do another gc to get those documents actually deleted
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        stats = gc.gc(1, HOURS);
        assertEquals(0, stats.updatedDetailedGCDocsCount);
        assertEquals(0, stats.deletedDocGCCount);
        assertBranchRevisionRemovedFromAllDocuments(br1);
        assertBranchRevisionRemovedFromAllDocuments(br2);
        assertBranchRevisionRemovedFromAllDocuments(br3);
        assertBranchRevisionRemovedFromAllDocuments(br4);
    }

    private void assertNotExists(String id) {
        NodeDocument doc = store.getDocumentStore().find(Collection.NODES, id);
        assertNull("doc exists but was expected not to : id=" + id, doc);
    }

    private void assertExists(String id) {
        NodeDocument doc = store.getDocumentStore().find(Collection.NODES, id);
        assertNotNull("doc does not exist : id=" + id, doc);
    }

    @Test
    public void unmergedAddAndRemoveChild() throws Exception {
        mergedBranchCommit(b -> {
            b.child("foo");
            b.child("test");
        });
        RevisionVector br = unmergedBranchCommit(b -> {
            b.child("test").remove();
            b.getChildNode("foo").child("childfoo");
        });
        store.runBackgroundOperations();

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hours
        VersionGarbageCollector.VersionGCStats stats = gc.gc(1, HOURS);

        // first gc round will only mark document for deleting by second round
        assertEquals(0, stats.deletedDocGCCount);
        assertEquals(4, stats.updatedDetailedGCDocsCount);

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // now do second gc round to get those documents actually deleted
        stats = gc.gc(1, HOURS);
        assertEquals(0, stats.updatedDetailedGCDocsCount);
        assertEquals(1, stats.deletedDocGCCount);
        assertBranchRevisionRemovedFromAllDocuments(br);
    }

    private void assertBranchRevisionRemovedFromAllDocuments(RevisionVector br) {
        assertTrue(br.isBranch());
        Revision br1 = br.getRevision(1);
        Revision r1 = br1.asTrunkRevision();
        for (NodeDocument nd : Utils.getAllDocuments(store.getDocumentStore())) {
            if (nd.getId().equals("0:/")) {
                NodeDocument target = new NodeDocument(store.getDocumentStore());
                nd.deepCopy(target);
            }
            System.out.println("nd=" + nd.asString());
            if (nd.asString().contains(r1.toString())) {
                System.out.println("break");
            }
            assertFalse("document not fully cleaned up: " + nd.asString(),
                    nd.asString().contains(r1.toString()));
        }
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
        // clean everything older than one hours
        stats = gc.gc(1, HOURS);

        assertEquals(2, stats.updatedDetailedGCDocsCount);
        assertBranchRevisionRemovedFromAllDocuments(br);
    }

    @Test
    public void unmergedAddProperty() throws Exception {
        mergedBranchCommit(b -> b.child("foo"));
        RevisionVector br = unmergedBranchCommit(
                b -> b.child("foo").setProperty("a", "b"));
        store.runBackgroundOperations();

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hours
        stats = gc.gc(1, HOURS);

        assertEquals(2, stats.updatedDetailedGCDocsCount);
        assertBranchRevisionRemovedFromAllDocuments(br);
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

        final List<RevisionVector> brs = new LinkedList<>();
        for (int j = 0; j < 10; j++) {
            brs.add(unmergedBranchCommit(b -> b.child("foo").remove()));
        }
        store.runBackgroundOperations();

        NodeDocument doc = store.getDocumentStore().find(Collection.NODES, "1:/foo");
        Long originalModified = doc.getModified();

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hours
        stats = gc.gc(1, HOURS);

        assertEquals(2, stats.updatedDetailedGCDocsCount);

        doc = store.getDocumentStore().find(Collection.NODES, "1:/foo");
        Long finalModified = doc.getModified();

        assertEquals(originalModified, finalModified);

        for (RevisionVector br : brs) {
            assertBranchRevisionRemovedFromAllDocuments(br);
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
        // clean everything older than one hours
        stats = gc.gc(1, HOURS);

        assertEquals(2, stats.updatedDetailedGCDocsCount);
        for (RevisionVector br : brs) {
            assertBranchRevisionRemovedFromAllDocuments(br);
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

        RevisionVector br = unmergedBranchCommit(b -> {
            b.setProperty("rootProp", "v");
            b.child("foo").removeProperty("a");
        });
        store.runBackgroundOperations();
        DocumentNodeStore store2 = newStore(2);
        mergedBranchCommit(store2, b -> b.child("foo").removeProperty("a"));
        store2.runBackgroundOperations();
        store2.dispose();
        store.runBackgroundOperations();

        // wait two hours
        clock.waitUntil(clock.getTime() + HOURS.toMillis(2));
        // clean everything older than one hours
        stats = gc.gc(1, HOURS);

        assertEquals(2, stats.updatedDetailedGCDocsCount);
        assertBranchRevisionRemovedFromAllDocuments(br);
    }

    private DocumentNodeStore newStore(int clusterId) {
        Builder builder = builderProvider.newBuilder().clock(clock)
                .setLeaseCheckMode(LeaseCheckMode.DISABLED).setDetailedGCEnabled(true)
                .setAsyncDelay(0).setDocumentStore(store.getDocumentStore());
        if (clusterId > 0) {
            builder.setClusterId(clusterId);
        }
        DocumentNodeStore store2 = builder.getNodeStore();
        return store2;
    }

    private RevisionVector mergedBranchCommit(Consumer<NodeBuilder> buildFunction)
            throws Exception {
        return build(store, true, true, buildFunction);
    }

    private RevisionVector mergedBranchCommit(NodeStore store,
            Consumer<NodeBuilder> buildFunction) throws Exception {
        return build(store, true, true, buildFunction);
    }

    private RevisionVector unmergedBranchCommit(Consumer<NodeBuilder> buildFunction)
            throws Exception {
        RevisionVector result = build(store, true, false, buildFunction);
        assertTrue(result.isBranch());
        return result;
    }

    private RevisionVector build(NodeStore store, boolean persistToBranch, boolean merge,
            Consumer<NodeBuilder> buildFunction) throws Exception {
        if (!persistToBranch && !merge) {
            throw new IllegalArgumentException("must either persistToBranch or merge");
        }
        NodeBuilder b = store.getRoot().builder();
        buildFunction.accept(b);
        RevisionVector result = null;
        if (persistToBranch) {
            DocumentRootBuilder drb = (DocumentRootBuilder) b;
            drb.persist();
            DocumentNodeState ns = (DocumentNodeState) drb.getNodeState();
            result = ns.getLastRevision();
        }
        if (merge) {
            DocumentNodeState dns = (DocumentNodeState) merge(b);
            result = dns.getLastRevision();
        }
        return result;
    }

    private NodeState merge(NodeBuilder builder) throws Exception {
        return store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

}
