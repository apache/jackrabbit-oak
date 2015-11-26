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

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link NodeDocument}.
 */
public class NodeDocumentTest {

    @Test
    public void splitCollisions() throws Exception {
        MemoryDocumentStore docStore = new MemoryDocumentStore();
        String id = Utils.getPathFromId("/");
        NodeDocument doc = new NodeDocument(docStore);
        doc.put(Document.ID, id);
        UpdateOp op = new UpdateOp(id, false);
        for (int i = 0; i < NodeDocument.NUM_REVS_THRESHOLD + 1; i++) {
            Revision r = Revision.newRevision(1);
            NodeDocument.setRevision(op, r, "c");
            NodeDocument.addCollision(op, r);
        }
        UpdateUtils.applyChanges(doc, op);
        Revision head = DummyRevisionContext.INSTANCE.getHeadRevision();
        doc.split(DummyRevisionContext.INSTANCE, head);
    }

    @Test
    public void getAllChanges() throws Exception {
        final int NUM_CHANGES = 200;
        DocumentNodeStore ns = createTestStore(NUM_CHANGES);
        Revision previous = ns.newRevision();
        NodeDocument root = getRootDocument(ns.getDocumentStore());
        for (Revision r : root.getAllChanges()) {
            assertTrue(previous.compareRevisionTime(r) > 0);
            previous = r;
        }
        // NUM_CHANGES + one revision when node was created
        assertEquals(NUM_CHANGES + 1, Iterables.size(root.getAllChanges()));
        ns.dispose();
    }

    @Test
    public void getAllChangesAfterGC1() throws Exception {
        int numChanges = 200;
        DocumentNodeStore ns = createTestStore(numChanges);
        NodeDocument root = getRootDocument(ns.getDocumentStore());
        // remove most recent previous doc
        NodeDocument toRemove = root.getAllPreviousDocs().next();
        int numDeleted = new SplitDocumentCleanUp(ns.store, new VersionGCStats(),
                Collections.singleton(toRemove)).disconnect().deleteSplitDocuments();
        assertEquals(1, numDeleted);
        numChanges -= Iterables.size(toRemove.getAllChanges());

        root = getRootDocument(ns.getDocumentStore());
        Revision previous = ns.newRevision();
        for (Revision r : root.getAllChanges()) {
            assertTrue(previous.compareRevisionTime(r) > 0);
            previous = r;
        }
        // numChanges + one revision when node was created
        assertEquals(numChanges + 1, Iterables.size(root.getAllChanges()));
        ns.dispose();
    }

    @Test
    public void getAllChangesAfterGC2() throws Exception {
        int numChanges = 200;
        DocumentNodeStore ns = createTestStore(numChanges);
        NodeDocument root = getRootDocument(ns.getDocumentStore());
        // remove oldest previous doc
        NodeDocument toRemove = Iterators.getLast(root.getAllPreviousDocs());
        int numDeleted = new SplitDocumentCleanUp(ns.store, new VersionGCStats(),
                Collections.singleton(toRemove)).disconnect().deleteSplitDocuments();
        assertEquals(1, numDeleted);
        numChanges -= Iterables.size(toRemove.getAllChanges());

        root = getRootDocument(ns.getDocumentStore());
        Revision previous = ns.newRevision();
        for (Revision r : root.getAllChanges()) {
            assertTrue(previous.compareRevisionTime(r) > 0);
            previous = r;
        }
        // numChanges + one revision when node was created
        assertEquals(numChanges + 1, Iterables.size(root.getAllChanges()));
        ns.dispose();
    }

    @Test
    public void getAllChangesCluster() throws Exception {
        final int NUM_CLUSTER_NODES = 3;
        final int NUM_CHANGES = 500;
        DocumentStore store = new MemoryDocumentStore();
        List<DocumentNodeStore> docStores = Lists.newArrayList();
        for (int i = 0; i < NUM_CLUSTER_NODES; i++) {
            DocumentNodeStore ns = new DocumentMK.Builder()
                    .setDocumentStore(store)
                    .setAsyncDelay(0).getNodeStore();
            docStores.add(ns);
        }
        Random r = new Random(42);
        for (int i = 0; i < NUM_CHANGES; i++) {
            // randomly pick a clusterNode
            int clusterIdx = r.nextInt(NUM_CLUSTER_NODES);
            DocumentNodeStore ns = docStores.get(clusterIdx);
            NodeBuilder builder = ns.getRoot().builder();
            builder.setProperty("p-" + clusterIdx, i);
            merge(ns, builder);
            if (r.nextFloat() < 0.2) {
                Revision head = ns.getHeadRevision();
                for (UpdateOp op : SplitOperations.forDocument(
                        getRootDocument(store), ns, head, 2)) {
                    store.createOrUpdate(NODES, op);
                }
            }
        }
        DocumentNodeStore ns = docStores.get(0);
        NodeDocument root = getRootDocument(ns.getDocumentStore());
        Revision previous = ns.newRevision();
        for (Revision rev : root.getAllChanges()) {
            assertTrue(previous.compareRevisionTimeThenClusterId(rev) > 0);
            previous = rev;
        }
        // numChanges + one revision when node was created
        assertEquals(NUM_CHANGES + 1, Iterables.size(root.getAllChanges()));

        for (DocumentNodeStore dns : docStores) {
            dns.dispose();
        }
    }

    @Test
    public void getPreviousDocLeaves() throws Exception {
        DocumentNodeStore ns = createTestStore(200);
        Revision previous = ns.newRevision();
        NodeDocument root = getRootDocument(ns.getDocumentStore());
        Iterator<NodeDocument> it = root.getPreviousDocLeaves();
        while (it.hasNext()) {
            NodeDocument leaf = it.next();
            Revision r = leaf.getAllChanges().iterator().next();
            assertTrue(previous.compareRevisionTime(r) > 0);
            previous = r;
        }
        ns.dispose();
    }

    @Test
    public void getPreviousDocLeavesAfterGC1() throws Exception {
        DocumentNodeStore ns = createTestStore(200);
        Revision previous = ns.newRevision();
        NodeDocument root = getRootDocument(ns.getDocumentStore());
        int numLeaves = Iterators.size(root.getPreviousDocLeaves());
        // remove most recent previous doc
        NodeDocument toRemove = root.getAllPreviousDocs().next();
        int numDeleted = new SplitDocumentCleanUp(ns.store, new VersionGCStats(),
                Collections.singleton(toRemove)).disconnect().deleteSplitDocuments();
        assertEquals(1, numDeleted);

        root = getRootDocument(ns.getDocumentStore());
        assertEquals(numLeaves - 1, Iterators.size(root.getPreviousDocLeaves()));
        Iterator<NodeDocument> it = root.getPreviousDocLeaves();
        while (it.hasNext()) {
            NodeDocument leaf = it.next();
            Revision r = leaf.getAllChanges().iterator().next();
            assertTrue(previous.compareRevisionTime(r) > 0);
            previous = r;
        }
        ns.dispose();
    }

    @Test
    public void getPreviousDocLeavesAfterGC2() throws Exception {
        DocumentNodeStore ns = createTestStore(200);
        Revision previous = ns.newRevision();
        NodeDocument root = getRootDocument(ns.getDocumentStore());
        int numLeaves = Iterators.size(root.getPreviousDocLeaves());
        // remove oldest previous doc
        NodeDocument toRemove = Iterators.getLast(root.getAllPreviousDocs());
        int numDeleted = new SplitDocumentCleanUp(ns.store, new VersionGCStats(),
                Collections.singleton(toRemove)).disconnect().deleteSplitDocuments();
        assertEquals(1, numDeleted);

        root = getRootDocument(ns.getDocumentStore());
        assertEquals(numLeaves - 1, Iterators.size(root.getPreviousDocLeaves()));
        Iterator<NodeDocument> it = root.getPreviousDocLeaves();
        while (it.hasNext()) {
            NodeDocument leaf = it.next();
            Revision r = leaf.getAllChanges().iterator().next();
            assertTrue(previous.compareRevisionTime(r) > 0);
            previous = r;
        }
        ns.dispose();
    }

    @Test
    public void getNewestRevisionTooExpensive() throws Exception {
        final int NUM_CHANGES = 200;
        final Set<String> prevDocCalls = Sets.newHashSet();
        DocumentStore store = new MemoryDocumentStore() {
            @Override
            public <T extends Document> T find(Collection<T> collection,
                                               String key) {
                if (Utils.getPathFromId(key).startsWith("p")) {
                    prevDocCalls.add(key);
                }
                return super.find(collection, key);
            }
        };
        DocumentNodeStore ns = new DocumentMK.Builder()
                .setDocumentStore(store)
                .setAsyncDelay(0).getNodeStore();
        // create test data
        for (int i = 0; i < NUM_CHANGES; i++) {
            NodeBuilder builder = ns.getRoot().builder();
            if (builder.hasChildNode("test")) {
                builder.child("test").remove();
                builder.child("foo").remove();
            } else {
                builder.child("test");
                builder.child("foo");
            }
            merge(ns, builder);
            if (Math.random() < 0.2) {
                Revision head = ns.getHeadRevision();
                NodeDocument doc = ns.getDocumentStore().find(
                        NODES, Utils.getIdFromPath("/test"));
                for (UpdateOp op : SplitOperations.forDocument(
                        doc, ns, head, 2)) {
                    store.createOrUpdate(NODES, op);
                }
            }
        }
        NodeDocument doc = ns.getDocumentStore().find(
                NODES, Utils.getIdFromPath("/test"));
        // get most recent previous doc
        NodeDocument prev = doc.getAllPreviousDocs().next();
        // simulate a change revision within the range of
        // the most recent previous document
        Iterable<Revision> changes = prev.getAllChanges();
        Revision baseRev = Iterables.getLast(changes);
        Revision changeRev = new Revision(baseRev.getTimestamp(), 1000, ns.getClusterId());
        // reset calls to previous documents
        prevDocCalls.clear();
        doc.getNewestRevision(ns, baseRev, changeRev, null, new HashSet<Revision>());
        // must not read all previous docs
        assertTrue("too many calls for previous documents: " + prevDocCalls,
                prevDocCalls.size() <= 5);

        ns.dispose();
    }

    @Test
    public void getNewestRevision() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns1 = createTestStore(store, 0);
        DocumentNodeStore ns2 = createTestStore(store, 0);

        NodeBuilder b1 = ns1.getRoot().builder();
        b1.child("test");
        merge(ns1, b1);
        Revision created = ns1.getHeadRevision();

        NodeDocument doc = store.find(NODES, Utils.getIdFromPath("/test"));
        Set<Revision> collisions = Sets.newHashSet();
        Revision newest = doc.getNewestRevision(ns1, ns1.getHeadRevision(),
                ns1.newRevision(), null, collisions);
        assertEquals(created, newest);
        assertEquals(0, collisions.size());

        // from ns2 POV newest must be null, because the node is not yet visible
        newest = doc.getNewestRevision(ns2, ns2.getHeadRevision(),
                ns2.newRevision(), null, collisions);
        assertNull(newest);
        assertEquals(1, collisions.size());
        assertEquals(created, collisions.iterator().next());

        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();
        collisions.clear();

        // now ns2 sees /test
        doc = store.find(NODES, Utils.getIdFromPath("/test"));
        newest = doc.getNewestRevision(ns2, ns2.getHeadRevision(),
                ns2.newRevision(), null, collisions);
        assertEquals(created, newest);
        assertEquals(0, collisions.size());

        Revision uncommitted = ns1.newRevision();
        UpdateOp op = new UpdateOp(Utils.getIdFromPath("/test"), false);
        NodeDocument.setCommitRoot(op, uncommitted, 0);
        op.setMapEntry("p", uncommitted, "v");
        assertNotNull(store.findAndUpdate(NODES, op));

        collisions.clear();
        // ns1 must report uncommitted in collisions
        doc = store.find(NODES, Utils.getIdFromPath("/test"));
        newest = doc.getNewestRevision(ns1, ns1.getHeadRevision(),
                ns1.newRevision(), null, collisions);
        assertEquals(created, newest);
        assertEquals(1, collisions.size());
        assertEquals(uncommitted, collisions.iterator().next());

        collisions.clear();
        // ns2 must report uncommitted in collisions
        newest = doc.getNewestRevision(ns2, ns2.getHeadRevision(),
                ns2.newRevision(), null, collisions);
        assertEquals(created, newest);
        assertEquals(1, collisions.size());
        assertEquals(uncommitted, collisions.iterator().next());

        b1 = ns1.getRoot().builder();
        b1.child("test").setProperty("q", "v");
        merge(ns1, b1);
        Revision committed = ns1.getHeadRevision();

        collisions.clear();
        // ns1 must now report committed revision as newest
        // uncommitted is not considered a collision anymore
        // because it is older than the base revision
        doc = store.find(NODES, Utils.getIdFromPath("/test"));
        newest = doc.getNewestRevision(ns1, created,
                ns1.newRevision(), null, collisions);
        assertEquals(committed, newest);
        assertEquals(0, collisions.size());

        // ns2 must report committed revision as collision because
        // it is not yet visible. newest is when the node was created
        newest = doc.getNewestRevision(ns2, ns2.getHeadRevision(),
                ns2.newRevision(), null, collisions);
        assertEquals(created, newest);
        assertEquals(2, collisions.size());
        assertTrue(collisions.contains(uncommitted));
        assertTrue(collisions.contains(committed));


        ns1.dispose();
        ns2.dispose();
    }

    @Test
    public void getNewestRevisionCheckArgument() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns = createTestStore(store, 0);

        NodeBuilder builder = ns.getRoot().builder();
        builder.child("test");
        merge(ns, builder);

        Set<Revision> collisions = Sets.newHashSet();
        NodeDocument doc = store.find(NODES, Utils.getIdFromPath("/test"));
        Revision branchBase = ns.getHeadRevision().asBranchRevision();
        try {
            doc.getNewestRevision(ns, branchBase, ns.newRevision(), null, collisions);
            fail("Must fail with IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            Revision head = ns.getHeadRevision();
            Branch b = ns.getBranches().create(head, ns.newRevision(), null);
            doc.getNewestRevision(ns, head, ns.newRevision(), b, collisions);
            fail("Must fail with IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }

        ns.dispose();
    }

    @Test
    public void getChanges() throws Exception {
        final int numChanges = 200;
        Random random = new Random();
        DocumentNodeStore ns = createTestStore(numChanges);
        DocumentStore store = ns.getDocumentStore();
        NodeDocument doc = getRootDocument(store);
        for (int i = 0; i < 10; i++) {
            int idx = random.nextInt(numChanges);
            Revision r = Iterables.get(doc.getValueMap("p").keySet(), idx);
            Iterable<Revision> revs = doc.getChanges("p", r, ns);
            assertEquals(idx, Iterables.size(revs));
        }
        ns.dispose();
    }

    @Test
    public void getChangesMixedClusterIds() throws Exception {
        final int numChanges = 200;
        Random random = new Random();
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns1 = createTestStore(store, 0);
        DocumentNodeStore ns2 = createTestStore(store, 0);
        List<DocumentNodeStore> nodeStores = Lists.newArrayList(ns1, ns2);

        for (int i = 0; i < numChanges; i++) {
            DocumentNodeStore ns = nodeStores.get(random.nextInt(nodeStores.size()));
            ns.runBackgroundOperations();
            NodeBuilder builder = ns.getRoot().builder();
            builder.setProperty("p", i);
            merge(ns, builder);
            ns.runBackgroundOperations();
            if (random.nextDouble() < 0.2) {
                Revision head = ns.getHeadRevision();
                for (UpdateOp op : SplitOperations.forDocument(
                        getRootDocument(store), ns, head, 2)) {
                    store.createOrUpdate(NODES, op);
                }
            }
        }

        NodeDocument doc = getRootDocument(store);
        for (int i = 0; i < 10; i++) {
            int idx = random.nextInt(numChanges);
            Revision r = Iterables.get(doc.getValueMap("p").keySet(), idx);
            Iterable<Revision> revs1 = doc.getChanges("p", r, ns1);
            Iterable<Revision> revs2 = doc.getChanges("p", r, ns2);
            assertEquals(Iterables.size(revs1), Iterables.size(revs2));
            assertEquals(idx, Iterables.size(revs1));
        }

        ns1.dispose();
        ns2.dispose();
    }

    // OAK-3557
    @Test
    public void isConflicting() throws Exception {
        final int numChanges = 200;
        final Set<String> prevDocCalls = Sets.newHashSet();
        MemoryDocumentStore store = new MemoryDocumentStore() {
            @Override
            public <T extends Document> T find(Collection<T> collection,
                                               String key) {
                if (Utils.getPathFromId(key).startsWith("p")) {
                    prevDocCalls.add(key);
                }
                return super.find(collection, key);
            }
        };
        DocumentNodeStore ns = createTestStore(store, numChanges);
        NodeDocument doc = getRootDocument(store);
        Map<Revision, String> valueMap = doc.getValueMap("p");
        assertEquals(200, valueMap.size());
        Revision baseRev = valueMap.keySet().iterator().next();
        Revision commitRev = ns.newRevision();
        UpdateOp op = new UpdateOp(Utils.getIdFromPath("/"), false);
        op.setMapEntry("p", commitRev, "v");

        prevDocCalls.clear();
        assertFalse(doc.isConflicting(op, baseRev, commitRev, ns, false));
        assertTrue("too many calls for previous documents: " + prevDocCalls,
                prevDocCalls.size() <= 6);
        ns.dispose();
    }

    private DocumentNodeStore createTestStore(int numChanges) throws Exception {
        return createTestStore(new MemoryDocumentStore(), numChanges);
    }

    private DocumentNodeStore createTestStore(DocumentStore store,
                                              int numChanges) throws Exception {
        DocumentNodeStore ns = new DocumentMK.Builder()
                .setDocumentStore(store)
                .setAsyncDelay(0).getNodeStore();
        for (int i = 0; i < numChanges; i++) {
            NodeBuilder builder = ns.getRoot().builder();
            builder.setProperty("p", i);
            merge(ns, builder);
            if (Math.random() < 0.2) {
                Revision head = ns.getHeadRevision();
                for (UpdateOp op : SplitOperations.forDocument(
                        getRootDocument(store), ns, head, 2)) {
                    store.createOrUpdate(NODES, op);
                }
            }
        }
        return ns;
    }

    private void merge(NodeStore store, NodeBuilder builder)
            throws CommitFailedException {
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }

    private static NodeDocument getRootDocument(DocumentStore store) {
        String rootId = Utils.getIdFromPath("/");
        NodeDocument root = store.find(Collection.NODES, rootId);
        if (root == null) {
            throw new IllegalStateException("missing root document");
        }
        return root;
    }
}
