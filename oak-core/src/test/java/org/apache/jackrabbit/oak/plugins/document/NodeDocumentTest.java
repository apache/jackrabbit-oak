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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.document.VersionGarbageCollector.VersionGCStats;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

import static com.google.common.collect.Maps.newLinkedHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Collections.singletonList;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.COLLISIONS;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.NO_BINARY;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getRootDocument;
import static org.hamcrest.CoreMatchers.everyItem;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
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
            NodeDocument.addCollision(op, r, Revision.newRevision(1));
        }
        UpdateUtils.applyChanges(doc, op);
        RevisionVector head = DummyRevisionContext.INSTANCE.getHeadRevision();
        doc.split(DummyRevisionContext.INSTANCE, head, NO_BINARY);
    }

    @Test
    public void getConflictsFor() {
        MemoryDocumentStore docStore = new MemoryDocumentStore();
        String id = Utils.getPathFromId("/");
        NodeDocument doc = new NodeDocument(docStore);
        doc.put(Document.ID, id);

        Iterable<Revision> branchCommits = Collections.emptyList();
        Set<Revision> conflicts = doc.getConflictsFor(branchCommits);
        assertTrue(conflicts.isEmpty());

        // add some collisions
        UpdateOp op = new UpdateOp(id, false);
        Revision r0 = Revision.newRevision(1);
        Revision r1 = Revision.newRevision(1);
        Revision c1 = Revision.newRevision(1);
        Revision r2 = Revision.newRevision(1);
        Revision c2 = Revision.newRevision(1);
        // backward compatibility test
        op.setMapEntry(COLLISIONS, r0, String.valueOf(true));
        // regular collision entries
        NodeDocument.addCollision(op, r1, c1);
        NodeDocument.addCollision(op, r2, c2);
        UpdateUtils.applyChanges(doc, op);

        branchCommits = Collections.singleton(r0);
        conflicts = doc.getConflictsFor(branchCommits);
        assertTrue(conflicts.isEmpty());

        branchCommits = Collections.singleton(r1.asBranchRevision());
        conflicts = doc.getConflictsFor(branchCommits);
        assertEquals(newHashSet(c1), conflicts);

        branchCommits = Collections.singleton(r2.asBranchRevision());
        conflicts = doc.getConflictsFor(branchCommits);
        assertEquals(newHashSet(c2), conflicts);

        branchCommits = Lists.newArrayList(r1.asBranchRevision(), r2.asBranchRevision());
        conflicts = doc.getConflictsFor(branchCommits);
        assertEquals(newHashSet(c1, c2), conflicts);

        branchCommits = Lists.newArrayList(r2.asBranchRevision(), r1.asBranchRevision());
        conflicts = doc.getConflictsFor(branchCommits);
        assertEquals(newHashSet(c1, c2), conflicts);
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
        int numDeleted = new SplitDocumentCleanUp(ns.getDocumentStore(), new VersionGCStats(),
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
        int numDeleted = new SplitDocumentCleanUp(ns.getDocumentStore(), new VersionGCStats(),
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
                    .setAsyncDelay(0).setClusterId(i + 1).getNodeStore();
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
                RevisionVector head = ns.getHeadRevision();
                for (UpdateOp op : SplitOperations.forDocument(
                        getRootDocument(store), ns, head,
                        NO_BINARY, 2)) {
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
        int numDeleted = new SplitDocumentCleanUp(ns.getDocumentStore(), new VersionGCStats(),
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
        int numDeleted = new SplitDocumentCleanUp(ns.getDocumentStore(), new VersionGCStats(),
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
        final Set<String> prevDocCalls = newHashSet();
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
                RevisionVector head = ns.getHeadRevision();
                NodeDocument doc = ns.getDocumentStore().find(
                        NODES, Utils.getIdFromPath("/test"));
                for (UpdateOp op : SplitOperations.forDocument(
                        doc, ns, head, NO_BINARY, 2)) {
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
        doc.getNewestRevision(ns, new RevisionVector(baseRev), changeRev,
                null, new HashSet<Revision>());
        // must not read all previous docs
        assertTrue("too many calls for previous documents: " + prevDocCalls,
                prevDocCalls.size() <= 5);

        ns.dispose();
    }

    @Test
    public void getNewestRevision() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns1 = createTestStore(store, 1, 0);
        DocumentNodeStore ns2 = createTestStore(store, 2, 0);

        NodeBuilder b1 = ns1.getRoot().builder();
        b1.child("test");
        merge(ns1, b1);
        RevisionVector headCreated = ns1.getHeadRevision();
        Revision created = headCreated.getRevision(ns1.getClusterId());

        NodeDocument doc = store.find(NODES, Utils.getIdFromPath("/test"));
        Set<Revision> collisions = newHashSet();
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
        Revision committed = ns1.getHeadRevision().getRevision(ns1.getClusterId());

        collisions.clear();
        // ns1 must now report committed revision as newest
        // uncommitted is not considered a collision anymore
        // because it is older than the base revision
        doc = store.find(NODES, Utils.getIdFromPath("/test"));
        newest = doc.getNewestRevision(ns1, headCreated,
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
        DocumentNodeStore ns = createTestStore(store, 0, 0);

        NodeBuilder builder = ns.getRoot().builder();
        builder.child("test");
        merge(ns, builder);

        Set<Revision> collisions = newHashSet();
        NodeDocument doc = store.find(NODES, Utils.getIdFromPath("/test"));
        RevisionVector branchBase = ns.getHeadRevision().asBranchRevision(ns.getClusterId());
        try {
            doc.getNewestRevision(ns, branchBase, ns.newRevision(), null, collisions);
            fail("Must fail with IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            // expected
        }
        try {
            RevisionVector head = ns.getHeadRevision();
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
            Iterable<Revision> revs = doc.getChanges("p", new RevisionVector(r));
            assertEquals(idx, Iterables.size(revs));
        }
        ns.dispose();
    }

    @Test
    public void getChangesMixedClusterIds() throws Exception {
        final int numChanges = 200;
        Random random = new Random();
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns1 = createTestStore(store, 1, 0);
        DocumentNodeStore ns2 = createTestStore(store, 2, 0);
        List<DocumentNodeStore> nodeStores = Lists.newArrayList(ns1, ns2);

        List<RevisionVector> headRevisions = Lists.reverse(
                createTestData(nodeStores, random, numChanges));
        NodeDocument doc = getRootDocument(store);
        for (int i = 0; i < 10; i++) {
            int idx = random.nextInt(numChanges);
            RevisionVector r = headRevisions.get(idx);
            Iterable<Revision> revs1 = doc.getChanges("p", r);
            Iterable<Revision> revs2 = doc.getChanges("p", r);
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
        final Set<String> prevDocCalls = newHashSet();
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
        DocumentNodeStore ns = createTestStore(store, 0, numChanges);
        NodeDocument doc = getRootDocument(store);
        Map<Revision, String> valueMap = doc.getValueMap("p");
        assertEquals(200, valueMap.size());
        RevisionVector baseRev = new RevisionVector(valueMap.keySet().iterator().next());
        Revision commitRev = ns.newRevision();
        UpdateOp op = new UpdateOp(Utils.getIdFromPath("/"), false);
        op.setMapEntry("p", commitRev, "v");

        prevDocCalls.clear();
        assertFalse(doc.isConflicting(op, baseRev, commitRev, false));
        assertTrue("too many calls for previous documents: " + prevDocCalls,
                prevDocCalls.size() <= 6);
        ns.dispose();
    }

    // OAK-4358
    @Test
    public void tooManyReadsOnGetNewestRevision() throws Exception {
        final Set<String> prevDocCalls = newHashSet();
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
        DocumentNodeStore ns1 = createTestStore(store, 1, 0);
        DocumentNodeStore ns2 = createTestStore(store, 2, 0);

        // create a test node on ns1
        NodeBuilder b1 = ns1.getRoot().builder();
        b1.child("test");
        merge(ns1, b1);
        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();

        // modify node on ns2 a couple of time and
        // split off some changes
        for (int i = 0; i < 3; i++) {
            NodeBuilder b2 = ns2.getRoot().builder();
            b2.child("test").setProperty("ns2", i);
            merge(ns2, b2);
        }
        String testId = Utils.getIdFromPath("/test");
        NodeDocument test = ns2.getDocumentStore().find(NODES, testId);
        assertNotNull(test);
        List<UpdateOp> ops = SplitOperations.forDocument(test, ns2,
                ns2.getHeadRevision(), NO_BINARY, 2);
        assertEquals(2, ops.size());
        for (UpdateOp op : ops) {
            ns2.getDocumentStore().createOrUpdate(NODES, op);
        }
        ns2.runBackgroundOperations();
        ns1.runBackgroundOperations();

        List<RevisionVector> headRevs = Lists.newArrayList();
        // perform many changes on ns1 and split
        for (int i = 0; i < 20; i++) {
            b1 = ns1.getRoot().builder();
            b1.child("test").setProperty("ns1", i);
            merge(ns1, b1);
            test = ns1.getDocumentStore().find(NODES, testId);
            for (UpdateOp op : SplitOperations.forDocument(test, ns1,
                    ns1.getHeadRevision(), NO_BINARY, 3)) {
                ns1.getDocumentStore().createOrUpdate(NODES, op);
            }
            headRevs.add(ns1.getHeadRevision());
        }

        int numPrevDocs = Iterators.size(test.getPreviousDocLeaves());
        assertEquals(10, numPrevDocs);

        // getNewestRevision must not read all previous documents
        prevDocCalls.clear();
        // simulate a call done by a commit with a
        // base revision somewhat in the past
        test.getNewestRevision(ns1, headRevs.get(16), ns1.newRevision(),
                null, new HashSet<Revision>());
        // must only read one previous document for ns1 changes
        assertEquals(1, prevDocCalls.size());
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void sealedNodeDocumentFromJSON() throws Exception{
        DocumentNodeStore ns = createTestStore(1);
        NodeDocument root = getRootDocument(ns.getDocumentStore());
        String json = root.asString();
        NodeDocument doc2 = NodeDocument.fromString(ns.getDocumentStore(), json);
        doc2.put("foo", "bar");
        ns.dispose();
    }

    @Test
    public void tooManyReadsOnGetNodeAtRevision() throws Exception {
        final int numChanges = 200;
        final Set<String> prevDocCalls = newHashSet();
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
        DocumentNodeStore ns = createTestStore(store, 0, numChanges);
        NodeDocument doc = getRootDocument(store);
        Map<Revision, String> valueMap = doc.getValueMap("p");
        assertEquals(200, valueMap.size());
        Revision oldest = Iterables.getLast(valueMap.keySet());

        prevDocCalls.clear();
        DocumentNodeState state = doc.getNodeAtRevision(ns,
                new RevisionVector(oldest), null);
        assertNotNull(state);
        PropertyState prop = state.getProperty("p");
        assertNotNull(prop);
        assertEquals(0L, (long) prop.getValue(Type.LONG));

        assertTrue("too many calls for previous documents: " + prevDocCalls,
                prevDocCalls.size() <= 2);

        ns.dispose();
    }

    // OAK-5207
    @Test
    public void tooManyReadsOnGetVisibleChanges() throws Exception {
        final int numChanges = 500;
        final Set<String> prevDocCalls = newHashSet();
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
        Random random = new Random(42);
        DocumentNodeStore ns1 = createTestStore(store, 1, 0);
        DocumentNodeStore ns2 = createTestStore(store, 2, 0);
        List<DocumentNodeStore> nodeStores = Lists.newArrayList(ns1, ns2);
        List<RevisionVector> headRevisions = Lists.reverse(
                createTestData(nodeStores, random, numChanges));

        NodeDocument doc = getRootDocument(store);

        for (int i = 0; i < 20; i++) {
            prevDocCalls.clear();
            String value = doc.getVisibleChanges("p", headRevisions.get(i)).iterator().next().getValue();
            assertEquals(String.valueOf(numChanges - (i + 1)), value);
            assertTrue("too many calls for previous documents: " + prevDocCalls,
                    prevDocCalls.size() <= 3);
        }

        ns1.dispose();
        ns2.dispose();
    }

    @Test
    public void tooManyReadsOnGetVisibleChangesWithLongRunningBranchCommit() throws Exception {
        int numChanges = 843;
        final Map<String, Document> prevDocCalls = newLinkedHashMap();
        MemoryDocumentStore store = new MemoryDocumentStore() {
            @Override
            public <T extends Document> T find(Collection<T> collection,
                                               String key) {
                T doc = super.find(collection, key);
                if (Utils.getPathFromId(key).startsWith("p")) {
                    prevDocCalls.put(key, doc);
                }
                return doc;
            }
        };
        Random random = new Random(42);
        DocumentNodeStore ns1 = createTestStore(store, 1, 0);
        DocumentNodeStore ns2 = createTestStore(store, 2, 0);
        List<DocumentNodeStore> nodeStores = Lists.newArrayList(ns1, ns2);
        List<RevisionVector> headRevisions = Lists.reverse(
                createTestData(nodeStores, random, numChanges));

        NodeBuilder builder = ns1.getRoot().builder();
        builder.setProperty("q", 1);
        int numNodes = 0;
        while (getRootDocument(store).getValueMap("q").isEmpty()) {
            // write some other changes until a branch is created
            NodeBuilder child = builder.child("n-" + numNodes++);
            for (int i = 0; i < 50; i++) {
                child.setProperty("p-" + i, i);
            }
        }
        // do not yet merge, but create more test data
        int numMoreChanges = 50;
        List<RevisionVector> moreRevs = Lists.reverse(
                createTestData(nodeStores, random, numMoreChanges, numChanges));
        headRevisions = Lists.newArrayList(Iterables.concat(moreRevs, headRevisions));
        numChanges += numMoreChanges;

        // now merge the branch and update 'q'. this will split
        // the old value to a previous document
        merge(ns1, builder);
        builder = ns1.getRoot().builder();
        builder.setProperty("q", 2);
        merge(ns1, builder);

        // and create yet more test data
        numMoreChanges = 50;
        moreRevs = Lists.reverse(
                createTestData(nodeStores, random, numMoreChanges, numChanges));
        headRevisions = Lists.newArrayList(Iterables.concat(moreRevs, headRevisions));
        numChanges += numMoreChanges;

        NodeDocument doc = getRootDocument(store);

        for (int i = 0; i < 20; i++) {
            prevDocCalls.clear();
            String value = doc.getVisibleChanges("p", headRevisions.get(i)).iterator().next().getValue();
            assertEquals(String.valueOf(numChanges - (i + 1)), value);
            assertTrue("too many calls for previous documents ("
                            + prevDocCalls.size() + "): " + prevDocCalls,
                    prevDocCalls.size() <= 8);
        }

        ns1.dispose();
        ns2.dispose();
    }

    @Test
    public void readsWithOverlappingPreviousDocuments() throws Exception {
        final Map<String, Document> prevDocCalls = newLinkedHashMap();
        MemoryDocumentStore store = new MemoryDocumentStore() {
            @Override
            public <T extends Document> T find(Collection<T> collection,
                                               String key) {
                T doc = super.find(collection, key);
                if (collection == NODES && Utils.getPathFromId(key).startsWith("p")) {
                    prevDocCalls.put(key, doc);
                }
                return doc;
            }
        };
        Random random = new Random(42);
        DocumentNodeStore ns = createTestStore(store, 1, 0);
        List<RevisionVector> headRevisions = Lists.newArrayList();

        for (int i = 0; i < 1000; i++) {
            NodeBuilder builder = ns.getRoot().builder();
            NodeBuilder test = builder.child("test");
            test.setProperty("p", i);
            // maintain four other properties and update
            // them with different probabilities
            for (int j = 0; j < 4; j++) {
                if (random.nextInt(j + 1) == 0) {
                    test.setProperty("p" + j, i);
                }
            }
            merge(ns, builder);
            headRevisions.add(ns.getHeadRevision());
            if (i % 3 == 0) {
                // split the document
                RevisionVector head = ns.getHeadRevision();
                NodeDocument doc = store.find(NODES, Utils.getIdFromPath("/test"));
                for (UpdateOp op : SplitOperations.forDocument(
                        doc, ns, head, NO_BINARY, 2)) {
                    store.createOrUpdate(NODES, op);
                }
            }
        }
        ns.runBackgroundOperations();

        NodeDocument doc = store.find(NODES, Utils.getIdFromPath("/test"));
        List<Integer> numCalls = Lists.newArrayList();
        long count = 0;
        // go back in time and check number of find calls
        for (RevisionVector rv : headRevisions) {
            prevDocCalls.clear();
            DocumentNodeState s = doc.getNodeAtRevision(ns, rv, null);
            assertNotNull(s);
            assertEquals(count++, s.getProperty("p").getValue(Type.LONG).longValue());
            numCalls.add(prevDocCalls.size());
        }
        assertThat(numCalls.toString(), numCalls, everyItem(lessThan(36)));

        ns.dispose();
    }

    @Test
    public void getVisibleChanges() throws Exception {
        final int numChanges = 200;
        Random random = new Random();
        DocumentNodeStore ns = createTestStore(numChanges);
        DocumentStore store = ns.getDocumentStore();
        NodeDocument doc = getRootDocument(store);
        for (int i = 0; i < 10; i++) {
            int idx = random.nextInt(numChanges);
            Revision r = Iterables.get(doc.getValueMap("p").keySet(), idx);
            Iterable<Map.Entry<Revision, String>> revs = doc.getVisibleChanges("p", new RevisionVector(r));
            assertEquals(idx, numChanges - Iterables.size(revs));
        }
        ns.dispose();
    }

    @Test
    public void getVisibleChangesMixedClusterIds() throws Exception {
        final int numChanges = 200;
        Random random = new Random();
        MemoryDocumentStore store = new MemoryDocumentStore();
        DocumentNodeStore ns1 = createTestStore(store, 1, 0);
        DocumentNodeStore ns2 = createTestStore(store, 2, 0);
        List<DocumentNodeStore> nodeStores = Lists.newArrayList(ns1, ns2);

        List<RevisionVector> headRevisions = Lists.reverse(
                createTestData(nodeStores, random, numChanges));
        NodeDocument doc = getRootDocument(store);
        for (int i = 0; i < 10; i++) {
            int idx = random.nextInt(numChanges);
            RevisionVector r = headRevisions.get(idx);
            Iterable<Map.Entry<Revision, String>> revs1 = doc.getVisibleChanges("p", r);
            Iterable<Map.Entry<Revision, String>> revs2 = doc.getVisibleChanges("p", r);
            assertEquals(Iterables.size(revs1), Iterables.size(revs2));
            assertEquals(idx, numChanges - Iterables.size(revs1));
        }

        ns1.dispose();
        ns2.dispose();
    }

    @Test
    public void getSweepRevisions() throws Exception {
        MemoryDocumentStore store = new MemoryDocumentStore();
        NodeDocument doc = new NodeDocument(store);
        RevisionVector sweepRev = doc.getSweepRevisions();
        assertNotNull(sweepRev);
        assertEquals(0, sweepRev.getDimensions());

        Revision r1 = Revision.newRevision(1);
        Revision r2 = Revision.newRevision(2);

        UpdateOp op = new UpdateOp("id", true);
        NodeDocument.setSweepRevision(op, r1);
        UpdateUtils.applyChanges(doc, op);

        sweepRev = doc.getSweepRevisions();
        assertNotNull(sweepRev);
        assertEquals(1, sweepRev.getDimensions());
        assertEquals(new RevisionVector(r1), sweepRev);

        op = new UpdateOp("id", false);
        NodeDocument.setSweepRevision(op, r2);
        UpdateUtils.applyChanges(doc, op);

        sweepRev = doc.getSweepRevisions();
        assertNotNull(sweepRev);
        assertEquals(2, sweepRev.getDimensions());
        assertEquals(new RevisionVector(r1, r2), sweepRev);
    }

    @Test
    public void noPreviousDocAccessAfterSweep() throws Exception {
        final Set<String> findCalls = newHashSet();
        DocumentStore ds = new MemoryDocumentStore() {
            @Override
            public <T extends Document> T find(Collection<T> collection,
                                               String key) {
                findCalls.add(key);
                return super.find(collection, key);
            }
        };
        DocumentNodeStore ns = createTestStore(ds, 0, 0, 0);
        // create test nodes with the root document as commit root
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        builder.child("bar");
        merge(ns, builder);
        // now add many changes to the root document, which will
        // move the commit information to a previous document
        createTestData(singletonList(ns), new Random(), 200);
        ns.runBackgroundUpdateOperations();

        NodeDocument doc = ds.find(NODES, getIdFromPath("/foo"));
        assertNotNull(doc);
        findCalls.clear();
        doc.getNodeAtRevision(ns, ns.getHeadRevision(), null);
        // with an old sweep revision, there will be find calls
        // to look up the commit root document
        assertTrue(findCalls.size() > 0);

        // run sweeper
        ns.runBackgroundSweepOperation();

        // now number of find calls must be zero
        doc = ds.find(NODES, getIdFromPath("/foo"));
        assertNotNull(doc);
        findCalls.clear();
        doc.getNodeAtRevision(ns, ns.getHeadRevision(), null);
        assertEquals(0, findCalls.size());

        ns.dispose();
    }

    private DocumentNodeStore createTestStore(int numChanges) throws Exception {
        return createTestStore(new MemoryDocumentStore(), 0, numChanges);
    }

    private DocumentNodeStore createTestStore(DocumentStore store,
                                              int clusterId,
                                              int numChanges,
                                              int commitValueCacheSize)
            throws Exception {
        DocumentNodeStore ns = new DocumentMK.Builder()
                .setDocumentStore(store).setCommitValueCacheSize(commitValueCacheSize)
                .setAsyncDelay(0).setClusterId(clusterId).getNodeStore();
        for (int i = 0; i < numChanges; i++) {
            NodeBuilder builder = ns.getRoot().builder();
            builder.setProperty("p", i);
            merge(ns, builder);
            if (Math.random() < 0.2) {
                RevisionVector head = ns.getHeadRevision();
                for (UpdateOp op : SplitOperations.forDocument(
                        getRootDocument(store), ns, head,
                        NO_BINARY, 2)) {
                    store.createOrUpdate(NODES, op);
                }
            }
        }
        return ns;
    }

    private DocumentNodeStore createTestStore(DocumentStore store,
                                              int clusterId,
                                              int numChanges) throws Exception {
        return createTestStore(store, clusterId, numChanges, 10000);
    }

    private List<RevisionVector> createTestData(List<DocumentNodeStore> nodeStores,
                                                Random random,
                                                int numChanges)
            throws CommitFailedException {
        return createTestData(nodeStores, random, numChanges, 0);
    }

    private List<RevisionVector> createTestData(List<DocumentNodeStore> nodeStores,
                                                Random random,
                                                int numChanges,
                                                int startValue)
            throws CommitFailedException {
        List<RevisionVector> headRevisions = Lists.newArrayList();
        for (int i = startValue; i < numChanges + startValue; i++) {
            DocumentNodeStore ns = nodeStores.get(random.nextInt(nodeStores.size()));
            ns.runBackgroundUpdateOperations();
            ns.runBackgroundReadOperations();
            NodeBuilder builder = ns.getRoot().builder();
            builder.setProperty("p", i);
            merge(ns, builder);
            headRevisions.add(ns.getHeadRevision());
            ns.runBackgroundUpdateOperations();
            ns.runBackgroundReadOperations();
            if (random.nextDouble() < 0.2) {
                DocumentStore store = ns.getDocumentStore();
                RevisionVector head = ns.getHeadRevision();
                for (UpdateOp op : SplitOperations.forDocument(
                        getRootDocument(store), ns, head,
                        NO_BINARY, 2)) {
                    store.createOrUpdate(NODES, op);
                }
            }
        }
        return headRevisions;
    }

    private void merge(NodeStore store, NodeBuilder builder)
            throws CommitFailedException {
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }
}
