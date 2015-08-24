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
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
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
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.revisionAreAmbiguous;
import static org.apache.jackrabbit.oak.plugins.document.Revision.RevisionComparator;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getRootDocument;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
        UpdateUtils.applyChanges(doc, op, StableRevisionComparator.INSTANCE);
        Revision head = DummyRevisionContext.INSTANCE.getHeadRevision();
        doc.split(DummyRevisionContext.INSTANCE, head);
    }

    @Test
    public void ambiguousRevisions() {
        // revisions from same cluster node are not ambiguous
        RevisionContext context = DummyRevisionContext.INSTANCE;
        Revision r1 = new Revision(1, 0, 1);
        Revision r2 = new Revision(2, 0, 1);
        assertFalse(revisionAreAmbiguous(context, r1, r1));
        assertFalse(revisionAreAmbiguous(context, r1, r2));
        assertFalse(revisionAreAmbiguous(context, r2, r1));

        // revisions from different cluster nodes are not ambiguous
        // if seen with stable revision comparator
        r1 = new Revision(1, 0, 2);
        r2 = new Revision(2, 0, 1);
        assertFalse(revisionAreAmbiguous(context, r1, r1));
        assertFalse(revisionAreAmbiguous(context, r1, r2));
        assertFalse(revisionAreAmbiguous(context, r2, r1));

        // now use a revision comparator with seen-at support
        final RevisionComparator comparator = new RevisionComparator(1);
        context = new DummyRevisionContext() {
            @Override
            public Comparator<Revision> getRevisionComparator() {
                return comparator;
            }
        };
        r1 = new Revision(1, 0, 2);
        r2 = new Revision(2, 0, 1);
        // add revision to comparator in reverse time order
        comparator.add(r2, new Revision(2, 0, 0));
        comparator.add(r1, new Revision(3, 0, 0)); // r1 seen after r2
        assertFalse(revisionAreAmbiguous(context, r1, r1));
        assertFalse(revisionAreAmbiguous(context, r2, r2));
        assertTrue(revisionAreAmbiguous(context, r1, r2));
        assertTrue(revisionAreAmbiguous(context, r2, r1));
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
        Revision changeRev = new Revision(Iterables.getLast(changes).getTimestamp(), 1000, ns.getClusterId());
        // reset calls to previous documents
        prevDocCalls.clear();
        doc.getNewestRevision(ns, changeRev, new CollisionHandler() {
            @Override
            void concurrentModification(Revision other) {
                // ignore
            }
        });
        // must not read all previous docs
        assertTrue("too many calls for previous documents: " + prevDocCalls,
                prevDocCalls.size() <= 4);

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
}
