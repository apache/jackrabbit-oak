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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import javax.annotation.Nonnull;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import static com.google.common.collect.ImmutableList.copyOf;
import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.MongoBlobGCTest.randomStream;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.DOC_SIZE_THRESHOLD;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.NUM_REVS_THRESHOLD;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.PREV_SPLIT_FACTOR;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.NO_BINARY;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation.Type.REMOVE_MAP_ENTRY;
import static org.apache.jackrabbit.oak.plugins.document.UpdateOp.Operation.Type.SET_MAP_ENTRY;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.isCommitted;
import static org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState.binaryProperty;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Check correct splitting of documents (OAK-926 & OAK-1342).
 */
public class DocumentSplitTest extends BaseDocumentMKTest {

    @Test
    public void splitRevisions() throws Exception {
        DocumentStore store = mk.getDocumentStore();
        DocumentNodeStore ns = mk.getNodeStore();
        Set<Revision> revisions = Sets.newHashSet();
        NodeDocument doc = store.find(NODES, Utils.getIdFromPath("/"));
        assertNotNull(doc);
        revisions.addAll(doc.getLocalRevisions().keySet());
        revisions.add(Revision.fromString(mk.commit("/", "+\"foo\":{}+\"bar\":{}", null, null)));
        // create nodes
        while (revisions.size() <= NodeDocument.NUM_REVS_THRESHOLD) {
            revisions.add(Revision.fromString(mk.commit("/", "+\"foo/node-" + revisions.size() + "\":{}" +
                    "+\"bar/node-" + revisions.size() + "\":{}", null, null)));
        }
        mk.runBackgroundOperations();
        String head = mk.getHeadRevision();
        doc = store.find(NODES, Utils.getIdFromPath("/"));
        assertNotNull(doc);
        Map<Revision, String> revs = doc.getLocalRevisions();
        // one remaining in the local revisions map
        assertEquals(1, revs.size());
        for (Revision rev : revisions) {
            assertTrue(doc.containsRevision(rev));
            assertTrue(isCommitted(ns.getCommitValue(rev, doc)));
        }
        // check if document is still there
        assertNotNull(ns.getNode("/", RevisionVector.fromString(head)));

        NodeDocument prevDoc = Iterators.getOnlyElement(doc.getAllPreviousDocs());
        assertThat(prevDoc.getSplitDocType(), either(is(SplitDocType.DEFAULT)).or(is(SplitDocType.DEFAULT_NO_BRANCH)));

        mk.commit("/", "+\"baz\":{}", null, null);
        ns.setAsyncDelay(0);
        mk.backgroundWrite();
    }

    @Test
    public void splitDeleted() throws Exception {
        DocumentStore store = mk.getDocumentStore();
        DocumentNodeStore ns = mk.getNodeStore();
        Set<Revision> revisions = Sets.newHashSet();
        mk.commit("/", "+\"foo\":{}", null, null);
        NodeDocument doc = store.find(NODES, Utils.getIdFromPath("/foo"));
        assertNotNull(doc);
        revisions.addAll(doc.getLocalRevisions().keySet());
        boolean create = false;
        while (revisions.size() <= NodeDocument.NUM_REVS_THRESHOLD) {
            if (create) {
                revisions.add(Revision.fromString(mk.commit("/", "+\"foo\":{}", null, null)));
            } else {
                revisions.add(Revision.fromString(mk.commit("/", "-\"foo\"", null, null)));
            }
            create = !create;
        }
        mk.runBackgroundOperations();
        String head = mk.getHeadRevision();
        doc = store.find(NODES, Utils.getIdFromPath("/foo"));
        assertNotNull(doc);
        Map<Revision, String> deleted = doc.getLocalDeleted();
        // one remaining in the local deleted map
        assertEquals(1, deleted.size());
        for (Revision rev : revisions) {
            assertTrue("document should contain revision (or have revision in commit root path):" + rev, doc.containsRevision(rev)
                    || doc.getCommitRootPath(rev) != null);
            assertTrue(isCommitted(ns.getCommitValue(rev, doc)));
        }
        DocumentNodeState node = ns.getNode("/foo", RevisionVector.fromString(head));
        // check status of node
        if (create) {
            assertNull(node);
        } else {
            assertNotNull(node);
        }
    }

    @Test
    public void splitCommitRoot() throws Exception {
        DocumentStore store = mk.getDocumentStore();
        DocumentNodeStore ns = mk.getNodeStore();
        mk.commit("/", "+\"foo\":{}+\"bar\":{}", null, null);
        NodeDocument doc = store.find(NODES, Utils.getIdFromPath("/foo"));
        assertNotNull(doc);
        Set<Revision> commitRoots = Sets.newHashSet();
        commitRoots.addAll(doc.getLocalCommitRoot().keySet());
        // create nodes
        while (commitRoots.size() <= NodeDocument.NUM_REVS_THRESHOLD) {
            commitRoots.add(Revision.fromString(mk.commit("/", "^\"foo/prop\":" +
                    commitRoots.size() + "^\"bar/prop\":" + commitRoots.size(), null, null)));
        }
        mk.runBackgroundOperations();
        doc = store.find(NODES, Utils.getIdFromPath("/foo"));
        assertNotNull(doc);
        Map<Revision, String> commits = doc.getLocalCommitRoot();
        // two remaining in the local commit root map
        // the first _commitRoot entry for the _deleted when the node was created
        // the second _commitRoot entry for the most recent prop change
        assertEquals(2, commits.size());
        for (Revision rev : commitRoots) {
            assertTrue(isCommitted(ns.getCommitValue(rev, doc)));
        }
    }

    @Test
    public void splitPropertyRevisions() throws Exception {
        DocumentStore store = mk.getDocumentStore();
        DocumentNodeStore ns = mk.getNodeStore();
        mk.commit("/", "+\"foo\":{}", null, null);
        NodeDocument doc = store.find(NODES, Utils.getIdFromPath("/foo"));
        assertNotNull(doc);
        Set<Revision> revisions = Sets.newHashSet();
        // create nodes
        while (revisions.size() <= NodeDocument.NUM_REVS_THRESHOLD) {
            revisions.add(Revision.fromString(mk.commit("/", "^\"foo/prop\":" +
                    revisions.size(), null, null)));
        }
        mk.runBackgroundOperations();
        doc = store.find(NODES, Utils.getIdFromPath("/foo"));
        assertNotNull(doc);
        Map<Revision, String> localRevs = doc.getLocalRevisions();
        // one remaining in the local revisions map
        assertEquals(1, localRevs.size());
        for (Revision rev : revisions) {
            assertTrue(isCommitted(ns.getCommitValue(rev, doc)));
        }
        // all revisions in the prop map
        Map<Revision, String> valueMap = doc.getValueMap("prop");
        assertEquals((long) revisions.size(), valueMap.size());
        // one remaining revision in the local map
        valueMap = doc.getLocalMap("prop");
        assertEquals(1L, valueMap.size());
    }

    @Test
    public void cluster() {
        MemoryDocumentStore ds = new MemoryDocumentStore();
        MemoryBlobStore bs = new MemoryBlobStore();
        DocumentMK.Builder builder;

        builder = new DocumentMK.Builder();
        builder.setDocumentStore(ds).setBlobStore(bs).setAsyncDelay(0);
        DocumentMK mk1 = builder.setClusterId(1).open();

        mk1.commit("/", "+\"test\":{\"prop1\":0}", null, null);
        // make sure the new node is visible to other DocumentMK instances
        mk1.backgroundWrite();

        builder = new DocumentMK.Builder();
        builder.setDocumentStore(ds).setBlobStore(bs).setAsyncDelay(0);
        DocumentMK mk2 = builder.setClusterId(2).open();
        builder = new DocumentMK.Builder();
        builder.setDocumentStore(ds).setBlobStore(bs).setAsyncDelay(0);
        DocumentMK mk3 = builder.setClusterId(3).open();

        for (int i = 0; i < NodeDocument.NUM_REVS_THRESHOLD; i++) {
            mk1.commit("/", "^\"test/prop1\":" + i, null, null);
            mk2.commit("/", "^\"test/prop2\":" + i, null, null);
            mk3.commit("/", "^\"test/prop3\":" + i, null, null);
        }

        mk1.runBackgroundOperations();
        mk2.runBackgroundOperations();
        mk3.runBackgroundOperations();

        NodeDocument doc = ds.find(NODES, Utils.getIdFromPath("/test"));
        assertNotNull(doc);
        Map<Revision, String> revs = doc.getLocalRevisions();
        assertEquals(3, revs.size());
        revs = doc.getValueMap("_revisions");
        assertEquals(3 * NodeDocument.NUM_REVS_THRESHOLD, revs.size());
        Revision previous = null;
        for (Map.Entry<Revision, String> entry : revs.entrySet()) {
            if (previous != null) {
                assertTrue(previous.compareRevisionTimeThenClusterId(entry.getKey()) > 0);
            }
            previous = entry.getKey();
        }
        mk1.dispose();
        mk2.dispose();
        mk3.dispose();
    }

    @Test // OAK-1233
    public void manyRevisions() {
        final int numMKs = 3;
        MemoryDocumentStore ds = new MemoryDocumentStore();
        MemoryBlobStore bs = new MemoryBlobStore();

        List<Set<String>> changes = new ArrayList<Set<String>>();
        List<DocumentMK> mks = new ArrayList<DocumentMK>();
        for (int i = 1; i <= numMKs; i++) {
            DocumentMK.Builder builder = new DocumentMK.Builder();
            builder.setDocumentStore(ds).setBlobStore(bs).setAsyncDelay(0);
            DocumentMK mk = builder.setClusterId(i).open();
            mks.add(mk);
            changes.add(new HashSet<String>());
            if (i == 1) {
                mk.commit("/", "+\"test\":{}", null, null);
                mk.runBackgroundOperations();
            }
        }

        List<String> propNames = Arrays.asList("prop1", "prop2", "prop3");
        Random random = new Random(0);

        for (int i = 0; i < 1000; i++) {
            int mkIdx = random.nextInt(mks.size());
            // pick mk
            DocumentMK mk = mks.get(mkIdx);
            DocumentNodeStore ns = mk.getNodeStore();
            // pick property name to update
            String name = propNames.get(random.nextInt(propNames.size()));
            // need to sync?
            for (int j = 0; j < changes.size(); j++) {
                Set<String> c = changes.get(j);
                if (c.contains(name)) {
                    syncMKs(mks, j);
                    c.clear();
                    break;
                }
            }
            // read current value
            NodeDocument doc = ds.find(NODES, Utils.getIdFromPath("/test"));
            assertNotNull(doc);
            RevisionVector head = ns.getHeadRevision();
            Revision lastRev = ns.getPendingModifications().get("/test");
            DocumentNodeState n = doc.getNodeAtRevision(mk.getNodeStore(), head, lastRev);
            assertNotNull(n);
            String value = n.getPropertyAsString(name);
            // set or increment
            if (value == null) {
                value = String.valueOf(0);
            } else {
                value = String.valueOf(Integer.parseInt(value) + 1);
            }
            mk.commit("/test", "^\"" + name + "\":" + value, null, null);
            changes.get(mkIdx).add(name);
        }
        for (DocumentMK mk : mks) {
            mk.dispose();
        }
    }

    @Test
    public void commitRootInPrevious() {
        DocumentStore store = mk.getDocumentStore();
        DocumentNodeStore ns = mk.getNodeStore();
        mk.commit("/", "+\"test\":{\"node\":{}}", null, null);
        mk.commit("/test", "+\"foo\":{}+\"bar\":{}", null, null);
        mk.commit("/test", "^\"foo/prop\":0^\"bar/prop\":0", null, null);
        NodeDocument doc = store.find(NODES, Utils.getIdFromPath("/test/foo"));
        assertNotNull(doc);
        String rev = null;
        for (int i = 0; i < NodeDocument.NUM_REVS_THRESHOLD; i++) {
            rev = mk.commit("/test/foo", "^\"prop\":" + i, null, null);
        }
        ns.runBackgroundOperations();
        doc = store.find(NODES, Utils.getIdFromPath("/test/foo"));
        assertNotNull(doc);
        DocumentNodeState node = doc.getNodeAtRevision(ns,
                RevisionVector.fromString(rev), null);
        assertNotNull(node);
    }

    @Test
    public void testSplitDocNoChild() throws Exception{
        DocumentStore store = mk.getDocumentStore();
        DocumentNodeStore ns = mk.getNodeStore();
        mk.commit("/", "+\"test\":{\"node\":{}}", null, null);
        mk.commit("/test", "+\"foo\":{}+\"bar\":{}", null, null);
        for (int i = 0; i < NodeDocument.NUM_REVS_THRESHOLD; i++) {
            mk.commit("/test/foo", "^\"prop\":" + i, null, null);
        }
        ns.runBackgroundOperations();
        NodeDocument doc = store.find(NODES, Utils.getIdFromPath("/test/foo"));
        List<NodeDocument> prevDocs = ImmutableList.copyOf(doc.getAllPreviousDocs());
        assertEquals(1, prevDocs.size());
        assertEquals(SplitDocType.DEFAULT_LEAF, prevDocs.get(0).getSplitDocType());
    }

    @Test
    public void testSplitPropAndCommitOnly() throws Exception{
        DocumentStore store = mk.getDocumentStore();
        DocumentNodeStore ns = mk.getNodeStore();
        NodeBuilder b1 = ns.getRoot().builder();
        b1.child("test").child("foo").child("bar");
        ns.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //Commit on a node which has a child and where the commit root
        // is parent
        for (int i = 0; i < NodeDocument.NUM_REVS_THRESHOLD; i++) {
            b1 = ns.getRoot().builder();
            b1.child("test").child("foo").setProperty("prop",i);
            b1.child("test").setProperty("prop",i);
            ns.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }
        ns.runBackgroundOperations();
        NodeDocument doc = store.find(NODES, Utils.getIdFromPath("/test/foo"));
        List<NodeDocument> prevDocs = ImmutableList.copyOf(doc.getAllPreviousDocs());
        assertEquals(1, prevDocs.size());
        assertEquals(SplitDocType.COMMIT_ROOT_ONLY, prevDocs.get(0).getSplitDocType());
    }

    @Test
    public void splitDocWithHasBinary() throws Exception{
        DocumentStore store = mk.getDocumentStore();
        DocumentNodeStore ns = mk.getNodeStore();
        NodeBuilder b1 = ns.getRoot().builder();
        b1.child("test").child("foo").setProperty("binaryProp",ns.createBlob(randomStream(1, 4096)));
        ns.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        //Commit on a node which has a child and where the commit root
        // is parent
        for (int i = 0; i < NodeDocument.NUM_REVS_THRESHOLD; i++) {
            b1 = ns.getRoot().builder();
            b1.child("test").child("foo").setProperty("prop",i);
            ns.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }
        ns.runBackgroundOperations();
        NodeDocument doc = store.find(NODES, Utils.getIdFromPath("/test/foo"));
        List<NodeDocument> prevDocs = ImmutableList.copyOf(doc.getAllPreviousDocs());
        assertEquals(1, prevDocs.size());

        //Check for hasBinary
        assertTrue(doc.hasBinary());
        assertTrue(prevDocs.get(0).hasBinary());

    }

    @Test
    public void cascadingSplit() {
        cascadingSplit("/test/node");
    }

    @Test
    public void cascadingSplitLongPath() {
        String p = "/";
        while (!Utils.isLongPath(p)) {
            p = PathUtils.concat(p, "long-path-element");
        }
        cascadingSplit(p);
    }

    private void cascadingSplit(String path) {
        // use a store without sync delay
        mk.dispose();
        mk = new DocumentMK.Builder().setAsyncDelay(0).open();
        DocumentStore store = mk.getDocumentStore();
        DocumentNodeStore ns = mk.getNodeStore();

        String rev = null;
        String p = "/";
        for (String name : PathUtils.elements(path)) {
            rev = mk.commit(p, "+\"" + name + "\":{}", rev, null);
            p = PathUtils.concat(p, name);
        }

        List<String> revs = Lists.newArrayList();
        for (int i = 0; i < NodeDocument.PREV_SPLIT_FACTOR + 1; i++) {
            NodeDocument doc = store.find(NODES, Utils.getIdFromPath(path));
            assertNotNull(doc);
            assertEquals(i, doc.getPreviousRanges().size());
            for (int j = 0; j < NodeDocument.NUM_REVS_THRESHOLD; j++) {
                int value = (i * NodeDocument.NUM_REVS_THRESHOLD + j);
                rev = mk.commit(path, "^\"prop\":" + value, rev, null);
                revs.add(rev);
            }
            ns.runBackgroundOperations();
        }
        NodeDocument doc = store.find(NODES, Utils.getIdFromPath(path));
        assertNotNull(doc);
        assertEquals(2, doc.getPreviousRanges().size());

        List<NodeDocument> prevDocs = ImmutableList.copyOf(doc.getAllPreviousDocs());
        //1 intermediate and 11 previous doc
        assertEquals(1 + 11, prevDocs.size());
        assertTrue(Iterables.any(prevDocs, new Predicate<NodeDocument>() {
            @Override
            public boolean apply(NodeDocument input) {
                return input.getSplitDocType() == SplitDocType.INTERMEDIATE;
            }
        }));

        for (String s : revs) {
            Revision r = Revision.fromString(s);
            if (doc.getLocalRevisions().containsKey(r)) {
                continue;
            }
            Iterable<NodeDocument> prev = doc.getPreviousDocs("prop", r);
            assertEquals(1, Iterables.size(prev));
            for (NodeDocument d : prev) {
                assertTrue(d.containsRevision(r));
            }
        }

        int numPrev = 0;
        for (NodeDocument prev : doc.getPreviousDocs("prop", null)) {
            numPrev++;
            assertTrue(!prev.getValueMap("prop").isEmpty());
        }
        assertEquals(2, numPrev);

        Revision previous = null;
        int numValues = 0;
        Map<Revision, String> valueMap = doc.getValueMap("prop");
        for (Map.Entry<Revision, String> entry : valueMap.entrySet()) {
            if (previous != null) {
                assertTrue(previous.compareRevisionTime(entry.getKey()) > 0);
            }
            previous = entry.getKey();
            numValues++;
            assertEquals(entry.getValue(), valueMap.get(entry.getKey()));
        }
        assertEquals(revs.size(), numValues);
        assertEquals(revs.size(), valueMap.size());

        assertNotNull(doc.getNodeAtRevision(ns, RevisionVector.fromString(rev), null));
    }

    @Test
    public void mainPath() {
        Revision r = Revision.fromString("r1-0-1");
        for (String path : new String[]{"/", "/test", "/test/path"}) {
            DocumentStore store = mk.getDocumentStore();
            NodeDocument doc = new NodeDocument(store);
            String id = Utils.getPreviousIdFor(path, r, 0);
            doc.put(NodeDocument.ID, id);
            assertEquals(path, doc.getMainPath());
        }
    }

    // OAK-1692
    @Test
    public void cascadingWithSplitRatio() {
        String id = Utils.getIdFromPath("/test");
        mk.commit("/", "+\"test\":{}", null, null);
        DocumentStore store = mk.getDocumentStore();
        int clusterId = mk.getNodeStore().getClusterId();

        UpdateOp op = new UpdateOp(id, false);
        // create some baggage from another cluster node
        for (int i = 0; i < 4000; i++) {
            Revision r = Revision.newRevision(2);
            op.setMapEntry("prop", r, "some long test value with many characters");
            NodeDocument.setRevision(op, r, "c");
        }
        store.findAndUpdate(NODES, op);
        NodeDocument doc = store.find(NODES, id);
        assertNotNull(doc);
        assertTrue(doc.getMemory() > DOC_SIZE_THRESHOLD);

        // some fake previous doc references to trigger UpdateOp
        // for an intermediate document
        TreeSet<Revision> prev = Sets.newTreeSet(StableRevisionComparator.INSTANCE);
        for (int i = 0; i < PREV_SPLIT_FACTOR; i++) {
            Revision low = Revision.newRevision(clusterId);
            Revision high = Revision.newRevision(clusterId);
            prev.add(high);
            NodeDocument.setPrevious(op, new Range(high, low, 0));
        }
        store.findAndUpdate(NODES, op);

        doc = store.find(NODES, id);
        assertNotNull(doc);
        List<UpdateOp> splitOps = Lists.newArrayList(doc.split(
                mk.getNodeStore(), mk.getNodeStore().getHeadRevision(),
                NO_BINARY));
        assertEquals(2, splitOps.size());
        // first update op is for the new intermediate doc
        op = splitOps.get(0);
        String newPrevId = Utils.getPreviousIdFor("/test", prev.last(), 1);
        assertEquals(newPrevId, op.getId());
        // second update op is for the main document
        op = splitOps.get(1);
        assertEquals(id, op.getId());
        for (Map.Entry<Key, Operation> entry : op.getChanges().entrySet()) {
            Revision r = entry.getKey().getRevision();
            assertNotNull(r);
            assertEquals(clusterId, r.getClusterId());
            if (entry.getKey().getName().equals("_prev")) {
                if (entry.getValue().type == REMOVE_MAP_ENTRY) {
                    assertTrue(prev.contains(r));
                } else if (entry.getValue().type == SET_MAP_ENTRY) {
                    assertEquals(newPrevId, Utils.getPreviousIdFor("/test", r, 1));
                } else {
                    fail("unexpected update operation " + entry);
                }
            } else {
                fail("unexpected update operation " + entry);
            }
        }
    }

    // OAK-1770
    @Test
    public void splitRevisionsManyClusterNodes() {
        int numClusterNodes = 5;
        String id = Utils.getIdFromPath("/test");
        mk.commit("/", "+\"test\":{}", null, null);
        DocumentStore store = mk.getDocumentStore();
        int clusterId = mk.getNodeStore().getClusterId();

        List<Revision> revs = Lists.newArrayList();
        UpdateOp op = new UpdateOp(id, false);
        for (int i = 0; i < numClusterNodes; i++) {
            // create some commits for each cluster node
            for (int j = 0; j < NUM_REVS_THRESHOLD; j++) {
                Revision r = Revision.newRevision(i + 1);
                if (clusterId == r.getClusterId()) {
                    revs.add(r);
                }
                op.setMapEntry("prop", r, "value");
                NodeDocument.setRevision(op, r, "c");
            }
        }
        store.findAndUpdate(NODES, op);
        NodeDocument doc = store.find(NODES, id);
        assertNotNull(doc);

        // must split document and create a previous document starting at
        // the second most recent revision
        List<UpdateOp> splitOps = Lists.newArrayList(doc.split(
                mk.getNodeStore(), mk.getNodeStore().getHeadRevision(),
                NO_BINARY));
        assertEquals(2, splitOps.size());
        String prevId = Utils.getPreviousIdFor("/test", revs.get(revs.size() - 2), 0);
        assertEquals(prevId, splitOps.get(0).getId());
        assertEquals(id, splitOps.get(1).getId());
    }

    // OAK-1794
    @Test
    public void keepRevisionsForMostRecentChanges() throws Exception {
        DocumentStore store = mk.getDocumentStore();
        NodeStore ns = mk.getNodeStore();
        NodeBuilder builder = ns.getRoot().builder();
        builder.setProperty("foo", -1);
        builder.setProperty("bar", -1);
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        for (int i = 0; i < NUM_REVS_THRESHOLD; i++) {
            builder = ns.getRoot().builder();
            builder.setProperty("foo", i);
            ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }
        mk.runBackgroundOperations();
        NodeDocument doc = store.find(NODES, Utils.getIdFromPath("/"));
        assertNotNull(doc);
        // the local _revisions map must still contain the entry for
        // the initial 'bar' property
        Map<Revision, String> valueMap = doc.getValueMap("bar");
        assertFalse(valueMap.isEmpty());
        Revision r = valueMap.keySet().iterator().next();
        assertTrue(doc.getLocalRevisions().containsKey(r));
        // but also the previous document must contain the revision
        List<NodeDocument> prevDocs = Lists.newArrayList(doc.getAllPreviousDocs());
        assertEquals(1, prevDocs.size());
        NodeDocument prev = prevDocs.get(0);
        assertTrue(prev.getLocalRevisions().containsKey(r));
    }

    // OAK-1794
    @Test
    public void keepCommitRootForMostRecentChanges() throws Exception {
        DocumentStore store = mk.getDocumentStore();
        NodeStore ns = mk.getNodeStore();
        NodeBuilder builder = ns.getRoot().builder();
        builder.setProperty("p", -1);
        NodeBuilder test = builder.child("test");
        test.setProperty("foo", -1);
        test.setProperty("bar", -1);
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        for (int i = 0; i < NUM_REVS_THRESHOLD; i++) {
            builder = ns.getRoot().builder();
            builder.setProperty("p", i);
            test = builder.child("test");
            test.setProperty("foo", i);
            ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }
        mk.runBackgroundOperations();
        NodeDocument doc = store.find(NODES, Utils.getIdFromPath("/test"));
        assertNotNull(doc);
        // the local _commitRoot map must still contain the entry for
        // the initial 'bar' property
        Map<Revision, String> valueMap = doc.getValueMap("bar");
        assertFalse(valueMap.isEmpty());
        Revision r = valueMap.keySet().iterator().next();
        assertTrue(doc.getLocalCommitRoot().containsKey(r));
        // but also the previous document must contain the commitRoot entry
        List<NodeDocument> prevDocs = Lists.newArrayList(doc.getAllPreviousDocs());
        assertEquals(1, prevDocs.size());
        NodeDocument prev = prevDocs.get(0);
        assertTrue(prev.getLocalCommitRoot().containsKey(r));
    }

    @Test(expected = IllegalArgumentException.class)
    public void splitPreviousDocument() {
        NodeDocument doc = new NodeDocument(mk.getDocumentStore());
        doc.put(NodeDocument.ID, Utils.getIdFromPath("/test"));
        doc.put(NodeDocument.SD_TYPE, NodeDocument.SplitDocType.DEFAULT.type);
        RevisionVector head = mk.getNodeStore().getHeadRevision();
        SplitOperations.forDocument(doc, DummyRevisionContext.INSTANCE, head,
                NO_BINARY, NUM_REVS_THRESHOLD);
    }

    @Test
    public void readLocalCommitInfo() throws Exception {
        final Set<String> readSet = Sets.newHashSet();
        DocumentStore store = new MemoryDocumentStore() {
            @Override
            public <T extends Document> T find(Collection<T> collection,
                                               String key,
                                               int maxCacheAge) {
                readSet.add(key);
                return super.find(collection, key, maxCacheAge);
            }
        };
        DocumentNodeStore ns = new DocumentMK.Builder()
                .setDocumentStore(store).setAsyncDelay(0).getNodeStore();
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("test");
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        for (int i = 0; i < NUM_REVS_THRESHOLD; i++) {
            builder = ns.getRoot().builder();
            builder.setProperty("p", i);
            builder.child("test").setProperty("p", i);
            builder.child("test").setProperty("q", i);
            ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        }
        builder = ns.getRoot().builder();
        builder.child("test").removeProperty("q");
        ns.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        ns.runBackgroundOperations();

        NodeDocument doc = store.find(NODES, Utils.getIdFromPath("/test"));
        assertNotNull(doc);

        readSet.clear();

        // must not access previous document of /test
        doc.getNodeAtRevision(ns, ns.getHeadRevision(), null);
        for (String id : Sets.newHashSet(readSet)) {
            doc = store.find(NODES, id);
            assertNotNull(doc);
            if (doc.isSplitDocument() && !doc.getMainPath().equals("/")) {
                fail("must not access previous document: " + id);
            }
        }

        ns.dispose();
    }
    
    // OAK-2528
    @Test
    public void commitRootForChildrenFlag() throws Exception {
        DocumentStore store = mk.getDocumentStore();
        DocumentNodeStore ns = mk.getNodeStore();
        
        for (int i = 0; i < NUM_REVS_THRESHOLD * 2; i++) {
            NodeBuilder builder = ns.getRoot().builder();
            builder.child("test").child("child-" + i);
            merge(ns, builder);
        }
        
        ns.runBackgroundOperations();
        
        NodeDocument doc = store.find(NODES, Utils.getIdFromPath("/test"));
        assertNotNull(doc);
        assertTrue(doc.getLocalCommitRoot().size() < NUM_REVS_THRESHOLD);
    }

    // OAK-3333
    @Test
    public void purgeAllButMostRecentCommitRoot() throws Exception {
        DocumentStore store = mk.getDocumentStore();
        DocumentNodeStore ns1 = mk.getNodeStore();
        NodeBuilder builder1 = ns1.getRoot().builder();
        builder1.child("test");
        merge(ns1, builder1);
        ns1.runBackgroundOperations();

        DocumentNodeStore ns2 = new DocumentMK.Builder().setDocumentStore(store)
                .setAsyncDelay(0).setClusterId(ns1.getClusterId() + 1).getNodeStore();
        // prevent merge retries
        ns2.setMaxBackOffMillis(0);
        assertTrue(ns2.getRoot().hasChildNode("test"));
        NodeBuilder builder2 = ns2.getRoot().builder();
        builder2.child("test").remove();

        for (int i = 0; i < NUM_REVS_THRESHOLD * 2; i++) {
            builder1 = ns1.getRoot().builder();
            builder1.child("test").child("child-" + i);
            merge(ns1, builder1);
        }
        ns1.runBackgroundOperations();

        try {
            merge(ns2, builder2);
            fail("merge must fail with CommitFailedException");
        } catch (CommitFailedException e) {
            // expected
        }
        ns2.dispose();
    }

    // OAK-4050
    @Test
    public void purgeAllButMostRecentCommittedCommitRoot() throws Exception {
        DocumentStore store = mk.getDocumentStore();
        DocumentNodeStore ns1 = mk.getNodeStore();
        NodeBuilder builder1 = ns1.getRoot().builder();
        builder1.child("test");
        merge(ns1, builder1);
        ns1.runBackgroundOperations();

        DocumentNodeStore ns2 = new DocumentMK.Builder().setDocumentStore(store)
                .setAsyncDelay(0).setClusterId(ns1.getClusterId() + 1).getNodeStore();
        // prevent merge retries
        ns2.setMaxBackOffMillis(0);
        assertTrue(ns2.getRoot().hasChildNode("test"));
        NodeBuilder builder2 = ns2.getRoot().builder();
        builder2.child("test").remove();

        for (int i = 0; i < NUM_REVS_THRESHOLD * 2; i++) {
            builder1 = ns1.getRoot().builder();
            builder1.child("test").child("child-" + i);
            merge(ns1, builder1);
        }
        // create a _commitRoot entry for a revision, which is not committed
        UpdateOp op = new UpdateOp(Utils.getIdFromPath("/test"), false);
        NodeDocument.setCommitRoot(op, ns1.newRevision(), 0);
        store.findAndUpdate(NODES, op);

        ns1.runBackgroundOperations();

        try {
            merge(ns2, builder2);
            fail("merge must fail with CommitFailedException");
        } catch (CommitFailedException e) {
            // expected
        }
        ns2.dispose();
    }

    // OAK-3081
    @Test
    public void removeGarbage() throws Exception {
        final DocumentStore store = mk.getDocumentStore();
        final DocumentNodeStore ns = mk.getNodeStore();
        final List<Exception> exceptions = Lists.newArrayList();
        final List<RevisionVector> revisions = Lists.newArrayList();

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    for (int i = 0; i < 200; i++) {
                        NodeBuilder builder = ns.getRoot().builder();
                        builder.child("foo").child("node").child("node").child("node").child("node");
                        builder.child("bar").child("node").child("node").child("node").child("node");
                        merge(ns, builder);
                        revisions.add(ns.getHeadRevision());

                        builder = ns.getRoot().builder();
                        builder.child("foo").child("node").remove();
                        builder.child("bar").child("node").remove();
                        merge(ns, builder);
                        revisions.add(ns.getHeadRevision());
                    }
                } catch (CommitFailedException e) {
                    exceptions.add(e);
                }
            }
        });
        t.start();

        // Use a revision context, which wraps the DocumentNodeStore and
        // randomly delays calls to get the head revision
        RevisionContext rc = new TestRevisionContext(ns);
        while (t.isAlive()) {
            for (String id : ns.getSplitCandidates()) {
                RevisionVector head = ns.getHeadRevision();
                NodeDocument doc = store.find(NODES, id);
                List<UpdateOp> ops = SplitOperations.forDocument(doc, rc, head,
                        NO_BINARY, NUM_REVS_THRESHOLD);
                Set<Revision> removed = Sets.newHashSet();
                Set<Revision> added = Sets.newHashSet();
                for (UpdateOp op : ops) {
                    for (Map.Entry<Key, Operation> e : op.getChanges().entrySet()) {
                        if (!"_deleted".equals(e.getKey().getName())) {
                            continue;
                        }
                        Revision r = e.getKey().getRevision();
                        if (e.getValue().type == Operation.Type.REMOVE_MAP_ENTRY) {
                            removed.add(r);
                        } else if (e.getValue().type == Operation.Type.SET_MAP_ENTRY) {
                            added.add(r);
                        }
                    }
                }
                removed.removeAll(added);
                assertTrue("SplitOperations must not remove committed changes: " + removed, removed.isEmpty());
            }
            // perform the actual cleanup
            ns.runBackgroundOperations();
        }

        // check documents below /foo and /bar
        // the _deleted map must contain all revisions
        for (NodeDocument doc : Utils.getAllDocuments(store)) {
            if (doc.isSplitDocument() || Utils.getDepthFromId(doc.getId()) < 2) {
                continue;
            }
            Set<Revision> revs = Sets.newHashSet();
            for (RevisionVector rv : revisions) {
                Iterables.addAll(revs, rv);
            }
            revs.removeAll(doc.getValueMap("_deleted").keySet());
            assertTrue("Missing _deleted entries on " + doc.getId() + ": " + revs, revs.isEmpty());
        }
    }

    @Test
    public void splitDocumentWithBinary() throws Exception {
        DocumentStore store = mk.getDocumentStore();
        DocumentNodeStore ns = mk.getNodeStore();
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        merge(ns, builder);

        // use more than 4k of binary data (OAK-5205)
        PropertyState binary = binaryProperty("p", randomBytes(5 * 1024));

        for (int i = 0; i < 10; i++) {
            builder = ns.getRoot().builder();
            builder.child("foo").setProperty(binary);
            merge(ns, builder);
            ns.runBackgroundOperations();
        }

        NodeDocument foo = store.find(NODES, Utils.getIdFromPath("/foo"));
        assertNotNull(foo);
        List<NodeDocument> prevDocs = copyOf(foo.getAllPreviousDocs());
        // all but most recent value are moved to individual previous docs
        assertEquals(9, prevDocs.size());
    }

    @Test
    public void noBinarySplitWhenRemoved() throws Exception {
        DocumentStore store = mk.getDocumentStore();
        DocumentNodeStore ns = mk.getNodeStore();
        NodeBuilder builder = ns.getRoot().builder();
        // use more than 4k of binary data (OAK-5205)
        PropertyState binary = binaryProperty("p", randomBytes(5 * 1024));
        builder.child("foo").setProperty(binary);
        merge(ns, builder);

        builder = ns.getRoot().builder();
        builder.child("foo").remove();
        merge(ns, builder);
        ns.runBackgroundOperations();

        // must not create split document in this case. See OAK-5010
        NodeDocument foo = store.find(NODES, Utils.getIdFromPath("/foo"));
        assertNotNull(foo);
        assertEquals(0, foo.getPreviousRanges().size());

        // re-create it
        builder = ns.getRoot().builder();
        builder.child("foo");
        merge(ns, builder);
        ns.runBackgroundOperations();

        // now the old binary value must be moved to a previous document
        foo = store.find(NODES, Utils.getIdFromPath("/foo"));
        assertNotNull(foo);
        List<NodeDocument> prevDocs = copyOf(foo.getAllPreviousDocs());
        assertEquals(1, prevDocs.size());
    }

    // OAK-5205
    @Test
    public void noSplitForSmallBinary() throws Exception {
        DocumentStore store = mk.getDocumentStore();
        DocumentNodeStore ns = mk.getNodeStore();
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        merge(ns, builder);

        for (int i = 0; i < 10; i++) {
            builder = ns.getRoot().builder();
            builder.child("foo").setProperty(
                    binaryProperty("p", ("value" + i).getBytes()));
            merge(ns, builder);
            ns.runBackgroundOperations();
        }

        NodeDocument foo = store.find(NODES, Utils.getIdFromPath("/foo"));
        assertNotNull(foo);
        List<NodeDocument> prevDocs = copyOf(foo.getAllPreviousDocs());
        // must not create split documents for small binaries less 4k
        assertEquals(0, prevDocs.size());
    }

    @Test
    public void nonSplittableBigDocument() throws Exception {
        DocumentStore store = mk.getDocumentStore();
        DocumentNodeStore ns = mk.getNodeStore();
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        merge(ns, builder);

        String id = Utils.getIdFromPath("/foo");
        int num = 0;
        while (store.find(NODES, id).getMemory() < DOC_SIZE_THRESHOLD) {
            builder = ns.getRoot().builder();
            for (int i = 0; i < 50; i++) {
                builder.child("foo").setProperty("p" + num++,
                        "some value as payload for the document");
            }
            merge(ns, builder);
        }

        Iterable<UpdateOp> splitOps = store.find(NODES, id)
                .split(ns, ns.getHeadRevision(), NO_BINARY);
        assertEquals(0, Iterables.size(splitOps));
    }

    @Test
    public void splitWithBranchCommit() throws Exception {
        DocumentStore store = mk.getDocumentStore();
        DocumentNodeStore ns = mk.getNodeStore();
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo");
        merge(ns, builder);

        String branch = mk.branch(null);
        branch = mk.commit("/foo", "^\"p\":\"value\"", branch, null);
        mk.merge(branch, null);

        String id = Utils.getIdFromPath("/foo");
        NodeDocument doc = store.find(NODES, id);
        assertNotNull(doc);
        assertThat(doc.getLocalBranchCommits(), is(not(empty())));

        for (int i = 0; i < 5; i++) {
            builder = ns.getRoot().builder();
            builder.child("foo").setProperty("p", "value-" + i);
            merge(ns, builder);
        }
        ns.runBackgroundOperations();
        doc = store.find(NODES, id);
        for (UpdateOp op : SplitOperations.forDocument(doc, ns, ns.getHeadRevision(), NO_BINARY, 5)) {
            store.createOrUpdate(NODES, op);
        }
        doc = store.find(NODES, id);
        // must have a previous document now
        assertThat(doc.getPreviousRanges().keySet(), hasSize(1));
        // branch commit entry moved to previous document
        assertThat(doc.getLocalBranchCommits(), is(empty()));
        NodeDocument prev = doc.getAllPreviousDocs().next();
        assertThat(prev.getLocalBranchCommits(), is(not(empty())));
    }

    @Test
    public void splitDefaultNoBranch() throws Exception {
        DocumentStore store = mk.getDocumentStore();
        DocumentNodeStore ns = mk.getNodeStore();
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("foo").child("bar");
        merge(ns, builder);

        for (int i = 0; i < 5; i++) {
            builder = ns.getRoot().builder();
            builder.child("foo").setProperty("p", "value-" + i);
            merge(ns, builder);
        }
        ns.runBackgroundOperations();
        String id = Utils.getIdFromPath("/foo");
        NodeDocument doc = store.find(NODES, id);
        assertNotNull(doc);
        for (UpdateOp op : SplitOperations.forDocument(doc, ns, ns.getHeadRevision(), NO_BINARY, 5)) {
            store.createOrUpdate(NODES, op);
        }
        doc = store.find(NODES, id);
        // must have a previous document now
        assertThat(doc.getPreviousRanges().keySet(), is(not(empty())));
        Iterator<NodeDocument> it = doc.getAllPreviousDocs();
        while (it.hasNext()) {
            assertEquals(SplitDocType.DEFAULT_NO_BRANCH, it.next().getSplitDocType());
        }
    }

    private static class TestRevisionContext implements RevisionContext {

        private final RevisionContext rc;

        TestRevisionContext(RevisionContext rc) {
            this.rc = rc;
        }

        @Override
        public UnmergedBranches getBranches() {
            return rc.getBranches();
        }

        @Override
        public UnsavedModifications getPendingModifications() {
            return rc.getPendingModifications();
        }

        @Override
        public int getClusterId() {
            return rc.getClusterId();
        }

        @Nonnull
        @Override
        public RevisionVector getHeadRevision() {
            try {
                Thread.sleep((long) (Math.random() * 100));
            } catch (InterruptedException e) {
                // ignore
            }
            return rc.getHeadRevision();
        }

        @Nonnull
        @Override
        public Revision newRevision() {
            return rc.newRevision();
        }

        @Nonnull
        @Override
        public Clock getClock() {
            return rc.getClock();
        }

        @Override
        public String getCommitValue(@Nonnull Revision changeRevision,
                                     @Nonnull NodeDocument doc) {
            return rc.getCommitValue(changeRevision, doc);
        }
    }

    private static NodeState merge(NodeStore store, NodeBuilder root)
            throws CommitFailedException {
        return store.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }
    
    private void syncMKs(List<DocumentMK> mks, int idx) {
        mks.get(idx).runBackgroundOperations();
        for (int i = 0; i < mks.size(); i++) {
            if (idx != i) {
                mks.get(i).runBackgroundOperations();
            }
        }
    }

    private byte[] randomBytes(int num) {
        Random random = new Random(42);
        byte[] data = new byte[num];
        random.nextBytes(data);
        return data;
    }
}
