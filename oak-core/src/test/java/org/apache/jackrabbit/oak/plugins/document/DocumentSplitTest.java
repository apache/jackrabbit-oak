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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.jackrabbit.mk.blobs.MemoryBlobStore;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.junit.Test;

import com.google.common.collect.Sets;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Check correct splitting of documents (OAK-926).
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
            assertTrue(doc.isCommitted(rev));
        }
        // check if document is still there
        assertNotNull(ns.getNode("/", Revision.fromString(head)));
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
            assertTrue(doc.isCommitted(rev));
        }
        Node node = ns.getNode("/foo", Revision.fromString(head));
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
        // one remaining in the local commit root map
        assertEquals(1, commits.size());
        for (Revision rev : commitRoots) {
            assertTrue(doc.isCommitted(rev));
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
            assertTrue(doc.isCommitted(rev));
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
            Revision head = ns.getHeadRevision();
            Revision lastRev = ns.getPendingModifications().get("/test");
            Node n = doc.getNodeAtRevision(mk.getNodeStore(), head, lastRev);
            assertNotNull(n);
            String value = n.getProperty(name);
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

    private void syncMKs(List<DocumentMK> mks, int idx) {
        mks.get(idx).runBackgroundOperations();
        for (int i = 0; i < mks.size(); i++) {
            if (idx != i) {
                mks.get(i).runBackgroundOperations();
            }
        }
    }
}
