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
package org.apache.jackrabbit.oak.plugins.mongomk;

import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.mk.blobs.MemoryBlobStore;
import org.apache.jackrabbit.oak.plugins.mongomk.util.Utils;
import org.junit.Test;

import com.google.common.collect.Sets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Check correct splitting of documents (OAK-926).
 */
public class DocumentSplitTest extends BaseMongoMKTest {

    @Test
    public void splitRevisions() throws Exception {
        DocumentStore store = mk.getDocumentStore();
        MongoNodeStore ns = mk.getNodeStore();
        Set<Revision> revisions = Sets.newHashSet();
        NodeDocument doc = store.find(Collection.NODES, Utils.getIdFromPath("/"));
        assertNotNull(doc);
        revisions.addAll(doc.getLocalRevisions().keySet());
        revisions.add(Revision.fromString(mk.commit("/", "+\"foo\":{}+\"bar\":{}", null, null)));
        // create nodes
        while (revisions.size() <= NodeDocument.REVISIONS_SPLIT_OFF_SIZE) {
            revisions.add(Revision.fromString(mk.commit("/", "+\"foo/node-" + revisions.size() + "\":{}" +
                    "+\"bar/node-" + revisions.size() + "\":{}", null, null)));
        }
        mk.runBackgroundOperations();
        String head = mk.getHeadRevision();
        doc = store.find(Collection.NODES, Utils.getIdFromPath("/"));
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
        MongoNodeStore ns = mk.getNodeStore();
        Set<Revision> revisions = Sets.newHashSet();
        mk.commit("/", "+\"foo\":{}", null, null);
        NodeDocument doc = store.find(Collection.NODES, Utils.getIdFromPath("/foo"));
        assertNotNull(doc);
        revisions.addAll(doc.getLocalRevisions().keySet());
        boolean create = false;
        while (revisions.size() <= NodeDocument.REVISIONS_SPLIT_OFF_SIZE) {
            if (create) {
                revisions.add(Revision.fromString(mk.commit("/", "+\"foo\":{}", null, null)));
            } else {
                revisions.add(Revision.fromString(mk.commit("/", "-\"foo\"", null, null)));
            }
            create = !create;
        }
        mk.runBackgroundOperations();
        String head = mk.getHeadRevision();
        doc = store.find(Collection.NODES, Utils.getIdFromPath("/foo"));
        assertNotNull(doc);
        Map<Revision, String> deleted = doc.getLocalDeleted();
        // one remaining in the local deleted map
        assertEquals(1, deleted.size());
        for (Revision rev : revisions) {
            assertTrue(doc.containsRevision(rev));
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
        NodeDocument doc = store.find(Collection.NODES, Utils.getIdFromPath("/foo"));
        assertNotNull(doc);
        Set<Revision> commitRoots = Sets.newHashSet();
        commitRoots.addAll(doc.getLocalCommitRoot().keySet());
        // create nodes
        while (commitRoots.size() <= NodeDocument.REVISIONS_SPLIT_OFF_SIZE) {
            commitRoots.add(Revision.fromString(mk.commit("/", "^\"foo/prop\":" +
                    commitRoots.size() + "^\"bar/prop\":" + commitRoots.size(), null, null)));
        }
        mk.runBackgroundOperations();
        doc = store.find(Collection.NODES, Utils.getIdFromPath("/foo"));
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
        MongoNodeStore ns = mk.getNodeStore();
        mk.commit("/", "+\"foo\":{}", null, null);
        NodeDocument doc = store.find(Collection.NODES, Utils.getIdFromPath("/foo"));
        assertNotNull(doc);
        Set<Revision> revisions = Sets.newHashSet();
        // create nodes
        while (revisions.size() <= NodeDocument.REVISIONS_SPLIT_OFF_SIZE) {
            revisions.add(Revision.fromString(mk.commit("/", "^\"foo/prop\":" +
                    revisions.size(), null, null)));
        }
        mk.runBackgroundOperations();
        doc = store.find(Collection.NODES, Utils.getIdFromPath("/foo"));
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
        MongoMK.Builder builder;

        builder = new MongoMK.Builder();
        builder.setDocumentStore(ds).setBlobStore(bs).setAsyncDelay(0);
        MongoMK mk1 = builder.setClusterId(1).open();

        mk1.commit("/", "+\"test\":{\"prop1\":0}", null, null);
        // make sure the new node is visible to other MongoMK instances
        mk1.backgroundWrite();

        builder = new MongoMK.Builder();
        builder.setDocumentStore(ds).setBlobStore(bs).setAsyncDelay(0);
        MongoMK mk2 = builder.setClusterId(2).open();
        builder = new MongoMK.Builder();
        builder.setDocumentStore(ds).setBlobStore(bs).setAsyncDelay(0);
        MongoMK mk3 = builder.setClusterId(3).open();

        for (int i = 0; i < NodeDocument.REVISIONS_SPLIT_OFF_SIZE; i++) {
            mk1.commit("/", "^\"test/prop1\":" + i, null, null);
            mk2.commit("/", "^\"test/prop2\":" + i, null, null);
            mk3.commit("/", "^\"test/prop3\":" + i, null, null);
        }

        mk1.runBackgroundOperations();
        mk2.runBackgroundOperations();
        mk3.runBackgroundOperations();

        NodeDocument doc = ds.find(Collection.NODES, Utils.getIdFromPath("/test"));
        assertNotNull(doc);
        Map<Revision, String> revs = doc.getLocalRevisions();
        assertEquals(3, revs.size());
        revs = doc.getValueMap("_revisions");
        assertEquals(3 * NodeDocument.REVISIONS_SPLIT_OFF_SIZE + 1, revs.size());
        Revision previous = null;
        for (Map.Entry<Revision, String> entry : revs.entrySet()) {
            if (previous != null) {
                assertTrue(previous.compareRevisionTime(entry.getKey()) > 0);
            }
            previous = entry.getKey();
        }
    }
}
