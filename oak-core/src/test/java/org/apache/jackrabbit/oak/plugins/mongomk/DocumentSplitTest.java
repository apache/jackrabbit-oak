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
        Set<String> revisions = Sets.newHashSet();
        NodeDocument doc = store.find(Collection.NODES, Utils.getIdFromPath("/"));
        assertNotNull(doc);
        revisions.addAll(doc.getLocalRevisions().keySet());
        revisions.add(mk.commit("/", "+\"foo\":{}+\"bar\":{}", null, null));
        // create nodes
        while (revisions.size() <= NodeDocument.REVISIONS_SPLIT_OFF_SIZE) {
            revisions.add(mk.commit("/", "+\"foo/node-" + revisions.size() + "\":{}" +
                    "+\"bar/node-" + revisions.size() + "\":{}", null, null));
        }
        mk.runBackgroundOperations();
        String head = mk.getHeadRevision();
        doc = store.find(Collection.NODES, Utils.getIdFromPath("/"));
        assertNotNull(doc);
        Map<String, String> revs = doc.getLocalRevisions();
        // one remaining in the local revisions map
        assertEquals(1, revs.size());
        for (String r : revisions) {
            Revision rev = Revision.fromString(r);
            assertTrue(doc.containsRevision(rev));
            assertTrue(doc.isCommitted(rev));
        }
        // check if document is still there
        assertNotNull(doc.getNodeAtRevision(mk, Revision.fromString(head)));
        mk.commit("/", "+\"baz\":{}", null, null);
        mk.setAsyncDelay(0);
        mk.backgroundWrite();
    }

    @Test
    public void splitDeleted() throws Exception {
        DocumentStore store = mk.getDocumentStore();
        Set<String> revisions = Sets.newHashSet();
        mk.commit("/", "+\"foo\":{}", null, null);
        NodeDocument doc = store.find(Collection.NODES, Utils.getIdFromPath("/foo"));
        assertNotNull(doc);
        revisions.addAll(doc.getLocalRevisions().keySet());
        boolean create = false;
        while (revisions.size() <= NodeDocument.REVISIONS_SPLIT_OFF_SIZE) {
            if (create) {
                revisions.add(mk.commit("/", "+\"foo\":{}", null, null));
            } else {
                revisions.add(mk.commit("/", "-\"foo\"", null, null));
            }
            create = !create;
        }
        mk.runBackgroundOperations();
        String head = mk.getHeadRevision();
        doc = store.find(Collection.NODES, Utils.getIdFromPath("/foo"));
        assertNotNull(doc);
        Map<String, String> deleted = doc.getLocalDeleted();
        // one remaining in the local deleted map
        assertEquals(1, deleted.size());
        for (String r : revisions) {
            Revision rev = Revision.fromString(r);
            assertTrue(doc.containsRevision(rev));
            assertTrue(doc.isCommitted(rev));
        }
        Node node = doc.getNodeAtRevision(mk, Revision.fromString(head));
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
        Set<String> commitRoots = Sets.newHashSet();
        commitRoots.addAll(doc.getLocalCommitRoot().keySet());
        // create nodes
        while (commitRoots.size() <= NodeDocument.REVISIONS_SPLIT_OFF_SIZE) {
            commitRoots.add(mk.commit("/", "^\"foo/prop\":" + commitRoots.size() +
                    "^\"bar/prop\":" + commitRoots.size(), null, null));
        }
        mk.runBackgroundOperations();
        doc = store.find(Collection.NODES, Utils.getIdFromPath("/foo"));
        assertNotNull(doc);
        Map<String, String> commits = doc.getLocalCommitRoot();
        // one remaining in the local commit root map
        assertEquals(1, commits.size());
        for (String r : commitRoots) {
            Revision rev = Revision.fromString(r);
            assertTrue(doc.isCommitted(rev));
        }
    }
}
