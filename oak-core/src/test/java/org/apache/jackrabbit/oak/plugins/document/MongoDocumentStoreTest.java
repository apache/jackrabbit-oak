/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

/**
 * Tests the document store.
 */
public class MongoDocumentStoreTest {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDocumentStoreTest.class);

    @Rule
    public MongoConnectionFactory connectionFactory = new MongoConnectionFactory();

//    private static final boolean MONGO_DB = true;
//    private static final int NODE_COUNT = 2000;

    private static final boolean MONGO_DB = false;
    private static final int NODE_COUNT = 10;

    DocumentStore openDocumentStore() {
        if (MONGO_DB) {
            return new MongoDocumentStore(connectionFactory.getConnection().getDB(), new DocumentMK.Builder());
        }
        return new MemoryDocumentStore();
    }

    void dropCollections() {
        if (MONGO_DB) {
            MongoUtils.dropCollections(connectionFactory.getConnection().getDB());
        }
    }

    @Before
    @After
    public void cleanUp() {
        dropCollections();
    }

    @Test
    public void addGetAndRemove() throws Exception {
        DocumentStore docStore = openDocumentStore();

        UpdateOp updateOp = new UpdateOp("/", true);
        Revision r1 = new Revision(0, 0, 0);
        updateOp.setMapEntry("property1", r1, "value1");
        updateOp.increment("property2", 1);
        updateOp.set("property3", "value3");
        docStore.createOrUpdate(Collection.NODES, updateOp);
        NodeDocument doc = docStore.find(Collection.NODES, "/");
        assertNotNull(doc);

        Map<?, ?> property1 = (Map<?, ?>) doc.get("property1");
        assertNotNull(property1);
        String value1 = (String) property1.get(r1);
        assertEquals("value1", value1);

        Long value2 = (Long) doc.get("property2");
        assertEquals(Long.valueOf(1), value2);

        String value3 = (String) doc.get("property3");
        assertEquals("value3", value3);

        docStore.remove(Collection.NODES, "/");
        doc = docStore.find(Collection.NODES, "/");
        assertTrue(doc == null);
    }

    @Test
    public void batchRemove() throws Exception {
        DocumentStore docStore = openDocumentStore();
        int nUpdates = 10;
        Revision r1 = new Revision(0, 0, 0);
        List<String> ids = Lists.newArrayList();
        List<UpdateOp> updateOps = new ArrayList<UpdateOp>();
        for (int i = 0; i < nUpdates; i++) {
            String path = "/node" + i;
            UpdateOp updateOp = new UpdateOp(path, true);
            updateOp.setMapEntry("property1", r1, "value1");
            updateOp.increment("property2", 1);
            updateOp.set("property3", "value3");
            updateOps.add(updateOp);
            ids.add(updateOp.getId());
        }
        docStore.create(Collection.NODES, updateOps);

        for(String id : ids){
            assertNotNull(docStore.find(Collection.NODES, id));
        }

        docStore.remove(Collection.NODES, ids);
        for(String id : ids){
            assertNull(docStore.find(Collection.NODES, id));
        }
    }

    @Test
    public void batchAdd() throws Exception {
        DocumentStore docStore = openDocumentStore();
        Revision r1 = new Revision(0, 0, 0);
        int nUpdates = 10;
        List<UpdateOp> updateOps = new ArrayList<UpdateOp>();
        for (int i = 0; i < nUpdates; i++) {
            String path = "/node" + i;
            UpdateOp updateOp = new UpdateOp(path, true);
            updateOp.setMapEntry("property1", r1, "value1");
            updateOp.increment("property2", 1);
            updateOp.set("property3", "value3");
            updateOps.add(updateOp);
        }
        docStore.create(Collection.NODES, updateOps);
    }

    @Test
    public void addLotsOfNodes() throws Exception {
        char[] nPrefix = new char[]{'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'};
        int nNodes = NODE_COUNT;

        for (int nThreads = 1; nThreads < 32; nThreads = nThreads * 2) {
            DocumentStore docStore = openDocumentStore();
            dropCollections();

            log("Adding and updating " + nNodes + " nodes in each " + nThreads + " threads");
            long start = System.currentTimeMillis();

            ExecutorService executor = Executors.newFixedThreadPool(nThreads);
            for (int j = 0; j < nThreads; j++) {
                executor.submit(new AddAndUpdateNodesTask(docStore, "node" + nPrefix[j], nNodes));
            }
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.MINUTES);

            long end = System.currentTimeMillis();
            log("Done: " + (end - start) + "ms");

        }
    }

    @Test
    public void containsMapEntry() {
        Revision r = new Revision(0, 0, 0);
        Revision unknown = new Revision(0, 1, 0);
        DocumentStore docStore = openDocumentStore();
        UpdateOp op = new UpdateOp("/node", true);
        op.setMapEntry("map", r, "value");
        docStore.createOrUpdate(Collection.NODES, op);

        op = new UpdateOp("/node", false);
        op.set("prop", "value");
        op.containsMapEntry("map", unknown, true);
        // update if unknown-key exists -> must not succeed
        assertNull(docStore.findAndUpdate(Collection.NODES, op));

        op = new UpdateOp("/node", false);
        op.set("prop", "value");
        op.containsMapEntry("map", r, true);
        // update if key exists -> must succeed
        NodeDocument doc = docStore.findAndUpdate(Collection.NODES, op);
        assertNotNull(doc);

        doc = docStore.find(Collection.NODES, "/node");
        assertNotNull(doc);
        assertNotNull(doc.get("prop"));
        assertEquals("value", doc.get("prop"));

        op = new UpdateOp("/node", false);
        op.set("prop", "other");
        op.containsMapEntry("map", r, false);
        // update if key does not exist -> must not succeed
        assertNull(docStore.findAndUpdate(Collection.NODES, op));

        // value must still be the same
        doc = docStore.find(Collection.NODES, "/node");
        assertNotNull(doc);
        assertNotNull(doc.get("prop"));
        assertEquals("value", doc.get("prop"));
    }

    @Test
    public void queryWithLimit() throws Exception {
        DocumentStore docStore = openDocumentStore();
        DocumentNodeStore store = new DocumentMK.Builder()
                .setDocumentStore(docStore).setAsyncDelay(0).getNodeStore();
        Revision rev = Revision.newRevision(0);
        List<UpdateOp> inserts = new ArrayList<UpdateOp>();
        for (int i = 0; i < DocumentMK.MANY_CHILDREN_THRESHOLD * 2; i++) {
            DocumentNodeState n = new DocumentNodeState(store, "/node-" + i,
                    new RevisionVector(rev));
            inserts.add(n.asOperation(rev));
        }
        docStore.create(Collection.NODES, inserts);
        List<NodeDocument> docs = docStore.query(Collection.NODES,
                Utils.getKeyLowerLimit("/"),  Utils.getKeyUpperLimit("/"),
                DocumentMK.MANY_CHILDREN_THRESHOLD);
        assertEquals(DocumentMK.MANY_CHILDREN_THRESHOLD, docs.size());
        store.dispose();
    }

    @Test
    @Ignore
    public void batchInsert() throws Exception {
        doInsert(NODE_COUNT, true);
        doInsert(NODE_COUNT, false);
    }

    private void doInsert(int n, boolean batch) throws Exception {
        dropCollections();

        DBCollection collection = connectionFactory.getConnection().getDB().getCollection("batchInsertTest");
        DBObject index = new BasicDBObject();
        index.put("_path", 1L);
        DBObject options = new BasicDBObject();
        options.put("unique", Boolean.TRUE);
        collection.createIndex(index, options);

        log("Inserting " + n + " batch? " + batch);
        long start = System.currentTimeMillis();

        if (batch) {
            DBObject[] arr = new BasicDBObject[n];
            for (int i = 0; i < n; i++) {
                arr[i] = new BasicDBObject("_path", "/a" + i);
            }
            collection.insert(arr);
        } else {
            for (int i = 0; i < n; i++) {
                collection.insert(new BasicDBObject("_path", "/a" + i));
            }

        }

        long end = System.currentTimeMillis();
        log("Done: " + (end - start) + "ms");

        dropCollections();
    }

    private static void log(String s) {
        LOG.info(s);
    }

    /**
     * Task to create / update nodes.
     */
    private static class AddAndUpdateNodesTask implements Runnable {

        private final DocumentStore docStore;
        private final String nodeName;
        private final int nNodes;

        public AddAndUpdateNodesTask(DocumentStore docStore, String nodeName, int nNodes) {
            this.docStore = docStore;
            this.nodeName = nodeName;
            this.nNodes = nNodes;
        }

        @Override
        public void run() {
            addNodes();
            updateNodes();
        }

        private void addNodes() {
            Revision r1 = new Revision(0, 0, 0);
            for (int i = 0; i < nNodes; i++) {
                String path = "/" + nodeName + i;
                UpdateOp updateOp = new UpdateOp(path, true);
                updateOp.setMapEntry("property1", r1, "value1");
                updateOp.set("property3", "value3");
                docStore.createOrUpdate(Collection.NODES, updateOp);
            }
        }

        private void updateNodes() {
            Revision r2 = new Revision(0, 1, 0);
            for (int i = 0; i < nNodes; i++) {
                String path = "/" + nodeName + i;
                UpdateOp updateOp = new UpdateOp(path, false);
                updateOp.setMapEntry("property1", r2, "value2");
                updateOp.set("property4", "value4");
                docStore.createOrUpdate(Collection.NODES, updateOp);
            }
        }
    }
}