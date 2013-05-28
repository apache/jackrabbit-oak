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
package org.apache.jackrabbit.mongomk;

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

import org.apache.jackrabbit.mongomk.DocumentStore.Collection;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;

/**
 * Tests the document store.
 */
public class MongoDocumentStoreTest {
    
    private static final Logger LOG = LoggerFactory.getLogger(MongoDocumentStoreTest.class);

//    private static final boolean MONGO_DB = true;
//    private static final int NODE_COUNT = 2000;
    
    private static final boolean MONGO_DB = false;
    private static final int NODE_COUNT = 10;

    DocumentStore openDocumentStore() {
        if (MONGO_DB) {
            return new MongoDocumentStore(MongoUtils.getConnection().getDB());
        }
        return new MemoryDocumentStore();
    }

    void dropCollections() {
        if (MONGO_DB) {
            MongoUtils.dropCollections(MongoUtils.getConnection().getDB());
        }
    }

    @Test
    public void addGetAndRemove() throws Exception {
        dropCollections();
        DocumentStore docStore = openDocumentStore();

        UpdateOp updateOp = new UpdateOp("/", "/", true);
        updateOp.setMapEntry("property1", "key1", "value1");
        updateOp.increment("property2", 1);
        updateOp.set("property3", "value3");
        docStore.createOrUpdate(Collection.NODES, updateOp);
        Map<String, Object> obj = docStore.find(Collection.NODES, "/");

        Map<?, ?> property1 = (Map<?, ?>) obj.get("property1");
        String value1 = (String) property1.get("key1");
        assertEquals("value1", value1);

        Long value2 = (Long) obj.get("property2");
        assertEquals(Long.valueOf(1), value2);

        String value3 = (String) obj.get("property3");
        assertEquals("value3", value3);

        docStore.remove(Collection.NODES, "/");
        obj = docStore.find(Collection.NODES, "/");
        assertTrue(obj == null);
        dropCollections();
    }

    @Test
    public void batchAdd() throws Exception {
        dropCollections();

        DocumentStore docStore = openDocumentStore();
        int nUpdates = 10;
        List<UpdateOp> updateOps = new ArrayList<UpdateOp>();
        for (int i = 0; i < nUpdates; i++) {
            String path = "/node" + i;
            UpdateOp updateOp = new UpdateOp(path, path, true);
            updateOp.set(UpdateOp.ID, "/node" + i);
            updateOp.setMapEntry("property1", "key1", "value1");
            updateOp.increment("property2", 1);
            updateOp.set("property3", "value3");
            updateOps.add(updateOp);
        }
        docStore.create(Collection.NODES, updateOps);

        dropCollections();
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
        dropCollections();

    }

    @Test
    public void containsMapEntry() {
        dropCollections();

        DocumentStore docStore = openDocumentStore();
        UpdateOp op = new UpdateOp("/node", "/node", true);
        op.setMapEntry("map", "key", "value");
        docStore.createOrUpdate(Collection.NODES, op);

        op = new UpdateOp("/node", "/node", false);
        op.set("prop", "value");
        op.containsMapEntry("map", "unknown-key", true);
        // update if unknown-key exists -> must not succeed
        assertNull(docStore.findAndUpdate(Collection.NODES, op));

        op = new UpdateOp("/node", "/node", false);
        op.set("prop", "value");
        op.containsMapEntry("map", "key", true);
        // update if key exists -> must succeed
        Map doc = docStore.findAndUpdate(Collection.NODES, op);
        assertNotNull(doc);

        doc = docStore.find(Collection.NODES, "/node");
        assertTrue(doc.containsKey("prop"));
        assertEquals("value", doc.get("prop"));

        op = new UpdateOp("/node", "/node", false);
        op.set("prop", "other");
        op.containsMapEntry("map", "key", false);
        // update if key does not exist -> must not succeed
        assertNull(docStore.findAndUpdate(Collection.NODES, op));

        // value must still be the same
        doc = docStore.find(Collection.NODES, "/node");
        assertTrue(doc.containsKey("prop"));
        assertEquals("value", doc.get("prop"));

        dropCollections();
    }

    @Test
    @Ignore
    public void batchInsert() throws Exception {
        doInsert(NODE_COUNT, true);
        doInsert(NODE_COUNT, false);
    }

    private void doInsert(int n, boolean batch) throws Exception {
        dropCollections();

        DBCollection collection = MongoUtils.getConnection().getDB().getCollection("batchInsertTest");
        DBObject index = new BasicDBObject();
        index.put("_path", 1L);
        DBObject options = new BasicDBObject();
        options.put("unique", Boolean.TRUE);
        collection.ensureIndex(index, options);

        log("Inserting " + n + " batch? " + batch);
        long start = System.currentTimeMillis();

        if (batch) {
            DBObject[] arr = new BasicDBObject[n];
            for (int i = 0; i < n; i++) {
                arr[i] = new BasicDBObject("_path", "/a" + i);
            }
            WriteResult result = collection.insert(arr);
            if (result.getError() != null) {
                log("Error: " + result.getError());
            }
        } else {
            for (int i = 0; i < n; i++) {
                WriteResult result = collection.insert(new BasicDBObject("_path", "/a" + i));
                if (result.getError() != null) {
                    log("Error: " + result.getError());
                }
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
            for (int i = 0; i < nNodes; i++) {
                String path = "/" + nodeName + i;
                UpdateOp updateOp = new UpdateOp(path, path, true);
                updateOp.setMapEntry("property1", "key1", "value1");
                updateOp.set("property3", "value3");
                docStore.createOrUpdate(Collection.NODES, updateOp);
            }
        }

        private void updateNodes() {
            for (int i = 0; i < nNodes; i++) {
                String path = "/" + nodeName + i;
                UpdateOp updateOp = new UpdateOp(path, path, false);
                updateOp.setMapEntry("property1", "key2", "value2");
                updateOp.set("property4", "value4");
                docStore.createOrUpdate(Collection.NODES, updateOp);
            }
        }
    }
}