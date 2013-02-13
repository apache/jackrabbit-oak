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
package org.apache.jackrabbit.mongomk.prototype;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.mongomk.AbstractMongoConnectionTest;
import org.apache.jackrabbit.mongomk.prototype.DocumentStore.Collection;
import org.junit.Ignore;
import org.junit.Test;

public class MongoDocumentStoreTest extends AbstractMongoConnectionTest {

    @Test
    @Ignore
    public void addGetAndRemove() throws Exception {
        DocumentStore docStore = new MongoDocumentStore(mongoConnection.getDB());

        UpdateOp updateOp = new UpdateOp("/");
        updateOp.addMapEntry("property1", "key1", "value1");
        updateOp.increment("property2", 1);
        updateOp.set("property3", "value3");
        docStore.createOrUpdate(Collection.NODES, updateOp);
        Map<String, Object> obj = docStore.find(Collection.NODES, "/");

        Map property1 = (Map)obj.get("property1");
        String value1 = (String)property1.get("key1");
        assertEquals("value1", value1);

        Long value2 = (Long)obj.get("property2");
        assertEquals(Long.valueOf(1), value2);

        String value3 = (String)obj.get("property3");
        assertEquals("value3", value3);

        docStore.remove(Collection.NODES, "/");
        obj = docStore.find(Collection.NODES, "/");
        assertTrue(obj == null);
    }

    @Test
    @Ignore
    public void addLotsOfNodes() throws Exception {
        char[] nPrefix = new char[]{'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'};
        int nNodes = 2000;

        for (int nThreads = 1; nThreads < 32; nThreads = nThreads * 2) {
            DocumentStore docStore = new MongoDocumentStore(mongoConnection.getDB());

            System.out.println("Adding and updating " + nNodes + " nodes in each " + nThreads + " threads");
            long start = System.currentTimeMillis();

            ExecutorService executor = Executors.newFixedThreadPool(nThreads);
            for (int j = 0; j < nThreads; j++) {
                executor.submit(new AddAndUpdateNodesTask(docStore, "node" + nPrefix[j], nNodes));
            }
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.MINUTES);

            long end = System.currentTimeMillis();
            System.out.println("Done: " + (end - start) + "ms");
            dropCollections(mongoConnection.getDB());
        }
    }

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
                UpdateOp updateOp = new UpdateOp("/" + nodeName + i);
                updateOp.addMapEntry("property1", "key1", "value1");
                updateOp.set("property3", "value3");
                docStore.createOrUpdate(Collection.NODES, updateOp);
            }
        }

        private void updateNodes() {
            for (int i = 0; i < nNodes; i++) {
                UpdateOp updateOp = new UpdateOp("/" + nodeName + i);
                updateOp.addMapEntry("property1", "key2", "value2");
                updateOp.set("property4", "value4");
                docStore.createOrUpdate(Collection.NODES, updateOp);
            }
        }
    }
}