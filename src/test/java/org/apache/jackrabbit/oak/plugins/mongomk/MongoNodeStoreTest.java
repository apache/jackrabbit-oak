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

import java.util.concurrent.Semaphore;

import org.apache.jackrabbit.oak.plugins.mongomk.util.TimingDocumentStoreWrapper;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MongoNodeStoreTest {

    // OAK-1254
    @Ignore
    @Test
    public void backgroundRead() throws Exception {
        final Semaphore semaphore = new Semaphore(1);
        DocumentStore docStore = new MemoryDocumentStore();
        DocumentStore testStore = new TimingDocumentStoreWrapper(docStore) {
            @Override
            public void invalidateCache() {
                super.invalidateCache();
                semaphore.acquireUninterruptibly();
                semaphore.release();
            }
        };
        final MongoNodeStore store1 = new MongoMK.Builder().setAsyncDelay(0)
                .setDocumentStore(testStore).setClusterId(1).getNodeStore();
        MongoNodeStore store2 = new MongoMK.Builder().setAsyncDelay(0)
                .setDocumentStore(docStore).setClusterId(2).getNodeStore();

        NodeBuilder builder = store2.getRoot().builder();
        builder.child("node2");
        store2.merge(builder, EmptyHook.INSTANCE, null);
        // force update of _lastRevs
        store2.runBackgroundOperations();

        // at this point only node2 must not be visible
        assertFalse(store1.getRoot().hasChildNode("node2"));

        builder = store1.getRoot().builder();
        builder.child("node1");
        NodeState root = store1.merge(builder, EmptyHook.INSTANCE, null);

        semaphore.acquireUninterruptibly();
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                store1.runBackgroundOperations();
            }
        });
        t.start();
        // sleep until 'background thread' invalidated cache
        // and is waiting for semaphore
        while (!semaphore.hasQueuedThreads()) {
            Thread.sleep(10);
        }

        // must still not be visible at this state
        try {
            assertFalse(root.hasChildNode("node2"));
        } finally {
            semaphore.release();
        }
        t.join();
        // background operations completed
        root = store1.getRoot();
        // now node2 is visible
        assertTrue(root.hasChildNode("node2"));
    }
}
