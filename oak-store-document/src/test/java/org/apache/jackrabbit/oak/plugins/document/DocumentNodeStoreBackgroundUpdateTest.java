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
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class DocumentNodeStoreBackgroundUpdateTest {

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private Clock clock;

    @Before
    public void before() throws Exception {
        clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        ClusterNodeInfo.setClock(clock);
    }

    @AfterClass
    public static void after() {
        Revision.resetClockToDefault();
        ClusterNodeInfo.resetClockToDefault();
    }

    @Test
    public void delayedRootDocumentUpdate() throws Throwable {
        final Lock defaultLock = new ReentrantLock();
        final Map<Thread, Lock> locks = new IdentityHashMap<>();

        DocumentStore store = new MemoryDocumentStore() {
            @Override
            public <T extends Document> T findAndUpdate(Collection<T> collection,
                                                        UpdateOp update) {
                if (!update.getId().equals(Utils.getIdFromPath(Path.ROOT))) {
                    return super.findAndUpdate(collection, update);
                }
                Lock lock = locks.getOrDefault(Thread.currentThread(), defaultLock);
                lock.lock();
                try {
                    return super.findAndUpdate(collection, update);
                } finally {
                    lock.unlock();
                }
            }
        };

        FailingDocumentStore failingStore = new FailingDocumentStore(store);

        int clusterId = 1;
        DocumentNodeStore ns = builderProvider.newBuilder().setAsyncDelay(0)
                .setClusterId(clusterId).clock(clock).setDocumentStore(failingStore)
                .build();
        NodeBuilder builder = ns.getRoot().builder();
        builder.child("node");
        merge(ns, builder);
        ns.runBackgroundOperations();

        builder = ns.getRoot().builder();
        builder.child("node").setProperty("p", "v");
        merge(ns, builder);

        List<Throwable> exceptions = new ArrayList<>();
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    try {
                        ns.runBackgroundOperations();
                        fail("background operations must fail because of lease failure");
                    } catch (Exception ex) {
                        assertThat(ex.getMessage(), containsString("concurrent update"));
                    }
                } catch (Throwable t) {
                    exceptions.add(t);
                }
            }
        });
        ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
        rwLock.writeLock().lock();
        locks.put(t, rwLock.readLock());
        t.start();

        while (rwLock.getQueueLength() == 0) {
            Thread.sleep(1);
        }

        // prevent further writes through the failingStore
        failingStore.fail().after(0).eternally();

        // wait until lease times out
        clock.waitUntil(clock.getTime() + TimeUnit.MINUTES.toMillis(2));

        // restart on non-failing store and immediately dispose to trigger recovery
        builderProvider.newBuilder().setAsyncDelay(0)
                .setClusterId(clusterId).clock(clock).setDocumentStore(store)
                .build().dispose();

        // let the background update finish
        rwLock.writeLock().unlock();

        t.join();

        NodeDocument root = failingStore.find(NODES, Utils.getIdFromPath(Path.ROOT));
        assertNotNull(root);
        ClusterNodeInfoDocument infoDoc = ClusterNodeInfoDocument.all(failingStore).get(0);
        Revision lastRev = root.getLastRev().get(clusterId);
        assertEquals(infoDoc.getLastWrittenRootRev(), lastRev.toString());

        for (Throwable ex : exceptions) {
            throw ex;
        }
    }
}
