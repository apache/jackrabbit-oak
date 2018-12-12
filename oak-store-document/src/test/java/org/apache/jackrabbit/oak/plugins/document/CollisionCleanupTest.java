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
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.COLLISIONS;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

// OAK-7956
public class CollisionCleanupTest {

    private static final Logger LOG = LoggerFactory.getLogger(CollisionCleanupTest.class);

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private DocumentStore store = new MemoryDocumentStore();
    private DocumentNodeStore ns1;
    private DocumentNodeStore ns2;

    private ExecutorService executor = Executors.newCachedThreadPool();

    @Before
    public void before() {
        ns1 = newDocumentNodeStore(1);
        ns2 = newDocumentNodeStore(2);
    }

    @After
    public void after() throws InterruptedException {
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
    }

    @Test
    public void conflictingUpdates() throws Exception {
        NodeBuilder builder = ns1.getRoot().builder();
        builder.child("test").setProperty("p", 0L, Type.LONG);
        merge(ns1, builder);
        ns1.runBackgroundOperations();
        ns2.runBackgroundOperations();

        List<Callable<Void>> tasks = new ArrayList<>();
        tasks.add(new Updater(ns1));
        tasks.add(new Updater(ns2));
        executor.invokeAll(tasks);

        String id = Utils.getIdFromPath("/test");
        ns1.addSplitCandidate(id);
        ns1.runBackgroundOperations();
        ns2.addSplitCandidate(id);
        ns2.runBackgroundOperations();

        DocumentStore store = ns1.getDocumentStore();
        NodeDocument doc = store.find(NODES, id);
        assertNotNull(doc);
        assertThat(doc.getValueMap(COLLISIONS).keySet(), empty());
    }

    @Test
    public void batchCleanup() throws Exception {
        Revision r = ns1.newRevision();
        NodeBuilder b1 = ns1.getRoot().builder();
        b1.child("test");
        merge(ns1, b1);

        String id = Utils.getIdFromPath("/test");
        Revision other = new Revision(r.getTimestamp(), r.getCounter(), r.getClusterId() + 1);
        // add lots of old collisions
        UpdateOp op = new UpdateOp(id, false);
        for (int i = 0; i < 5000; i++) {
            NodeDocument.addCollision(op, r, other);
            r = new Revision(r.getTimestamp() - 1, 0, r.getClusterId());
        }
        NodeDocument doc = ns1.getDocumentStore().findAndUpdate(NODES, op);
        assertNotNull(doc);

        for (int i = 1; i <= 5; i++) {
            // each background operation run will clean up 1000 collision entries
            ns1.addSplitCandidate(id);
            ns1.runBackgroundOperations();

            doc = ns1.getDocumentStore().find(NODES, id);
            assertNotNull(doc);
            assertEquals(5000 - i * 1000, doc.getLocalMap(COLLISIONS).size());
        }
    }

    @Test
    public void branchCollision() throws Exception {
        NodeBuilder builder = ns1.getRoot().builder();
        builder.child("conflict");
        // trigger a branch commit
        for (int i = 0; i < 200; i++) {
            builder.child("n-" + i).setProperty("p", "v");
        }

        // this one wins and will create a collision marker
        NodeBuilder b2 = ns1.getRoot().builder();
        b2.child("conflict");
        merge(ns1, b2);

        NodeDocument doc = Utils.getRootDocument(ns1.getDocumentStore());
        assertThat(doc.getLocalMap(COLLISIONS).keySet(), hasSize(1));

        // must not clean up marker
        ns1.addSplitCandidate(Utils.getIdFromPath("/"));
        ns1.runBackgroundOperations();

        doc = Utils.getRootDocument(ns1.getDocumentStore());
        assertThat(doc.getLocalMap(COLLISIONS).keySet(), hasSize(1));

        // must not be able to merge
        try {
            merge(ns1, builder);
            fail("CommitFailedException expected");
        } catch (CommitFailedException e) {
            // expected
        }
    }

    private DocumentNodeStore newDocumentNodeStore(int clusterId) {
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setClusterId(clusterId).setAsyncDelay(0)
                .setUpdateLimit(100)
                .setDocumentStore(store).build();
        // do not retry on conflicts
        ns.setMaxBackOffMillis(0);
        return ns;
    }

    private static final class Updater implements Callable<Void> {

        private final DocumentNodeStore ns;

        Updater(DocumentNodeStore ns) {
            this.ns = ns;
        }

        @Override
        public Void call() {
            for (int i = 0; i < 100; ) {
                try {
                    long value = increment();
                    i++;
                    LOG.info("NodeStore {} updated to {}", ns.getClusterId(), value);
                } catch (CommitFailedException e) {
                    // retry
                    LOG.info("Retrying update");
                }
                ns.runBackgroundOperations();
            }
            return null;
        }

        private long increment() throws CommitFailedException {
            NodeBuilder builder = ns.getRoot().builder();
            NodeBuilder test = builder.child("test");
            long v = test.getProperty("p").getValue(Type.LONG) + 1;
            test.setProperty("p", v);
            merge(ns, builder);
            return v;
        }
    }
}
