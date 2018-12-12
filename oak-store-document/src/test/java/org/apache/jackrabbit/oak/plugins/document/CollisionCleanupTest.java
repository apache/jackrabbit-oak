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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.NodeDocument.COLLISIONS;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

// OAK-7956
@Ignore("OAK-7956")
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
        DocumentStore store = ns1.getDocumentStore();
        store.invalidateCache(NODES, id);
        NodeDocument doc = store.find(NODES, id);
        assertNotNull(doc);
        assertThat(doc.getValueMap(COLLISIONS).keySet(), empty());
    }

    private DocumentNodeStore newDocumentNodeStore(int clusterId) {
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setClusterId(clusterId).setAsyncDelay(0)
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
