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

import java.util.Random;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getAllDocuments;
import static org.junit.Assert.assertTrue;

public class RandomDocumentNodeStoreSweepTest {

    private static final Logger LOG = LoggerFactory.getLogger(RandomDocumentNodeStoreSweepTest.class);

    private Random random = new Random(42);

    private int numNodes = 0;

    private int numProperties = 0;

    private Clock clock;

    private FailingDocumentStore store;

    private DocumentNodeStore ns;

    @Before
    public void before() throws Exception {
        clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        ClusterNodeInfo.setClock(clock);
        store = new FailingDocumentStore(new MemoryDocumentStore());
        ns = createDocumentNodeStore();
    }

    @After
    public void after() {
        ns.dispose();
        Revision.resetClockToDefault();
        ClusterNodeInfo.resetClockToDefault();
    }

    @Test
    public void randomFailures() throws Exception {
        Random r = new Random(23);
        for (int i = 0; i < 1000; i++) {
            int n = r.nextInt(10);
            switch (n) {
                case 0:
                case 1:
                case 2:
                    addNode();
                    break;
                case 3:
                case 4:
                case 5:
                case 6:
                    addProperty();
                    break;
                case 7:
                    removeProperty();
                    break;
                case 8:
                    ns.runBackgroundOperations();
                    break;
                case 9:
                    restartAndCheckStore();
                    break;
            }
        }
    }

    private void restartAndCheckStore() throws InterruptedException {
        LOG.info("crashing DocumentNodeStore");
        // let it crash
        store.fail().after(0).eternally();
        try {
            ns.dispose();
        } catch (DocumentStoreException e) {
            // expected
        }
        store.fail().never();
        // wait until lease expires
        clock.waitUntil(clock.getTime() + ClusterNodeInfo.DEFAULT_LEASE_DURATION_MILLIS);
        LOG.info("restarting DocumentNodeStore");
        ns = createDocumentNodeStore();
        LOG.info("checking for uncommitted changes");
        assertCleanStore();
    }

    private void removeProperty() throws CommitFailedException {
        maybeFail(new Operation() {
            @Override
            public void call() throws CommitFailedException {
                if (numNodes == 0) {
                    return;
                }
                NodeBuilder builder = ns.getRoot().builder();
                // try ten times to find a node with a property
                for (int i = 0; i < 10; i++) {
                    String name = "node-" + random.nextInt(numNodes);
                    for (PropertyState p : builder.child(name).getProperties()) {
                        builder.child(name).removeProperty(p.getName());
                        merge(ns, builder);
                        return;
                    }
                }
            }
        }, "removing property");
    }

    private void addProperty() throws CommitFailedException {
        maybeFail(new Operation() {
            @Override
            public void call() throws CommitFailedException {
                if (numNodes == 0) {
                    return;
                }
                String name = "node-" + random.nextInt(numNodes);
                NodeBuilder builder = ns.getRoot().builder();
                builder.child(name).setProperty("property-" + numProperties, "value");
                merge(ns, builder);
                numProperties++;
            }
        }, "adding property");
    }

    private void addNode() throws CommitFailedException {
        maybeFail(new Operation() {
            @Override
            public void call() throws CommitFailedException {
                NodeBuilder builder = ns.getRoot().builder();
                builder.child("node-" + numNodes);
                merge(ns, builder);
                numNodes++;
            }
        }, "adding node");
    }



    private void maybeFail(Operation op, String message)
            throws CommitFailedException {
        if (failOperation()) {
            guardedFail(op, message);
        } else {
            op.call();
            LOG.info(message);
        }
    }

    private void guardedFail(Operation op, String message)
            throws CommitFailedException {
        store.fail().after(1).eternally();
        try {
            op.call();
            LOG.info(message);
        } catch (Exception e) {
            LOG.info(message + " failed");
        } finally {
            store.fail().never();
        }
    }

    private boolean failOperation() {
        // fail 10% of the operations
        return random.nextInt(10) == 0;
    }

    private DocumentNodeStore createDocumentNodeStore() {
        DocumentNodeStore ns = new DocumentMK.Builder().setDocumentStore(store)
                .clock(clock).setClusterId(1).setAsyncDelay(0)
                .setLeaseCheck(false).getNodeStore();
        // do not retry commits
        ns.setMaxBackOffMillis(0);
        return ns;
    }

    private void assertCleanStore() {
        for (NodeDocument doc : getAllDocuments(store)) {
            for (Revision c : doc.getAllChanges()) {
                String commitValue = ns.getCommitValue(c, doc);
                assertTrue("Revision " + c + " on " + doc.getId() + " is not committed: " + commitValue,
                        Utils.isCommitted(commitValue));
            }
        }
    }

    interface Operation {
        void call() throws CommitFailedException;
    }
}
