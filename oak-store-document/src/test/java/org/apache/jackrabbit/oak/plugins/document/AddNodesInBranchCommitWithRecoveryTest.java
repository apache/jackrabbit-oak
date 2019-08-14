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

import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AddNodesInBranchCommitWithRecoveryTest {

    private static final Logger LOG = LoggerFactory.getLogger(AddNodesInBranchCommitWithRecoveryTest.class);

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

    // OAK-8538
    @Test
    public void idleAfterNodesAdded() throws Exception {
        performTest(new Callback() {
            @Override
            public void call(DocumentNodeStore ns) throws Exception {
                // delay the commit
                clock.waitUntil(clock.getTime() + TimeUnit.MINUTES.toMillis(1));

                // run background operations, this will trigger a refresh of the head
                // revision. the behaviour was introduced with OAK-3712 and refined
                // with OAK-8466.
                ns.runBackgroundOperations();
                clock.waitUntil(clock.getTime() + TimeUnit.MINUTES.toMillis(1));
                ns.runBackgroundOperations();
                logWithClockTime("Ran background operations");
            }
        });
    }

    // OAK-8538
    @Test
    public void trunkCommitAfterNodesAdded() throws Exception {
        performTest(new Callback() {
            @Override
            public void call(DocumentNodeStore ns) throws Exception {
                // delay the commit
                clock.waitUntil(clock.getTime() + TimeUnit.MINUTES.toMillis(1));
                ns.runBackgroundOperations();

                // perform some other change
                NodeBuilder builder = ns.getRoot().builder();
                builder.child("foo");
                merge(ns, builder);

                ns.runBackgroundOperations();
            }
        });
    }

    private void performTest(Callback afterNodesAdded) throws Exception {
        int numTestNodes = 100;
        FailingDocumentStore store = new FailingDocumentStore(new MemoryDocumentStore());
        DocumentNodeStore ns = builderProvider.newBuilder()
                .setDocumentStore(store).setAsyncDelay(0).clock(clock)
                .setLeaseCheckMode(LeaseCheckMode.DISABLED)
                .setUpdateLimit(20).build();

        NodeBuilder builder = ns.getRoot().builder();
        NodeBuilder t = builder.child("test").child("tree");
        for (int i = 0; i < numTestNodes; i++) {
            t.child("n-" + i).child("child");
        }
        merge(ns, builder);
        logWithClockTime("Created initial nodes");
        ns.runBackgroundOperations();

        builder = ns.getRoot().builder();
        t = builder.child("test").child("tree");
        for (int i = 0; i < numTestNodes; i++) {
            t.child("n-" + i).child("child").setProperty("p", "v");
        }
        logWithClockTime("Prepared nodes");

        afterNodesAdded.call(ns);

        // now merge and simulate a crash
        merge(ns, builder);
        logWithClockTime("Merged nodes");
        store.fail().after(0).eternally();

        try {
            ns.dispose();
            fail("Dispose must fail with exception");
        } catch (Exception e) {
            // expected
        }

        store.fail().never();
        // fast forward two minutes to let lease expire
        clock.waitUntil(clock.getTime() + TimeUnit.MINUTES.toMillis(2));

        ns = builderProvider.newBuilder()
                .setDocumentStore(store).setAsyncDelay(0).clock(clock)
                .setUpdateLimit(20).build();
        NodeState tree = ns.getRoot().getChildNode("test").getChildNode("tree");
        for (int i = 0; i < numTestNodes; i++) {
            NodeState c = tree.getChildNode("n-" + i).getChildNode("child");
            String path = "/test/tree/n-" + i + "/child";
            assertTrue("Node at " + path + " does not exist", c.exists());
            assertTrue("Node at " + path + " does not have a property 'p'", c.hasProperty("p"));
        }
    }

    interface Callback {
        void call(DocumentNodeStore ns) throws Exception;
    }

    private void logWithClockTime(String message) {
        LOG.info("{} {}", Utils.timestampToString(clock.getTime()), message);
    }
}
