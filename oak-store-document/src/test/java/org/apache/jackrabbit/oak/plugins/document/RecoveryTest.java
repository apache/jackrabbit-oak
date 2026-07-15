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
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture.MemoryFixture;
import org.apache.jackrabbit.oak.plugins.migration.NodeStateTestUtils;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.TestUtils.disposeQuietly;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.persistToBranch;
import static org.apache.jackrabbit.oak.plugins.migration.NodeStateTestUtils.assertExists;
import static org.apache.jackrabbit.oak.plugins.migration.NodeStateTestUtils.assertMissing;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

public class RecoveryTest extends AbstractTwoNodeTest {

    private FailingDocumentStore fds1;

    private CountingDocumentStore cds2;

    public RecoveryTest(DocumentStoreFixture fixture) {
        super(fixture);
    }

    @Override
    protected DocumentStore customize(DocumentStore store) {
        DocumentStore ds;
        if (fds1 == null) {
            // wrap the first store with a FailingDocumentStore
            fds1 = new FailingDocumentStore(store);
            ds = fds1;
        } else {
            cds2 = new CountingDocumentStore(store);
            ds = cds2;
        }
        return ds;
    }

    @Test
    public void recoverOther() throws Exception {
        NodeBuilder builder = ds1.getRoot().builder();
        builder.child("node");
        builder.child("parent").child("test").child("c1");
        builder.child("parent").child("other").child("c1");
        merge(ds1, builder);
        ds1.runBackgroundOperations();
        ds2.runBackgroundOperations();
        builder = ds2.getRoot().builder();
        builder.child("node").setProperty("p", 1);
        builder.child("parent").child("test").child("c2");
        builder.child("parent").child("other").child("c2");
        merge(ds2, builder);
        ds2.runBackgroundOperations();
        ds1.runBackgroundOperations();

        waitOneMinute();
        ds1.runBackgroundOperations();
        ds2.runBackgroundOperations();
        ds1.renewClusterIdLease();
        ds2.renewClusterIdLease();

        // apply several changes without background update
        // and then simulate a killed process
        builder = ds1.getRoot().builder();
        builder.child("node").setProperty("p", 2);
        builder.child("parent").child("test").child("c1").remove();
        builder.child("parent").child("test").child("c3");
        merge(ds1, builder);

        builder = ds1.getRoot().builder();
        builder.child("parent").child("other").child("c3");
        merge(ds1, builder);

        builder = ds1.getRoot().builder();
        builder.child("node").setProperty("p", 3);
        merge(ds1, builder);

        builder = ds1.getRoot().builder();
        builder.child("node").child("wont-make-it");

        // simulate crashed process
        fds1.fail().after(1).eternally();
        try {
            merge(ds1, builder);
            fail("merge must fail");
        } catch (CommitFailedException e) {
            // expected
        }
        disposeQuietly(ds1);

        waitOneMinute();
        ds2.runBackgroundOperations();
        ds2.renewClusterIdLease();
        listChildren(ds2, "/");
        listChildren(ds2, "/parent");
        listChildren(ds2, "/parent/test");

        builder = ds2.getRoot().builder();
        builder.child("node").setProperty("q", 1);
        merge(ds2, builder);

        waitOneMinute();
        ds2.runBackgroundOperations();
        ds2.renewClusterIdLease();

        waitOneMinute();
        ds2.runBackgroundOperations();
        ds2.renewClusterIdLease();

        // before recovery, changes by ds1 are not visible
        NodeState root = ds2.getRoot();
        assertExists(root, "parent/test/c1");
        assertMissing(root, "parent/test/c3");
        assertMissing(root, "node/wont-make-it");

        // clusterId 1 lease expired
        assertTrue(ds2.getLastRevRecoveryAgent().isRecoveryNeeded());
        int numDocs = ds2.getLastRevRecoveryAgent().recover(1);
        assertThat(numDocs, equalTo(4));

        // still not visible because background read did not yet happen
        NodeState root1 = ds2.getRoot();
        assertExists(root1, "parent/test/c1");
        assertMissing(root1, "parent/test/c3");
        assertMissing(root1, "node/wont-make-it");

        ds2.runBackgroundOperations();
        // now changes must be visible
        NodeState root2 = ds2.getRoot();

        assertMissing(root2, "parent/test/c1");
        assertExists(root2, "parent/test/c3");
        assertMissing(root2, "node/wont-make-it");

        TrackingDiff diff = new TrackingDiff();
        root2.compareAgainstBaseState(root1, diff);
        assertThat(diff.modified, containsInAnyOrder("/parent", "/parent/other", "/parent/test", "/node"));
        assertThat(diff.added, containsInAnyOrder("/parent/test/c3", "/parent/other/c3"));
        assertThat(diff.deleted, containsInAnyOrder("/parent/test/c1"));
    }

    @Test
    public void recoverLargeBranch() throws Exception {
        // only run this on memory fixture, others take too long
        assumeTrue(fixture instanceof MemoryFixture);

        String nodePrefix = "long-node-name-with-many-characters-to-increase-the-size-of-the-journal";
        NodeBuilder builder = ds1.getRoot().builder();
        int numNodes = 0;
        for (int i = 0; i < 100; i++) {
            NodeBuilder child = builder.child(nodePrefix + i);
            numNodes++;
            for (int j = 0; j < 350; j++) {
                child.child(nodePrefix + j);
                if (numNodes++ % 100 == 0) {
                    // create branch commit every 100 nodes
                    persistToBranch(builder);
                }
            }
        }
        merge(ds1, builder);

        // simulate crash of ds1
        fds1.fail().after(1).eternally();
        disposeQuietly(ds1);

        // nodes must not be visible yet
        assertFalse(ds2.getRoot().hasChildNode(nodePrefix + 0));

        waitOneMinute();
        ds2.runBackgroundOperations();
        ds2.renewClusterIdLease();

        waitOneMinute();
        ds2.runBackgroundOperations();
        ds2.renewClusterIdLease();

        NodeState root1 = ds2.getRoot();

        // clusterId 1 lease expired
        assertTrue(ds2.getLastRevRecoveryAgent().isRecoveryNeeded());
        cds2.resetCounters();
        int numDocs = ds2.getLastRevRecoveryAgent().recover(1);
        assertThat(numDocs, equalTo(101));
        // recovery must create three journal entries
        assertThat(cds2.getNumCreateOrUpdateCalls(Collection.JOURNAL), equalTo(3));

        ds2.runBackgroundOperations();
        // check some random nodes are present after recovery
        Random rand = new Random();
        for (int i = 0; i < 100; i++) {
            NodeState node = ds2.getRoot()
                    .getChildNode(nodePrefix + rand.nextInt(100))
                    .getChildNode(nodePrefix + rand.nextInt(350));
            assertTrue(node.exists());
        }

        NodeState root2 = ds2.getRoot();
        TrackingDiff diff = new TrackingDiff();
        root2.compareAgainstBaseState(root1, diff);
        assertThat(diff.added.size(), equalTo(35100));
    }

    private void waitOneMinute() throws Exception {
        clock.waitUntil(clock.getTime() + TimeUnit.MINUTES.toMillis(1));
    }

    private static void listChildren(NodeStore ns, String path) {
        NodeStateTestUtils.getNodeState(ns.getRoot(), path).getChildNodeEntries().forEach(ChildNodeEntry::getNodeState);
    }
}
