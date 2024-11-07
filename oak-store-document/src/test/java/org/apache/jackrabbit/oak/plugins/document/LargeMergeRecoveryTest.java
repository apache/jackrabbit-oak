/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.plugins.document;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.TestUtils.disposeQuietly;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import org.apache.jackrabbit.guava.common.collect.Iterables;

public class LargeMergeRecoveryTest extends AbstractTwoNodeTest {

    public LargeMergeRecoveryTest(DocumentStoreFixture fixture) {
        super(fixture);
    }

    private static NodeDocument getDocument(DocumentNodeStore nodeStore,
                                            String path) {
        return nodeStore.getDocumentStore().find(NODES, getIdFromPath(path));
    }

    @Parameterized.Parameters(name = "{0}")
    public static java.util.Collection<Object[]> fixtures() throws IOException {
        List<Object[]> fixtures = new ArrayList<>();
        // disabling MemoryFixture, as that runs into an OutOfMemoryError
//        fixtures.add(new Object[] {new DocumentStoreFixture.MemoryFixture()});

        DocumentStoreFixture rdb = new DocumentStoreFixture.RDBFixture("RDB-H2(file)", "jdbc:h2:file:./target/ds-test", "sa", "");
        if (rdb.isAvailable()) {
            fixtures.add(new Object[] { rdb });
        }

        DocumentStoreFixture mongo = new DocumentStoreFixture.MongoFixture();
        if (mongo.isAvailable()) {
            fixtures.add(new Object[] { mongo });
        }
        return fixtures;
    }

    /**
     * Does 1 large, and 2 minor normal commits, that all require recovery
     * Reproduces OAK-9535
     */
    @Test
    @Ignore(value = "slow test, we have one that reproduces OAK-9535 enabled, so this one is not ran by default")
    public void testMixedLargeBranchMergeRecovery() throws Exception {
        doTestMixedLargeBranchMergeRecovery(DocumentNodeStoreBuilder.DEFAULT_UPDATE_LIMIT);
    }

    /**
     * Does 1 large, and 2 minor normal commits, that all require recovery
     * Depending on actual update.limit it might or might not reproduce OAK-9535
     * (update limit must be 100'000 in order to reproduce the bug - but that
     * results in a long test duration)
     */
    @Test
    public void testMixedSmallBranchMergeRecovery() throws Exception {
        doTestMixedLargeBranchMergeRecovery(DocumentNodeStoreBuilder.UPDATE_LIMIT);
    }

    void doTestMixedLargeBranchMergeRecovery(int updateLimit) throws Exception {
        // 1. Create base structure /x/y
        NodeBuilder b1 = ds1.getRoot().builder();
        b1.child("x").child("y");
        ds1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        ds1.runBackgroundOperations();
        ds2.runBackgroundOperations();

        // 2. create a branch in C2
        final String childPrefix = "childWithMediumLengthJavaContentRepositoryNodeNameInUTFdashEight-";
        NodeBuilder b2 = ds2.getRoot().builder();
        NodeBuilder test = b2.child("x").child("y");
        System.out.println(
                "Creating large branch merge, can take over a minute... (limit = " + updateLimit + ")");

        for (int i = 0; i < updateLimit * 3; i++) {
            test.child(childPrefix + i);
        }

        assertFalse(ds1.getRoot().getChildNode("x").getChildNode("y").hasChildNode(childPrefix + "0"));

        // before merging, make another merge on C2 - non-conflicting
        NodeBuilder b22 = ds2.getRoot().builder();
        b22.child("a").child("b1");
        ds2.merge(b22, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        // aand another one:
        b22 = ds2.getRoot().builder();
        b22.child("a").child("b2");
        ds2.merge(b22, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        assertFalse(ds1.getRoot().getChildNode("x").getChildNode("y").hasChildNode(childPrefix + "0"));
        ds1.runBackgroundOperations();
        assertFalse(ds1.getRoot().getChildNode("x").getChildNode("y").hasChildNode(childPrefix + "0"));

        ds2.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        long leaseTime = ds1.getClusterInfo().getLeaseTime();
        ds1.runBackgroundOperations();

        clock.waitUntil(clock.getTime() + leaseTime + 10);

        // Renew the lease for C1
        ds1.getClusterInfo().renewLease();

        assertTrue(ds1.getLastRevRecoveryAgent().isRecoveryNeeded());

        Iterable<Integer> cids = ds1.getLastRevRecoveryAgent()
                .getRecoveryCandidateNodes();
        assertEquals(1, Iterables.size(cids));
        assertEquals(c2Id, Iterables.get(cids, 0).intValue());

        assertFalse(ds1.getRoot().getChildNode("x").getChildNode("y").hasChildNode(childPrefix + "0"));
        assertFalse(ds1.getRoot().getChildNode("a").hasChildNode("b1"));
        assertFalse(ds1.getRoot().getChildNode("a").hasChildNode("b2"));

        System.out.println("RECOVER...");
        ds1.getLastRevRecoveryAgent().recover(Iterables.get(cids, 0));
        System.out.println("RECOVER DONE");

        ds1.runBackgroundOperations();

        assertTrue(ds1.getRoot().getChildNode("x").getChildNode("y").hasChildNode(childPrefix + "0"));
        assertTrue(ds1.getRoot().getChildNode("a").hasChildNode("b1"));
        assertTrue(ds1.getRoot().getChildNode("a").hasChildNode("b2"));

        // dispose ds2 quietly because it may now throw an exception
        disposeQuietly(ds2);
    }

    /**
     * Does 1 large branch commit that requires recovery
     * Reproduces OAK-9535
     */
    @Test
    @Ignore(value = "still fails on jenkins - disabling again temporarily")
    public void testOneLargeBranchMergeRecovery() throws Exception {
        if (!new DocumentStoreFixture.MongoFixture().getName().equals(fixture.getName())) {
            // only run this test for MongoDB, might help avoid OutOfMemoryError on travis
            System.out.println("Skipping testOneLargeBranchMergeRecovery with fixture " + fixture.getName());
            return;
        }
        doTestOneLargeBranchMergeRecovery(DocumentNodeStoreBuilder.DEFAULT_UPDATE_LIMIT);
    }

    /**
     * Does 1 large branch commit that requires recovery
     * Depending on actual update.limit it might or might not reproduce OAK-9535
     * (with default likely set to 100 for tests, this is a quick version to test just the logic)
     */
    @Test
    public void testOneSmallBranchMergeRecovery() throws Exception {
        doTestOneLargeBranchMergeRecovery(DocumentNodeStoreBuilder.UPDATE_LIMIT);
    }

    void doTestOneLargeBranchMergeRecovery(int updateLimit) throws Exception {
        // 1. Create base structure /x/y
        NodeBuilder b1 = ds1.getRoot().builder();
        b1.child("x").child("y");
        ds1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        ds1.runBackgroundOperations();
        ds2.runBackgroundOperations();

        final String childPrefix = "childWithMediumLengthJavaContentRepositoryNodeNameInUTFdashEight-";

        // 2. create a branch in C2
        NodeBuilder b2 = ds2.getRoot().builder();
        NodeBuilder test = b2.child("x").child("y");
        System.out.println("Creating large branch merge, can take over a minute... (limit = " + updateLimit + ")");
        for (int i = 0; i < updateLimit * 3; i++) {
            test.child(childPrefix + i);
        }
        System.out.println("Done. Merging...");

        ds2.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        System.out.println("Merged.");

        Revision zlastRev2 = ds2.getHeadRevision().getRevision(ds2.getClusterId());

        long leaseTime = ds1.getClusterInfo().getLeaseTime();
        ds1.runBackgroundOperations();

        clock.waitUntil(clock.getTime() + leaseTime + 10);

        // Renew the lease for C1
        ds1.getClusterInfo().renewLease();

        assertTrue(ds1.getLastRevRecoveryAgent().isRecoveryNeeded());

        Iterable<Integer> cids = ds1.getLastRevRecoveryAgent()
                .getRecoveryCandidateNodes();
        assertEquals(1, Iterables.size(cids));
        assertEquals(c2Id, Iterables.get(cids, 0).intValue());

        assertFalse(ds1.getRoot().getChildNode("x").getChildNode("y").hasChildNode(childPrefix + "0"));

        System.out.println("RECOVER...");
        ds1.getLastRevRecoveryAgent().recover(Iterables.get(cids, 0));
        System.out.println("RECOVER DONE");

        assertEquals(zlastRev2, getDocument(ds1, "/x/y").getLastRev().get(c2Id));
        assertEquals(zlastRev2, getDocument(ds1, "/x").getLastRev().get(c2Id));
        assertEquals(zlastRev2, getDocument(ds1, "/").getLastRev().get(c2Id));

        ds1.runBackgroundOperations();
        assertTrue(ds1.getRoot().getChildNode("x").getChildNode("y").hasChildNode(childPrefix + "0"));

        // dispose ds2 quietly because it may now throw an exception
        disposeQuietly(ds2);
    }
}
