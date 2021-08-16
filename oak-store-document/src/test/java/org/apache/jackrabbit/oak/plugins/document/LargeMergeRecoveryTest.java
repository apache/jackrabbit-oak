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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;
import org.junit.runners.Parameterized;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

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
        List<Object[]> fixtures = Lists.newArrayList();
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
     * Reproduces OAK-9535
     */
    @Test
    public void testLargeBranchMergeRecovery() throws Exception {
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
        System.out.println("Creating large branch merge, can take over a minute... (limit = " + DocumentNodeStoreBuilder.DEFAULT_UPDATE_LIMIT + ")");
        for (int i = 0; i < DocumentNodeStoreBuilder.DEFAULT_UPDATE_LIMIT * 3; i++) {
            test.child(childPrefix + i);
        }
        System.out.println("Done.");

        ds2.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

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

        System.out.println("RECOVER...");
        ds1.getLastRevRecoveryAgent().recover(Iterables.get(cids, 0));
        System.out.println("RECOVER DONE");

        assertEquals(zlastRev2, getDocument(ds1, "/x/y").getLastRev().get(c2Id));
        assertEquals(zlastRev2, getDocument(ds1, "/x").getLastRev().get(c2Id));
        assertEquals(zlastRev2, getDocument(ds1, "/").getLastRev().get(c2Id));

        // dispose ds2 quietly because it may now throw an exception
        disposeQuietly(ds2);
    }
}
