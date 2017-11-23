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

import com.google.common.collect.Iterables;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.Collection.NODES;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.getIdFromPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class LastRevRecoveryAgentTest extends AbstractTwoNodeTest {

    public LastRevRecoveryAgentTest(DocumentStoreFixture fixture) {
        super(fixture);
    }

    @Test
    public void testIsRecoveryRequired() throws Exception{
        //1. Create base structure /x/y
        NodeBuilder b1 = ds1.getRoot().builder();
        b1.child("x").child("y");
        ds1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        ds1.runBackgroundOperations();

        ds2.runBackgroundOperations();

        //2. Add a new node /x/y/z in C2
        NodeBuilder b2 = ds2.getRoot().builder();
        b2.child("x").child("y").child("z").setProperty("foo", "bar");
        ds2.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        Revision zlastRev2 = ds2.getHeadRevision().getRevision(ds2.getClusterId());

        long leaseTime = ds1.getClusterInfo().getLeaseTime();
        ds1.runBackgroundOperations();

        clock.waitUntil(clock.getTime() + leaseTime + 10);

        //Renew the lease for C1
        ds1.getClusterInfo().renewLease();

        assertTrue(ds1.getLastRevRecoveryAgent().isRecoveryNeeded());

        Iterable<Integer> cids = ds1.getLastRevRecoveryAgent().getRecoveryCandidateNodes();
        assertEquals(1, Iterables.size(cids));
        assertEquals(c2Id, Iterables.get(cids, 0).intValue());

        ds1.getLastRevRecoveryAgent().recover(Iterables.get(cids, 0));

        assertEquals(zlastRev2, getDocument(ds1, "/x/y").getLastRev().get(c2Id));
        assertEquals(zlastRev2, getDocument(ds1, "/x").getLastRev().get(c2Id));
        assertEquals(zlastRev2, getDocument(ds1, "/").getLastRev().get(c2Id));
    }

    //OAK-5337
    @Test
    public void testSelfRecovery() throws Exception{
        //1. Create base structure /x/y
        NodeBuilder b1 = ds1.getRoot().builder();
        b1.child("x").child("y");
        merge(ds1, b1);
        ds1.runBackgroundOperations();

        //2. Add a new node /x/y/z in C1
        b1 = ds1.getRoot().builder();
        b1.child("x").child("y").child("z");
        merge(ds1, b1);

        long leaseTime = ds1.getClusterInfo().getLeaseTime();

        clock.waitUntil(clock.getTime() + leaseTime + 10);

        //Renew the lease for C2
        ds2.getClusterInfo().renewLease();
        //C1 needs recovery from lease timeout pov
        assertTrue(ds1.getLastRevRecoveryAgent().isRecoveryNeeded());

        Iterable<Integer> cids = ds1.getLastRevRecoveryAgent().getRecoveryCandidateNodes();
        //.. but, it won't be returned while we iterate candidate nodes from self
        assertEquals(0, Iterables.size(cids));

        cids = ds2.getLastRevRecoveryAgent().getRecoveryCandidateNodes();
        //... checking that from other node still reports
        assertEquals(1, Iterables.size(cids));
        assertEquals(c1Id, Iterables.get(cids, 0).intValue());

        ds2.runBackgroundOperations();
        assertFalse(ds2.getRoot().getChildNode("x").getChildNode("y").hasChildNode("z"));

        // yet, calling recover with self-cluster-id still works (useful for startup LRRA)
        ds1.getLastRevRecoveryAgent().recover(Iterables.get(cids, 0));

        ds2.runBackgroundOperations();
        assertTrue(ds2.getRoot().getChildNode("x").getChildNode("y").hasChildNode("z"));
    }

    @Test
    public void testRepeatedRecovery() throws Exception {
        //1. Create base structure /x/y
        NodeBuilder b1 = ds1.getRoot().builder();
        b1.child("x").child("y");
        ds1.merge(b1, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        ds1.runBackgroundOperations();

        ds2.runBackgroundOperations();

        //2. Add a new node /x/y/z in C2
        NodeBuilder b2 = ds2.getRoot().builder();
        b2.child("x").child("y").child("z").setProperty("foo", "bar");
        ds2.merge(b2, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        NodeDocument z1 = getDocument(ds1, "/x/y/z");
        Revision zlastRev2 = z1.getLastRev().get(c2Id);

        long leaseTime = ds1.getClusterInfo().getLeaseTime();
        ds1.runBackgroundOperations();

        clock.waitUntil(clock.getTime() + leaseTime + 10);

        //Renew the lease for C1
        ds1.getClusterInfo().renewLease();

        assertTrue(ds1.getLastRevRecoveryAgent().isRecoveryNeeded());
        ds1.getLastRevRecoveryAgent().performRecoveryIfNeeded();
        assertFalse(ds1.getLastRevRecoveryAgent().isRecoveryNeeded());
    }

    @Test
    public void recoveryOfModifiedDocument() throws Exception {
        // do not retry merges
        ds1.setMaxBackOffMillis(0);
        ds2.setMaxBackOffMillis(0);

        NodeBuilder b1 = ds1.getRoot().builder();
        b1.child("x").child("y").setProperty("p", "v1");
        merge(ds1, b1);

        ds1.runBackgroundOperations();
        ds2.runBackgroundOperations();

        NodeBuilder b2 = ds2.getRoot().builder();
        b2.child("x").child("y").setProperty("p", "v2");
        merge(ds2, b2);

        // simulate a crash of ds2
        long leaseTime = ds2.getClusterInfo().getLeaseTime();
        clock.waitUntil(clock.getTime() + leaseTime * 2);

        // this write will conflict because ds2 did not run
        // background ops after setting p=v2
        b1 = ds1.getRoot().builder();
        b1.child("x").child("y").setProperty("p", "v11");
        try {
            merge(ds1, b1);
            fail("CommitFailedException expected");
        } catch (CommitFailedException e) {
            // expected
        }

        ds1.getLastRevRecoveryAgent().recover(2);
        ds1.runBackgroundOperations();

        // now the write must succeed
        b1 = ds1.getRoot().builder();
        b1.child("x").child("y").setProperty("p", "v11");
        merge(ds1, b1);
    }

    private static NodeDocument getDocument(DocumentNodeStore nodeStore,
                                            String path) {
        return nodeStore.getDocumentStore().find(NODES, getIdFromPath(path));
    }

    private static void merge(DocumentNodeStore store, NodeBuilder builder)
            throws CommitFailedException {
        store.merge(builder, EmptyHook.INSTANCE, CommitInfo.EMPTY);
    }
}
