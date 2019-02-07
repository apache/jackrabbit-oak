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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.Clock;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DocumentNodeStoreStatsCollectorIT {
    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private DocumentNodeStoreStatsCollector statsCollector = mock(DocumentNodeStoreStatsCollector.class);

    private Clock clock;

    private DocumentStore store;

    private DocumentNodeStore nodeStore;

    @Before
    public void setUp() throws Exception {
        clock = new Clock.Virtual();
        clock.waitUntil(System.currentTimeMillis());
        Revision.setClock(clock);
        ClusterNodeInfo.setClock(clock);
        store = new MemoryDocumentStore();
        nodeStore = builderProvider.newBuilder()
                .clock(clock)
                .setDocumentStore(store)
                .setAsyncDelay(0)
                .setNodeStoreStatsCollector(statsCollector)
                .setUpdateLimit(10)
                .getNodeStore();
        // do not retry failed merges
        nodeStore.setMaxBackOffMillis(0);
    }

    @AfterClass
    public static void reset() {
        Revision.resetClockToDefault();
        ClusterNodeInfo.resetClockToDefault();
    }

    @Test
    public void doneBackgroundRead() {
        nodeStore.runBackgroundReadOperations();
        verify(statsCollector).doneBackgroundRead(any(BackgroundReadStats.class));
    }

    @Test
    public void doneBackgroundUpdate() {
        nodeStore.runBackgroundUpdateOperations();
        verify(statsCollector).doneBackgroundUpdate(any(BackgroundWriteStats.class));
    }

    @Test
    public void doneMerge() throws Exception {
        NodeBuilder nb = nodeStore.getRoot().builder();
        nb.child("a");
        nb.child("b");
        nb.child("c");
        nodeStore.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        verify(statsCollector).doneMerge(eq(3), eq(0), anyLong(), eq(0L), eq(false));
    }

    @Test
    public void failedMerge() {
        CommitHook failingHook = new CommitHook() {
            @Override
            public NodeState processCommit(NodeState before, NodeState after,
                                           CommitInfo info) throws CommitFailedException {

                throw new CommitFailedException(CommitFailedException.MERGE, 0 , "");

            }
        };

        NodeBuilder nb1 = nodeStore.getRoot().builder();
        nb1.child("a");

        try {
            nodeStore.merge(nb1, failingHook, CommitInfo.EMPTY);
            fail();
        } catch (CommitFailedException ignore){

        }

        verify(statsCollector).failedMerge(anyInt(), anyLong(), eq(0L), eq(false));

        //Should be called once more with exclusive lock
        verify(statsCollector).failedMerge(anyInt(), anyLong(), eq(0L), eq(true));
    }

    @Test
    public void branchCommit() throws Exception {
        int updateLimit = nodeStore.getUpdateLimit();
        NodeBuilder nb = nodeStore.getRoot().builder();
        for (int i = 0; i < updateLimit; i++) {
            nb.child("node-" + i).setProperty("p", "v");
        }
        nb.child("foo");
        nb.child("bar");
        merge(nodeStore, nb);

        verify(statsCollector, times(2)).doneBranchCommit();
        verify(statsCollector).doneMergeBranch(eq(2), eq(updateLimit + 2));
    }

    @Test
    public void leaseUpdate() throws Exception {
        clock.waitUntil(clock.getTime() + ClusterNodeInfo.DEFAULT_LEASE_UPDATE_INTERVAL_MILLIS * 2);
        assertTrue(nodeStore.renewClusterIdLease());
        verify(statsCollector).doneLeaseUpdate(anyLong());
    }

    @Test
    public void externalChangesLag() throws Exception {
        // start a second cluster node
        DocumentNodeStore ns2 = builderProvider.newBuilder()
                .setDocumentStore(store)
                .clock(clock)
                .setClusterId(2)
                .setAsyncDelay(0)
                .getNodeStore();
        ns2.runBackgroundOperations();
        nodeStore.runBackgroundOperations();
        
        NodeBuilder nb = ns2.getRoot().builder();
        nb.child("test");
        merge(ns2, nb);
        ns2.runBackgroundOperations();

        Mockito.reset(statsCollector);

        long lag = 2000;
        // wait two seconds
        clock.waitUntil(clock.getTime() + lag);
        // then run background read
        nodeStore.runBackgroundReadOperations();

        verify(statsCollector).doneBackgroundRead(argThat(
                // a bit more than 2000 ms
                stats -> stats.externalChangesLag > lag
                        && stats.externalChangesLag - lag < 100
        ));
    }

    @Test
    public void waitUntilHead() throws Exception {
        NodeBuilder nb = nodeStore.getRoot().builder();
        nb.child("a");
        nodeStore.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        verify(statsCollector).doneWaitUntilHead(eq(0L));
    }

    @Test
    public void mergeLockAcquired() throws Exception {
        NodeBuilder nb = nodeStore.getRoot().builder();
        nb.child("a");
        nodeStore.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        verify(statsCollector).doneMergeLockAcquired(anyLong());
    }

    @Test
    public void commitHookProcessed() throws Exception {
        NodeBuilder nb = nodeStore.getRoot().builder();
        nb.child("a");
        nodeStore.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        verify(statsCollector).doneCommitHookProcessed(anyLong());
    }

    @Test
    public void commitHookProcessedOnBranch() throws Exception {
        NodeBuilder nb = nodeStore.getRoot().builder();
        nb.child("a");
        TestUtils.persistToBranch(nb);
        nb.child("b");
        nodeStore.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        verify(statsCollector).doneCommitHookProcessed(anyLong());
    }

    @Test
    public void changesApplied() throws Exception {
        Mockito.reset(statsCollector);
        NodeBuilder nb = nodeStore.getRoot().builder();
        nb.child("a");
        nodeStore.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        verify(statsCollector).doneChangesApplied(anyLong());
    }
}