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
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.TestUtils.merge;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DocumentNodeStoreStatsCollectorIT {
    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private DocumentNodeStoreStatsCollector statsCollector = mock(DocumentNodeStoreStatsCollector.class);

    private DocumentNodeStore nodeStore;

    @Before
    public void setUp(){
        nodeStore = builderProvider.newBuilder()
                .setAsyncDelay(0)
                .setNodeStoreStatsCollector(statsCollector)
                .setUpdateLimit(10)
                .getNodeStore();
        // do not retry failed merges
        nodeStore.setMaxBackOffMillis(0);
    }

    @Test
    public void doneBackgroundRead() throws Exception {
        nodeStore.runBackgroundReadOperations();
        verify(statsCollector).doneBackgroundRead(any(BackgroundReadStats.class));
    }

    @Test
    public void doneBackgroundUpdate() throws Exception {
        nodeStore.runBackgroundUpdateOperations();
        verify(statsCollector).doneBackgroundUpdate(any(BackgroundWriteStats.class));
    }

    @Test
    public void doneMerge() throws Exception {
        NodeBuilder nb = nodeStore.getRoot().builder();
        nb.child("a");
        nodeStore.merge(nb, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        verify(statsCollector).doneMerge(eq(0), anyLong(), eq(false), eq(false));
    }

    @Test
    public void failedMerge() throws Exception {
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

        verify(statsCollector).failedMerge(anyInt(), anyLong(), eq(false), eq(false));

        //Should be called once more with exclusive lock
        verify(statsCollector).failedMerge(anyInt(), anyLong(), eq(false), eq(true));
    }

    @Test
    public void branchCommit() throws Exception {
        int updateLimit = nodeStore.getUpdateLimit();
        NodeBuilder nb = nodeStore.getRoot().builder();
        for (int i = 0; i < updateLimit; i++) {
            nb.child("node-" + i).setProperty("p", "v");
        }
        merge(nodeStore, nb);

        verify(statsCollector, times(2)).doneBranchCommit();
        verify(statsCollector).doneMergeBranch(2);
    }
}