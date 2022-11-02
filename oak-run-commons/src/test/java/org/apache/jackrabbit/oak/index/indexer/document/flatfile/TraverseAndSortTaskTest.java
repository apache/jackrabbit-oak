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

package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.index.indexer.document.LastModifiedRange;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverser;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverserFactory;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Phaser;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.MultithreadedTraverseWithSortStrategy.DirectoryHelper;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentTraverser.TraversingRange;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class TraverseAndSortTaskTest {

    private static class NodeStateEntryTraverserFactoryImpl implements NodeStateEntryTraverserFactory {

        @Override
        public NodeStateEntryTraverser create(TraversingRange range) {
            return new NodeStateEntryTraverser("Test-NSET", null, null, null, range);
        }
    }

    @Test
    public void taskSplit() throws IOException {

        LastModifiedRange lmRange = new LastModifiedRange(0, 10);
        TraversingRange traversingRange = new TraversingRange(lmRange, null);

        Phaser phaser = Mockito.mock(Phaser.class);
        when(phaser.register()).thenReturn(1);

        MemoryManager mockMemManager = Mockito.mock(MemoryManager.class);
        when(mockMemManager.isMemoryLow()).thenReturn(false);

        Queue<Callable<List<File>>> newTaskQueue = new LinkedList<>();
        File store = new File("target/" + this.getClass().getSimpleName() + "-" + System.currentTimeMillis());
        TraverseAndSortTask tst = new TraverseAndSortTask(traversingRange, null, null, store, Compression.GZIP,
                new LinkedList<>(Collections.singletonList("1")), newTaskQueue, phaser, new NodeStateEntryTraverserFactoryImpl(), mockMemManager,
                FlatFileNodeStoreBuilder.OAK_INDEXER_DUMP_THRESHOLD_IN_MB_DEFAULT * FileUtils.ONE_MB, new LinkedBlockingQueue<File>(), path -> true);

        NodeStateEntry mockEntry = Mockito.mock(NodeStateEntry.class);
        long lastModified = (lmRange.getLastModifiedFrom() + lmRange.getLastModifiedTo())/2;
        when(mockEntry.getLastModified()).thenReturn(lastModified);
        when(mockEntry.getPath()).thenReturn("/content");
        when(mockEntry.getNodeState()).thenReturn(EmptyNodeState.EMPTY_NODE);
        assertEquals(lmRange.getLastModifiedTo(), DirectoryHelper.getLastModifiedUpperLimit(tst.getSortWorkDir()));
        tst.addEntry(mockEntry);
        long newUpperLimit = DirectoryHelper.getLastModifiedUpperLimit(tst.getSortWorkDir());
        assertTrue(newUpperLimit > lastModified);
        assertTrue(newUpperLimit < lmRange.getLastModifiedTo());
        assertEquals(1, newTaskQueue.size());
        TraverseAndSortTask tst2 = (TraverseAndSortTask)newTaskQueue.remove();
        assertEquals(newUpperLimit, tst2.getLastModifiedLowerBound());
        assertEquals(lmRange.getLastModifiedTo(), tst2.getLastModifiedUpperBound());
    }

}
