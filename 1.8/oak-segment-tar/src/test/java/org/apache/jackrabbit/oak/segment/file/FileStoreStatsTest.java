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

package org.apache.jackrabbit.oak.segment.file;

import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FileStoreStatsTest {
    @Rule
    public final TemporaryFolder segmentFolder = new TemporaryFolder(new File("target"));

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    @After
    public void shutDown(){
        new ExecutorCloser(executor).close();
    }

    @Test
    public void initCall() throws Exception{
        FileStore store = mock(FileStore.class);
        StatisticsProvider statsProvider = new DefaultStatisticsProvider(executor);

        FileStoreStats stats = new FileStoreStats(statsProvider, store, 1000);
        assertEquals(1000, stats.getApproximateSize());

        stats.written(500);
        assertEquals(1500, stats.getApproximateSize());

        stats.reclaimed(250);
        assertEquals(1250, stats.getApproximateSize());

        assertEquals(1, stats.getTarFileCount());
    }

    @Test
    public void testJournalWriteStats() throws Exception {
        StatisticsProvider statsProvider = new DefaultStatisticsProvider(executor);
        FileStore fileStore = fileStoreBuilder(segmentFolder.newFolder()).withStatisticsProvider(statsProvider).build();
        FileStoreStats stats = new FileStoreStats(statsProvider, fileStore, 0);

        SegmentNodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();

        for (int i = 0; i < 10; i++) {
            NodeBuilder root = nodeStore.getRoot().builder();
            root.setProperty("count", i);
            nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

            fileStore.flush();
        }

        assertEquals(10, stats.getJournalWriteStatsAsCount());
    }
}
