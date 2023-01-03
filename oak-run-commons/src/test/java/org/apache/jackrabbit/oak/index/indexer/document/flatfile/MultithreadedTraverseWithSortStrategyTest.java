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
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverserFactory;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.MultithreadedTraverseWithSortStrategy.DirectoryHelper;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentTraverser.TraversingRange;
import static org.junit.Assert.assertEquals;

public class MultithreadedTraverseWithSortStrategyTest {

    @Test
    public void initialRanges() throws IOException {
        List<Long> lastModifiedBreakpoints = Arrays.asList(10L, 20L, 30L, 40L);
        List<TraversingRange> ranges = new ArrayList<>();
        MultithreadedTraverseWithSortStrategy mtws = new MultithreadedTraverseWithSortStrategy(null,
                lastModifiedBreakpoints, null, null, null, null, Compression.GZIP, null,
                FlatFileNodeStoreBuilder.OAK_INDEXER_DUMP_THRESHOLD_IN_MB_DEFAULT * FileUtils.ONE_MB, path -> true) {

            @Override
            void addTask(TraversingRange range, NodeStateEntryTraverserFactory nodeStateEntryTraverserFactory, BlobStore blobStore, ConcurrentLinkedQueue<String> completedTasks) throws IOException {
                ranges.add(range);
            }
        };
        assertEquals(lastModifiedBreakpoints.size(), ranges.size());
        for (int i = 0; i < lastModifiedBreakpoints.size(); i++) {
            long lm = lastModifiedBreakpoints.get(i);
            LastModifiedRange lmRange = new LastModifiedRange(lm, i < lastModifiedBreakpoints.size() - 1 ? lastModifiedBreakpoints.get(i+1) : lm+1);
            assertEquals(ranges.get(i), new TraversingRange(lmRange, null));
        }
    }

    static class Execution {
        String taskID;
        long lastModLowerBound;
        long lastModUpperBound;
        long lastDownloadedLastMod;
        String lastDownloadedID;
        boolean completed;

        public Execution(String taskID, long lastModLowerBound, long lastModUpperBound, long lastDownloadedLastMod,
                         String lastDownloadedID, boolean completed) {
            this.taskID = taskID;
            this.lastModLowerBound = lastModLowerBound;
            this.lastModUpperBound = lastModUpperBound;
            this.lastDownloadedLastMod = lastDownloadedLastMod;
            this.lastDownloadedID = lastDownloadedID;
            this.completed = completed;
        }
    }

    private void createSortWorkDir(File workDir, Execution execution) throws IOException {
        File dir = DirectoryHelper.createdSortWorkDir(workDir, execution.taskID, execution.lastModLowerBound,
                execution.lastModUpperBound);
        if (execution.completed) {
            DirectoryHelper.markCompleted(dir);
        } else if (execution.lastDownloadedID != null) {
            DirectoryHelper.markLastProcessedStatus(dir, execution.lastDownloadedLastMod, execution.lastDownloadedID);
        }
    }

    @Test
    public void rangesDuringResume() throws IOException {
        List<Execution> previousRun = new ArrayList<Execution>() {{
            add(new Execution("1", 10, 20, -1, null, true));
            add(new Execution("2", 20, 30, 22, "1:/content", false));
            add(new Execution("3", 30, 40, 34, "2:/sites/mypage", false));
        }};
        List<File> workDirs = new ArrayList<>();
        File workDir = new File("target/" + this.getClass().getSimpleName() + "-" + System.currentTimeMillis());
        for (Execution execution : previousRun) {
            createSortWorkDir(workDir, execution);
        }
        workDirs.add(workDir);
        List<TraversingRange> ranges = new ArrayList<>();
        MultithreadedTraverseWithSortStrategy mtws = new MultithreadedTraverseWithSortStrategy(null,
                null, null, null, null, workDirs, Compression.GZIP, null,
                FlatFileNodeStoreBuilder.OAK_INDEXER_DUMP_THRESHOLD_IN_MB_DEFAULT * FileUtils.ONE_MB, path -> true) {
            @Override
            void addTask(TraversingRange range, NodeStateEntryTraverserFactory nodeStateEntryTraverserFactory,
                         BlobStore blobStore, ConcurrentLinkedQueue<String> completedTasks) throws IOException {
                ranges.add(range);
            }
        };
        ranges.sort(Comparator.comparing(tr -> tr.getLastModifiedRange().getLastModifiedFrom()));
        List<TraversingRange> expectedRanges = new ArrayList<TraversingRange>() {{
            add(new TraversingRange(new LastModifiedRange(22, 23), "1:/content"));
            add(new TraversingRange(new LastModifiedRange(23, 30), null));
            add(new TraversingRange(new LastModifiedRange(34, 35), "2:/sites/mypage"));
            add(new TraversingRange(new LastModifiedRange(35, 40), null));
        }};
        assertEquals(expectedRanges.size(), ranges.size());
        for (int i = 0; i < expectedRanges.size(); i++) {
            assertEquals(expectedRanges.get(i), ranges.get(i));
        }
    }

}
