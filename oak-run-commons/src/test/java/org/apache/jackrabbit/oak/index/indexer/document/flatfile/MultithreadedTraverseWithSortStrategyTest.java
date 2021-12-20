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

import org.apache.jackrabbit.oak.index.indexer.document.LastModifiedRange;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverserFactory;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentTraverser.TraversingRange;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.MultithreadedTraverseWithSortStrategy.DirectoryHelper;
import static org.junit.Assert.assertEquals;

public class MultithreadedTraverseWithSortStrategyTest {

    @Test
    public void initialRanges() throws IOException {
        List<Long> lastModifiedBreakpoints = Arrays.asList(10L, 20L, 30L, 40L);
        List<TraversingRange> ranges = new ArrayList<>();
        MultithreadedTraverseWithSortStrategy mtws = new MultithreadedTraverseWithSortStrategy(null,
                lastModifiedBreakpoints, null, null, null, null, true, null) {

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

    private void createSortWorkDirs(File workDir, List<Long> lastModifiedBreakpoints, Set<Integer> partiallyCompletedDirs,
                                    String startAfterDocID) throws IOException {
        for (int i = 0; i < lastModifiedBreakpoints.size(); i++) {
            long lastModLowerBound = lastModifiedBreakpoints.get(i);
            long lastModUpperBound = i < lastModifiedBreakpoints.size() - 1 ? lastModifiedBreakpoints.get(i+1) : lastModLowerBound + 1;
            File dir = DirectoryHelper.createdSortWorkDir(workDir, "test-" + i, lastModLowerBound, lastModUpperBound);
            if (partiallyCompletedDirs.contains(i)) {
                DirectoryHelper.markLastProcessedStatus(dir, startAfterDocID);
            }
        }
    }

    @Test
    public void rangesDuringResume() throws IOException {
        int previousRunsCount = 5;
        List<Long> lastModifiedBreakpoints = Arrays.asList(10L, 20L, 30L, 40L, 50L);
        Set<Integer> partiallyCompletedDirs = new HashSet<>(Arrays.asList(1, 3)); // should be numbers between 0 and lastModifiedBreakpoints.length() - 1
        String startAfterDocID = "1:/content";
        List<File> workDirs = new ArrayList<>();
        for (int i = 1; i <= previousRunsCount; i++) {
            File workDir = new File("target/" + this.getClass().getSimpleName() + i + "-" + System.currentTimeMillis());
            createSortWorkDirs(workDir, lastModifiedBreakpoints, i == 1 ? Collections.emptySet() : partiallyCompletedDirs, startAfterDocID);
            workDirs.add(workDir);
        }
        List<TraversingRange> ranges = new ArrayList<>();
        MultithreadedTraverseWithSortStrategy mtws = new MultithreadedTraverseWithSortStrategy(null,
                null, null, null, null, workDirs, true, null) {
            @Override
            void addTask(TraversingRange range, NodeStateEntryTraverserFactory nodeStateEntryTraverserFactory,
                         BlobStore blobStore, ConcurrentLinkedQueue<String> completedTasks) throws IOException {
                ranges.add(range);
            }
        };
        ranges.sort(Comparator.comparing(tr -> tr.getLastModifiedRange().getLastModifiedFrom()));
        assertEquals(lastModifiedBreakpoints.size(), ranges.size());
        for (int i = 0; i < lastModifiedBreakpoints.size(); i++) {
            long lm = lastModifiedBreakpoints.get(i);
            LastModifiedRange lmRange = new LastModifiedRange(lm, i < lastModifiedBreakpoints.size() - 1 ? lastModifiedBreakpoints.get(i+1) : lm+1);
            String docID = partiallyCompletedDirs.contains(i) ? startAfterDocID : null;
            assertEquals(ranges.get(i), new TraversingRange(lmRange, docID));
        }
    }

}
