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

import com.google.common.base.Stopwatch;
import org.apache.jackrabbit.oak.commons.sort.ExternalSort;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverser;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.createWriter;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.getSortedStoreFileName;

public class MultithreadedTraversalWithSort implements SortStrategy {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Charset charset = UTF_8;
    private final boolean compressionEnabled;
    private final File storeDir;
    private final List<File> sortedFiles;
    private final Comparator<NodeStateHolder> comparator;
    private final List<TraverseWithSortStrategy> traverseWithSortStrategies;
    private final ExecutorService executorService;
    private final int threadPoolSize = Runtime.getRuntime().availableProcessors();

    MultithreadedTraversalWithSort(List<NodeStateEntryTraverser> nodeStatesProviders, PathElementComparator pathComparator,
                                   BlobStore blobStore, File storeDir, File existingDataDumpDir,
                                   boolean compressionEnabled) {
        this.storeDir = storeDir;
        this.compressionEnabled = compressionEnabled;
        this.sortedFiles = new ArrayList<>();
        if (existingDataDumpDir != null) {
            //include all sorted files from an incomplete previous run
            for (File file : existingDataDumpDir.listFiles()) {
                log.info("Including existing sorted file {}", file.getName());
                sortedFiles.add(file);
            }
        }
        this.comparator = (e1, e2) -> pathComparator.compare(e1.getPathElements(), e2.getPathElements());
        traverseWithSortStrategies = new ArrayList<>();
        for (NodeStateEntryTraverser nodeStateEntryTraverser : nodeStatesProviders) {
            traverseWithSortStrategies.add(new TraverseWithSortStrategy("TWS-" + nodeStateEntryTraverser.getId(),
                    nodeStateEntryTraverser, comparator,
                    new NodeStateEntryWriter(blobStore), storeDir, compressionEnabled));
        }
        executorService = Executors.newFixedThreadPool(threadPoolSize);
    }

    @Override
    public File createSortedStoreFile() throws IOException {
        List<Future<List<File>>> results = new ArrayList<>();
        for (TraverseWithSortStrategy sortStrategy : traverseWithSortStrategies) {
            results.add(executorService.submit(sortStrategy));
        }
        for (Future<List<File>> result : results) {
            try {
                sortedFiles.addAll(result.get());
            } catch (InterruptedException|ExecutionException e) {
                e.printStackTrace();
            }
        }
        executorService.shutdown();
        return sortStoreFile();
    }

    @Override
    public long getEntryCount() {
        return 0;
    }

    private File sortStoreFile() throws IOException {
        log.info("Proceeding to perform merge of {} sorted files", sortedFiles.size());
        Stopwatch w = Stopwatch.createStarted();
        File sortedFile = new File(storeDir, getSortedStoreFileName(compressionEnabled));
        try(BufferedWriter writer = createWriter(sortedFile, compressionEnabled)) {
            Function<String, NodeStateHolder> func1 = (line) -> line == null ? null : new SimpleNodeStateHolder(line);
            Function<NodeStateHolder, String> func2 = holder -> holder == null ? null : holder.getLine();
            ExternalSort.mergeSortedFiles(sortedFiles,
                    writer,
                    comparator,
                    charset,
                    true, //distinct
                    compressionEnabled, //useZip
                    func2,
                    func1
            );
        }
        log.info("Merging of sorted files completed in {}", w);
        return sortedFile;
    }
}
