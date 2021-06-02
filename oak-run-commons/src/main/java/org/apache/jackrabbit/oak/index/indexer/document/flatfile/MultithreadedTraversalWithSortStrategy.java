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
import org.apache.jackrabbit.oak.index.indexer.document.LastModifiedRange;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverser;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverserFactory;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Phaser;
import java.util.function.Function;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.createWriter;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.getSortedStoreFileName;

public class MultithreadedTraversalWithSortStrategy implements SortStrategy {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Charset charset = UTF_8;
    private final boolean compressionEnabled;
    private final File storeDir;
    private final Comparator<NodeStateHolder> comparator;
    private final ConcurrentLinkedQueue<File> sortedFiles;
    private final ConcurrentLinkedQueue<String> completedTasks;
    private final BlockingQueue<Callable<List<File>>> taskQueue;
    private final Phaser phaser;
    private final BlobStore blobStore;
    private static final Callable<List<File>> POISON_PILL = () -> null;

    private enum Phases {
        WAITING_FOR_TASK_SPLITS(0),
        WAITING_FOR_RESULTS(1);

        private final int value;

        Phases(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    MultithreadedTraversalWithSortStrategy(NodeStateEntryTraverserFactory nodeStateEntryTraverserFactory,
                                           List<Long> lastModifiedBreakPoints, PathElementComparator pathComparator,
                                           BlobStore blobStore, File storeDir, File existingDataDumpDir,
                                           boolean compressionEnabled) {
        this.storeDir = storeDir;
        this.compressionEnabled = compressionEnabled;
        this.sortedFiles = new ConcurrentLinkedQueue<>();
        if (existingDataDumpDir != null) {
            //include all sorted files from an incomplete previous run
            for (File file : existingDataDumpDir.listFiles()) {
                log.info("Including existing sorted file {}", file.getName());
                sortedFiles.add(file);
            }
        }
        this.comparator = (e1, e2) -> pathComparator.compare(e1.getPathElements(), e2.getPathElements());
        this.blobStore = blobStore;
        taskQueue = new LinkedBlockingQueue<>();
        completedTasks = new ConcurrentLinkedQueue<>();
        phaser = new Phaser() {
            @Override
            protected boolean onAdvance(int phase, int registeredParties) {
                //terminate phaser only if it is advancing from last phase and there are no registered parties
                return phase == Phases.WAITING_FOR_RESULTS.value && registeredParties == 0;
            }
        };
        for (int i = 0; i < lastModifiedBreakPoints.size(); i++) {
            long start = lastModifiedBreakPoints.get(i);
            long end = i < lastModifiedBreakPoints.size() - 1 ? lastModifiedBreakPoints.get(i+1) : Long.MAX_VALUE;
            LastModifiedRange range = new LastModifiedRange(start, end);
            NodeStateEntryTraverser nodeStateEntryTraverser = nodeStateEntryTraverserFactory.create(range);
            taskQueue.add(new TraverseAndSortTask(nodeStateEntryTraverser, comparator,
                    blobStore, storeDir, compressionEnabled, completedTasks, taskQueue, phaser, nodeStateEntryTraverserFactory));
        }
    }

    @Override
    public File createSortedStoreFile() throws IOException {
        String watcherThreadName = "watcher";
        Thread watcher = new Thread(new TaskRunner(), watcherThreadName);
        watcher.start();
        phaser.awaitAdvance(Phases.WAITING_FOR_TASK_SPLITS.value);
        log.debug("All tasks completed. Signalling {} to proceed to result collection.", watcherThreadName);
        taskQueue.add(POISON_PILL);
        phaser.awaitAdvance(Phases.WAITING_FOR_RESULTS.value);
        log.debug("Result collection complete. Proceeding to merge.");
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
        List<File> inputSortedFilesToMerge = new ArrayList<>(sortedFiles);
        try(BufferedWriter writer = createWriter(sortedFile, compressionEnabled)) {
            Function<String, NodeStateHolder> func1 = (line) -> line == null ? null : new SimpleNodeStateHolder(line);
            Function<NodeStateHolder, String> func2 = holder -> holder == null ? null : holder.getLine();
            ExternalSort.mergeSortedFiles(inputSortedFilesToMerge,
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

    private class TaskRunner implements Runnable {

        private final ExecutorService executorService;
        private final int threadPoolSize = Integer.parseInt(System.getProperty("dataDumpThreadPoolSize", "4"));//Runtime.getRuntime().availableProcessors();

        public TaskRunner() {
            this.executorService = Executors.newFixedThreadPool(threadPoolSize);
        }

        @Override
        public void run() {
            try {
                log.info("Using a thread pool of size {}", threadPoolSize);
                List<Future<List<File>>> results = new ArrayList<>();
                while (true) {
                    Callable<List<File>> task = taskQueue.take();
                    if (task == POISON_PILL) {
                        log.debug("Won't wait for new tasks now.");
                        break;
                    }
                    results.add(executorService.submit(task));
                }
                log.debug("Registering to phaser and waiting for results now.");
                phaser.register();
                for (Future<List<File>> result : results) {
                    try {
                        sortedFiles.addAll(result.get());
                    } catch (InterruptedException|ExecutionException e) {
                        e.printStackTrace();
                    }
                }
                log.debug("Obtained all results. Arriving at phaser now.");
                phaser.arrive();
            } catch (InterruptedException e) {
                log.error("Could not complete task submissions", e);
            }
            executorService.shutdown();
        }
    }
}
