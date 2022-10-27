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

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.comparator.SizeFileComparator;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.commons.sort.ExternalSort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.createWriter;


/**
 * Class responsible for -
 * <ol>
 *     <li>Watching {@link #sortedFiles} for new sorted files</li>
 *     <li>Submitting those files in batch to an {@link ExecutorService}</li>
 *     <li>Collecting the results (sorted files) created by those tasks</li>
 *     <li>Merge the result with any left over files to create a single sorted file</li>
 * </ol>
 * Strategy -
 * <ol>
 *      <li>Wait for n files</li>
 *      <li>construct new list of files to be merged by checking if its already merged</li>
 *    and create intermediate merge file
 *    (if final merge) merge all intermediate merge files and create sorted file
 *      <li>add all merged files to merged list</li>
 * </ol>
 *
 * <h3>Merge task explanation -</h3>
 *
 * SORTED_FILE_QUEUE=MultithreadedTraverseWithSortStrategy#sortedFiles
 * MERGED_FILE_LIST=MergeRunner#mergedFiles
 * UNMERGED_FILE_LIST=MergeRunner#unmergedFiles
 * <ol>
 *     <li>Have a BlockingQueue of sorted files (SORTED_FILE_QUEUE) that need to be executed for merge. Each of the task has been assigned a list of files.</li>
 *     <li>Task thread (TraverseAndSortTask) on completion adds sorted files to this queue</li>
 *     <li>Another monitoring thread (MultithreadedTraverseWithSortStrategy#MergeRunner) is consuming from this SORTED_FILE_QUEUE and submitting those
 *     part of the files in batch (batch file size is configurable via java system property {@link FlatFileNodeStoreBuilder#PROP_MERGE_TASK_BATCH_SIZE}
 *     to executor service for merge
 *          <ol>
 *              <li>The monitoring thread pulls any sorted file and add it in SORTED_FILE_QUEUE to the UNMERGED_FILE_LIST</li>
 *              <li>When UNMERGED_FILE_LIST grows larger than two times the batch merge size, a merge task is submitted for merge
 *              with the smaller half portion of the UNMERGED_FILE_LIST</li>
 *              <li>Files submitted for merge will be removed from UNMERGED_FILE_LIST and added to MERGED_FILE_LIST</li>
 *          </ol>
 *     </li>
 *     <li>A poison pill is added to SORTED_FILE_QUEUE upon download completion</li>
 *     <li>Once poison pill occurs, the monitoring thread stops submitting new merge task and proceed to final merging
 *          <ol>
 *              <li>Final merge waits for all existing tasks finish</li>
 *              <li>All files left in UNMERGED_FILE_LIST and all previously task results are collected to be merged</li>
 *          </ol>
 *     </li>
 *     <li>
 *         We use a phaser (Merge#mergePhaser) for coordination between main thread and the monitoring thread. This phaser has one phase -
 *         <ol>
 *             <li>Waiting for a single final merged file to be created</li>
 *         </ol>
 *     </li>
 *     <li>
 *         We use another phaser for coordination between monitoring thread (MergeRunner) and the merge task executor (MergeTask). This phaser has one phase -
 *         <ol>
 *             <li>Waiting for all merge tasks complete</li>
 *         </ol>
 *     </li>
 * </ol>
 *
 * <h3>Force Stop Explanation -</h3>
 * <ol>
 *     <li>On receiving MERGE_FORCE_STOP_POISON_PILL in SORTED_FILE_QUEUE from parent thread, all un-started tasks will be skipped.</li>
 *     <li>No more tasks will be created on MERGE_FORCE_STOP_POISON_PILL message is received.</li>
 *     <li>Running merge will continue to finish.</li>
 *     <li>Final merge will not be performed.</li>
 *     <li>This will not result in lose of data under conditions that -</li>
 *          <ol>
 *              <li>Files will not be removed until merge task is completed. Parent thread should add unmerged files to the SORTED_FILE_QUEUE on retry.</li>
 *              <li>Merged files will still be under the merge folder. Parent thread should add those files to the SORTED_FILE_QUEUE on retry.</li>
 *          </ol>
 *     </li>
 * </ol>
 */
public class MergeRunner implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(MergeRunner.class);
    private final Charset charset = UTF_8;
    private final Compression algorithm;
    private final ArrayList<File> mergedFiles = Lists.newArrayList();
    private final ArrayList<File> unmergedFiles = Lists.newArrayList();
    private ExecutorService executorService;
    private final int threadPoolSize;
    private final int batchMergeSize;
    private final Comparator<? super File> fileSizeComparator = new SizeFileComparator();

    /**
     * The end result file after merging all sorted files.
     */
    private final File sortedFile;

    /**
     * Directory where intermediate merged files will be created.
     */
    private final File mergeDir;

    /**
     * Comparator used for comparing node states for creating sorted files.
     */
    private final Comparator<NodeStateHolder> comparator;
    private final BlockingQueue<File> sortedFiles;
    private final ConcurrentLinkedQueue<Throwable> throwables;

    /**
     * Phaser used for coordination with the traverse/download and sort tasks. Advance of this phaser indicates that a single
     * merged and sorted file has been created.
     */
    private final Phaser phaser;

    /**
     * This poison pill is added to {@link #sortedFiles} to indicate that download phase has completed successfully and
     * merge should advance to the final merge stage.
     */
    public static final File MERGE_POISON_PILL = new File("");

    /**
     * This poison pill is added to {@link #sortedFiles} to indicate that download phase has failed and
     * merge should shut down immediately and not advance to the final merge stage.
     */
    public static final File MERGE_FORCE_STOP_POISON_PILL = new File("merge-force-stop-poison-pill");
    private final AtomicBoolean mergeCancelled;

    /**
     * Constructor.
     * @param sortedFiles thread safe list containing files to be merged.
     * @param mergeDir directory where sorted files will be created.
     * @param comparator comparator used to help with sorting of node state entries.
     * @param algorithm string representation of the compression algorithm, use "none" for disable compression.
     */
    MergeRunner(File sortedFile, BlockingQueue<File> sortedFiles, File mergeDir, Comparator<NodeStateHolder> comparator,
                Phaser phaser, int batchMergeSize, int threadPoolSize, Compression algorithm) {
        this.mergeDir = mergeDir;
        this.algorithm = algorithm;
        this.sortedFiles = sortedFiles;
        this.sortedFile = sortedFile;
        this.throwables = new ConcurrentLinkedQueue<>();
        this.comparator = comparator;
        this.phaser = phaser;
        this.batchMergeSize = batchMergeSize;
        this.threadPoolSize = threadPoolSize;
        this.mergeCancelled = new AtomicBoolean(false);
    }

    private boolean merge(List<File> files, File outputFile) {
        log.debug("performing merge for {} with size {} {}", outputFile.getName(), files.size(), files);
        try (BufferedWriter writer = createWriter(outputFile, algorithm)) {
            Function<String, NodeStateHolder> func1 = (line) -> line == null ? null : new SimpleNodeStateHolder(line);
            Function<NodeStateHolder, String> func2 = holder -> holder == null ? null : holder.getLine();
            ExternalSort.mergeSortedFiles(files,
                    writer,
                    comparator,
                    charset,
                    true, //distinct
                    algorithm,
                    func2,
                    func1
            );
        } catch (IOException e) {
            log.error("Merge failed with IOException", e);
            return false;
        }
        log.debug("merge complete for {} with {}", outputFile.getName(), files);
        return true;
    }

    private boolean finalMerge() {
        List<File> mergeTarget = new ArrayList<>();
        int count = 0;
        while(!unmergedFiles.isEmpty()) {
            count++;
            mergeTarget.clear();
            mergeTarget.addAll(getSmallestUnmergedFiles(batchMergeSize));
            markAsMerged(mergeTarget);
            File outputFile = new File(mergeDir, String.format("final-%s", count));
            if (unmergedFiles.isEmpty()) {
                outputFile = sortedFile;
            }

            log.info("running final batch merge task for {} with {}", outputFile.getName(), mergeTarget);
            if (!merge(mergeTarget, outputFile)) {
                return false;
            } else if (outputFile.equals(sortedFile)) {
                return true;
            }
            unmergedFiles.add(outputFile);
        }
        return false;
    }

    private List<File> getSmallestUnmergedFiles(int size) {
        ArrayList<File> result = new ArrayList<>(unmergedFiles);
        result.remove(MERGE_POISON_PILL);
        result.sort(fileSizeComparator);
        int endIdx = size > result.size() ? result.size() : size;
        return result.subList(0, endIdx);
    }

    private void markAsMerged(List<File> target) {
        mergedFiles.addAll(target);
        unmergedFiles.removeAll(target);
    }

    @Override
    public void run() {
        this.executorService = Executors.newFixedThreadPool(threadPoolSize);
        try {
            FileUtils.forceMkdir(mergeDir);
        } catch (IOException e) {
            log.error("failed to create merged directory {}", mergeDir.getAbsolutePath());
        }
        Phaser mergeTaskPhaser = new Phaser(1);
        List<Future<File>> results = Lists.newArrayList();
        int count = 0;

        while (true) {
            try {
                File f = sortedFiles.take();
                unmergedFiles.add(f);
                log.debug("added sorted file {} to the unmerged list", f.getName());
                if (f.equals(MERGE_POISON_PILL) || f.equals(MERGE_FORCE_STOP_POISON_PILL)) {
                    break;
                }
                // add another batchMergeSize so that we choose the smallest of a larger range
                if (unmergedFiles.size() >= 2*batchMergeSize) {
                    count++;
                    List<File> mergeTarget = getSmallestUnmergedFiles(batchMergeSize);
                    File intermediateMergeFile = new File(mergeDir, String.format("intermediate-%s", count));
                    Callable<File> mergeTask = new Task(mergeTarget, mergeTaskPhaser, intermediateMergeFile);
                    markAsMerged(mergeTarget);
                    results.add(executorService.submit(mergeTask));
                    log.info("created merge task for {} with {}", intermediateMergeFile.getName(), mergeTarget);
                }
            } catch (InterruptedException e) {
                log.error("Failed while draining from sortedFiles", e);
            }
        }
        log.info("Waiting for batch sorting tasks completion");

        // Parent thread signals to stop immediately
        if (unmergedFiles.contains(MERGE_FORCE_STOP_POISON_PILL)) {
            log.info("Merger receives force stop signal, shutting down all merge tasks");
            this.mergeCancelled.set(true);
            mergeTaskPhaser.arriveAndAwaitAdvance();
            executorService.shutdown();
            mergeTaskPhaser.arrive();
            phaser.arriveAndDeregister();
            return;
        }
        mergeTaskPhaser.arriveAndAwaitAdvance();
        executorService.shutdown();

        // final merge in batch 64
        sortedFiles.drainTo(unmergedFiles);
        unmergedFiles.remove(MERGE_POISON_PILL);
        log.info("There are still {} sorted files not merged yet", unmergedFiles.size());
        try {
            boolean exceptionsCaught = false;
            for (Future<File> result : results) {
                try {
                    unmergedFiles.add(result.get());
                } catch (Throwable e) {
                    throwables.add(e);
                    exceptionsCaught = true;
                }
            }
            log.debug("Completed merge result collection {}. Arriving at phaser now.", exceptionsCaught ? "partially" : "fully");
        } finally {
            mergeTaskPhaser.arrive();
        }

        finalMerge();
        log.info("Total batch sorted files length is {}", mergedFiles.size());

        phaser.arriveAndDeregister();
    }

    /**
     * Class responsible for merging sorted files
     */
    private class Task implements Callable<File> {
        private final Phaser mergeTaskPhaser;
        private final List<File> mergeTarget;
        private final File mergedFile;

        Task(List<File> mergeTarget, Phaser mergeTaskPhaser, File mergedFile) {
            this.mergeTarget = mergeTarget;
            this.mergeTaskPhaser = mergeTaskPhaser;
            this.mergedFile = mergedFile;
            mergeTaskPhaser.register();
        }


        @Override
        public File call() throws Exception {
            try {
                String mergedFileName = mergedFile.getName();
                if (mergeCancelled.get()) {
                    log.debug("merge cancelled, skipping merge task");
                    throw new EOFException("merge skipped for " + mergedFileName);
                } else if (merge(mergeTarget, mergedFile)) {
                    log.info("merge complete for {}", mergedFileName);
                } else {
                    log.error("merge failed for {}", mergedFileName);
                    throw new RuntimeException("merge failed for " + mergedFileName);
                }
            } finally {
                mergeTaskPhaser.arriveAndDeregister();
            }

            return mergedFile;
        }
    }
}
