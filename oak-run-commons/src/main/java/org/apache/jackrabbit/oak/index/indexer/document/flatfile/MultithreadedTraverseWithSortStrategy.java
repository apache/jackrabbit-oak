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
import org.apache.commons.io.FileUtils;
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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.stream.Stream;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.createWriter;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.getSortedStoreFileName;

/**
 * This class implements a sort strategy where node store is concurrently traversed for downloading node states by
 * multiple threads (number of threads is configurable via java system property <code>dataDumpThreadPoolSize</code>).
 * The traverse/download and sort tasks are submitted to an executor service. Each of those tasks create some sorted files which
 * are then merged (sorted) into one.
 *
 * <h3>Download task creation/splitting and result collection explanation -</h3>
 *
 * NEW_TASK_QUEUE=MultithreadedTraverseWithSortStrategy#taskQueue
 * FINISHED_QUEUE=TraverseAndSortTask#completedTasks
 * <ol>
 *     <li>Have a BlockingQueue of tasks (NEW_TASK_QUEUE) that need to be executed. Each of this task has been assigned a _modified range.</li>
 *     <li>Main thread (MultithreadedTraverseWithSortStrategy) adds an initial breakup of tasks (of type TraverseAndSortTask) to this queue.</li>
 *     <li>Another monitoring thread (MultithreadedTraverseWithSortStrategy#TaskRunner) is consuming from this NEW_TASK_QUEUE and submitting those
 *     tasks to executor service.</li>
 *     <li>The executor service is a fixed thread pool executor so that number of threads remains unchanged overtime.</li>
 *     <li>There is another queue for tasks that have finished execution - FINISHED_QUEUE. Each task adds itself or its ID to this queue when it
 *     finishes execution. This is ConcurrentLinkedQueue because running tasks monitor this queue for finished tasks but don't need to block if no
 *     finished task is there.
 *     </li>
 *     <li>Every running task monitors FINISHED_QUEUE as part of its execution cycle. If a running task has potential of splitting - identified by
 *     what range of _modified values is left to be examined and downloaded - and if there is a task in the FINISHED_QUEUE, the running task tries
 *     to dequeue the finished task.
 *     </li>
 *     <li>Only one of the running tasks would be able to dequeue a finished task. The running task which succeeds will reduce its upper limit of
 *     _modified range and submit a new task with the _modified range after that (till its original upper limit).
 *     </li>
 *     <li>The monitoring thread is reading from NEW_TASK_QUEUE (blocking queue), so it needs an indication about when to stop looking for new tasks
 *     from this queue. This is done by putting a poison pill entry in this queue. The main thread does it as explained below.
 *     </li>
 *     <li>
 *         We use a phaser for coordination between main thread, the running tasks and the monitoring thread. This phaser has two phases -
 *         <ol>
 *             <li>Waiting for new tasks (from task splits)</li>
 *             <li>Waiting for result collection</li>
 *         </ol>
 *     </li>
 *     <li>
 *         Each task registers itself (in phase 1 of phaser) inside its constructor itself. This ensures there is no created task which is not registered
 *         to a phaser. Also when the task finishes (either normally or due to exception), it arrives and deregisters (since it is not needed in phase 2 of phaser).
 *     </li>
 *     <li>
 *         Main thread after adding initial tasks to NEW_TASK_QUEUE, starts waiting for phase 1 to advance. Phase 1 would advance only if all the registered
 *         (and hence created) tasks have arrived. When all tasks arrive, it means we won't have any more new tasks from task splitting.
 *     </li>
 *     <li>
 *         So, once phase 1 advances, main thread adds the poison pill to NEW_TASK_QUEUE, reading which the monitoring thread advances to result collection.
 *         Before starting result collection, it registers itself to phaser (now in phase 2).
 *     </li>
 *     <li>
 *         Main thread after adding poison pill, starts waiting for phase 2 to advance. This advance happens when monitoring thread aggregates results from
 *         all tasks and adds it to data structure which main thread could access.
 *     </li>
 *     <li>After the advance of phase 2, the main thread returns the result - the list of sorted files which are then merged by the next step of indexing process.</li>
 * </ol>
 *
 */
public class MultithreadedTraverseWithSortStrategy implements SortStrategy {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Charset charset = UTF_8;
    private final boolean compressionEnabled;
    /**
     * Directory where sorted files will be created.
     */
    private final File storeDir;
    /**
     * Comparator used for comparing node states for creating sorted files.
     */
    private final Comparator<NodeStateHolder> comparator;
    private final ConcurrentLinkedQueue<File> sortedFiles;
    /**
     * Queue for traverse/download and sort tasks. After an initial creation of tasks, new tasks could be added to this queue
     * dynamically as already executing tasks decide to split up to provide more tasks for possibly idle threads.
     */
    private final BlockingQueue<Callable<List<File>>> taskQueue;
    /**
     * Phaser used for coordination with the traverse/download and sort tasks. All created tasks must register to this phaser
     * in the phase {@link Phases#WAITING_FOR_TASK_SPLITS}. If a task is created after this phase has advanced, its results
     * may get lost.
     */
    private final Phaser phaser;
    /**
     * This poison pill is added to {@link #taskQueue} to indicate that no more new tasks would be submitted to this queue.
     */
    private static final Callable<List<File>> POISON_PILL = () -> null;

    private final MemoryManager memoryManager;

    /**
     * Indicates the various phases of {@link #phaser}
     */
    private enum Phases {
        /**
         * This phase indicates we are in a state where new traverse/download and sort tasks could be created. After this
         * phase advances we don't expect any more new traverse/download and sort tasks to be created.
         */
        WAITING_FOR_TASK_SPLITS(0),
        /**
         * This phase indicates we are waiting for results from previously created tasks.
         */
        WAITING_FOR_RESULTS(1);

        private final int value;

        Phases(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    /**
     * Constructor.
     * @param nodeStateEntryTraverserFactory factory class for creating {@link NodeStateEntryTraverser}s.
     * @param lastModifiedBreakPoints list of last modified values. We create initial {@link NodeStateEntryTraverser}s based
     *                                on entries in this list. For every pair of valid indices (i, i+1) of this list, we create
     *                                a traverser whose lower limit is the last modified value at index i and upper limit is
     *                                the last modified value at index i+1. For the last entry of this list, we create a traverser
     *                                with lower limit equal to that value and upper limit equal to {@link Long#MAX_VALUE}. NOTE -
     *                                This parameter is only read when {@code existingDataDumpDir} parameter is null.
     * @param pathComparator comparator used to help with sorting of node state entries.
     * @param blobStore blob store
     * @param storeDir Directory where sorted files will be created.
     * @param existingDataDumpDir directory containing files from a previous incomplete run (which need to be merged with the results
     *                            from current run, if the current run has resumed from the point where previous run stopped). If this
     *                            is not null, the {@code lastModifiedBreakPoints} parameter is ignored.
     * @param compressionEnabled if true, the created files would be compressed
     */
    MultithreadedTraverseWithSortStrategy(NodeStateEntryTraverserFactory nodeStateEntryTraverserFactory,
                                          List<Long> lastModifiedBreakPoints, PathElementComparator pathComparator,
                                          BlobStore blobStore, File storeDir, File existingDataDumpDir,
                                          boolean compressionEnabled, MemoryManager memoryManager) throws IOException {
        this.storeDir = storeDir;
        this.compressionEnabled = compressionEnabled;
        this.sortedFiles = new ConcurrentLinkedQueue<>();
        this.comparator = (e1, e2) -> pathComparator.compare(e1.getPathElements(), e2.getPathElements());
        taskQueue = new LinkedBlockingQueue<>();
        phaser = new Phaser() {
            @Override
            protected boolean onAdvance(int phase, int registeredParties) {
                //terminate phaser only if it is advancing from last phase and there are no registered parties
                return phase == Phases.WAITING_FOR_RESULTS.value && registeredParties == 0;
            }
        };
        this.memoryManager = memoryManager;
        createInitialTasks(nodeStateEntryTraverserFactory, lastModifiedBreakPoints, blobStore, existingDataDumpDir);
    }

    private void createInitialTasks(NodeStateEntryTraverserFactory nodeStateEntryTraverserFactory,
                                    List<Long> lastModifiedBreakPoints, BlobStore blobStore, File existingDataDumpDir)
            throws IOException {
        ConcurrentLinkedQueue<String> completedTasks = new ConcurrentLinkedQueue<>();
        if (existingDataDumpDir != null) {
            List<LastModifiedRange> previousState = new ArrayList<>();
            //include all sorted files from an incomplete previous run
            for (File existingSortWorkDir : existingDataDumpDir.listFiles()) {
                if (!existingSortWorkDir.isDirectory()) {
                    log.info("Not a directory {}. Skipping it.", existingSortWorkDir.getAbsolutePath());
                    continue;
                }
                boolean downloadCompleted = DirectoryHelper.hasCompleted(existingSortWorkDir);
                if (!downloadCompleted) {
                    long start = DirectoryHelper.getLastModifiedTimeFromDirName(existingSortWorkDir);
                    long end = DirectoryHelper.getLastModifiedOfLastDownloadedDocument(existingSortWorkDir);
                    /*
                     Adding 1 to end since document with last modified equal to end was being worked upon and upper limit
                     in LastModifiedRange is exclusive. Also if end is -1, that means we didn't find any download updates
                     in this folder. So we create an empty range (lower limit = upper limit) and retry this folder from beginning.
                     */
                    previousState.add(new LastModifiedRange(start, end != -1 ? end + 1 : start));
                }
                log.info("Including existing sorted files from directory {} (hasCompleted={})",
                        existingSortWorkDir.getAbsolutePath(), downloadCompleted);
                DirectoryHelper.getDataFiles(existingSortWorkDir).forEach(file -> {
                    log.debug("Including existing sorted file {}", file.getName());
                    sortedFiles.add(file);
                });
            }
            resumeFromPreviousState(previousState, nodeStateEntryTraverserFactory, blobStore, completedTasks);
        } else {
            for (int i = 0; i < lastModifiedBreakPoints.size(); i++) {
                long start = lastModifiedBreakPoints.get(i);
                long end = i < lastModifiedBreakPoints.size() - 1 ? lastModifiedBreakPoints.get(i + 1) : Long.MAX_VALUE;
                addTask(start, end, nodeStateEntryTraverserFactory, blobStore, completedTasks);
            }
        }
    }

    private void resumeFromPreviousState(List<LastModifiedRange> previousState,
                                         NodeStateEntryTraverserFactory nodeStateEntryTraverserFactory, BlobStore blobStore,
                                         ConcurrentLinkedQueue<String> completedTasks) throws IOException {
        previousState.sort(Comparator.comparing(LastModifiedRange::getLastModifiedFrom));
        for (int i = 0; i < previousState.size(); i++) {
            LastModifiedRange currentRange = previousState.get(i);
            LastModifiedRange nextRange = i < previousState.size() - 1 ? previousState.get(i+1) : null;
            if (nextRange != null && currentRange.checkOverlap(nextRange)) {
                throw new IllegalStateException("Range overlap between " + currentRange + " and " + nextRange);
            }
            long start = currentRange.getLastModifiedTo() - 1;
            long end = nextRange != null ? nextRange.getLastModifiedFrom() : Long.MAX_VALUE;
            addTask(start, end, nodeStateEntryTraverserFactory, blobStore, completedTasks);
        }
    }


    private void addTask(long start, long end, NodeStateEntryTraverserFactory nodeStateEntryTraverserFactory, BlobStore blobStore,
                         ConcurrentLinkedQueue<String> completedTasks) throws IOException {
        LastModifiedRange range = new LastModifiedRange(start, end);
        NodeStateEntryTraverser nodeStateEntryTraverser = nodeStateEntryTraverserFactory.create(range);
        taskQueue.add(new TraverseAndSortTask(nodeStateEntryTraverser, comparator, blobStore, storeDir,
                compressionEnabled, completedTasks, taskQueue, phaser, nodeStateEntryTraverserFactory, memoryManager));
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

    /**
     * Class responsible for -
     * <ol>
     *     <li>Watching {@link #taskQueue} for new tasks</li>
     *     <li>Submitting those tasks to an {@link ExecutorService}</li>
     *     <li>Collecting the results (sorted files) created by those tasks into one place</li>
     * </ol>
     */
    private class TaskRunner implements Runnable {

        private final ExecutorService executorService;
        private static final String DEFAULT_NUMBER_OF_THREADS = "4";
        private static final String PROP_THREAD_POOL_SIZE = "oak.indexer.dataDumpThreadPoolSize";
        private final int threadPoolSize = Integer.parseInt(System.getProperty(PROP_THREAD_POOL_SIZE, DEFAULT_NUMBER_OF_THREADS));

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

    static class DirectoryHelper {

        private static final String PREFIX = "sort-work-dir-";
        private static final String LAST_MODIFIED_TIME_DELIMITER = "-from-";
        private static final String STATUS_FILE_NAME = "completion-status";
        private static final String COMPLETION_MARKER_FILE_NAME = "completed";
        private static final Logger log = LoggerFactory.getLogger(DirectoryHelper.class);

        static File createdSortWorkDir(File storeDir, String taskID, long lastModifiedLowerBound) throws IOException {
            File sortedFileDir = new File(storeDir, PREFIX + taskID + LAST_MODIFIED_TIME_DELIMITER + lastModifiedLowerBound);
            FileUtils.forceMkdir(sortedFileDir);
            return sortedFileDir;
        }

        static long getLastModifiedTimeFromDirName(File dir) {
            if (!dir.isDirectory()) {
                throw new IllegalArgumentException(dir.getAbsolutePath() + " is not a directory");
            }
            return Long.parseLong(dir.getName().substring(dir.getName().lastIndexOf(LAST_MODIFIED_TIME_DELIMITER) +
                    LAST_MODIFIED_TIME_DELIMITER.length()));
        }

        static void markCompleted(File sortWorkDir) {
            try {
                Files.write(Paths.get(sortWorkDir.getAbsolutePath() + "/" + COMPLETION_MARKER_FILE_NAME), ("completed").getBytes(),
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
            } catch (IOException e) {
                log.warn("Resuming download will not be accurate. Could not mark the directory "
                        + sortWorkDir.getAbsolutePath() + " completed.", e);
            }
        }

        static boolean hasCompleted(File sortWorkDir) {
            return new File(sortWorkDir + "/" + COMPLETION_MARKER_FILE_NAME).exists();
        }

        static void markCompletionTill(File sortWorkDir, long lastModifiedTime) {
            try {
                Files.write(Paths.get(sortWorkDir.getAbsolutePath() + "/" + STATUS_FILE_NAME), ("" + lastModifiedTime).getBytes(),
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
            } catch (IOException e) {
                log.warn("Resuming download will not be accurate. Could not save completion status = " + lastModifiedTime
                        + " in " + sortWorkDir.getAbsolutePath(), e);
            }
        }

        static long getLastModifiedOfLastDownloadedDocument(File sortWorkDir) throws IOException {
            File statusFile = new File(sortWorkDir.getAbsolutePath() + "/" + STATUS_FILE_NAME);
            if (!statusFile.exists()) {
                return -1;
            }
            return Long.parseLong(Files.readAllLines(statusFile.toPath()).get(0));
        }

        static Stream<File> getDataFiles(File sortWorkDir) {
            return Arrays.stream(sortWorkDir.listFiles()).filter(f -> !STATUS_FILE_NAME.equals(f.getName()) &&
                    !COMPLETION_MARKER_FILE_NAME.equals(f.getName()));
        }

    }
}
