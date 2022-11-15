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
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.index.indexer.document.CompositeException;
import org.apache.jackrabbit.oak.index.indexer.document.LastModifiedRange;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverser;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverserFactory;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Phaser;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.*;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.DEFAULT_NUMBER_OF_MERGE_TASK_THREADS;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.getSortedStoreFileName;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentTraverser.TraversingRange;

/**
 * This class implements a sort strategy where node store is concurrently traversed for downloading node states by
 * multiple threads (number of threads is configurable via java system property {@link FlatFileNodeStoreBuilder#PROP_THREAD_POOL_SIZE}
 * and sorted node states are written to files while concurrently being merged by multiple threads (number of threads is
 * configurable via java system property {@link FlatFileNodeStoreBuilder#PROP_MERGE_THREAD_POOL_SIZE}.
 * The traverse/download and sort tasks are submitted to an executor service. Each of those tasks create some sorted files which
 * are merged while still downloading and in the end being merged (sorted) into one.
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
 * <h3>Download task state save and resume explanation -</h3>
 * <ol>
 *     <li>Each {@link TraverseAndSortTask} is assigned a {@link TraversingRange} which indicates the range of documents which this task should process.</li>
 *     <li>Each task creates a directory sort-work-dir, where it stores the downloaded data and it also contains information about the traversing range this
 *     task has to process.</li>
 *     <li>Whenever task split happens, the upper limit of last modified range information stored in the sort-work-dir of split task is changed</li>
 *     <li>Data is downloaded from document store in increasing order of (_modified, _id)</li>
 *     <li>After the task dumps a batch of data into sort-work-dir, it also saves the (_modified, _id) state information of last dumped document.</li>
 *     <li>If a task completes successfully without any exception, the completion event is also marked in its sort-work-dir</li>
 *     <li>See {@link MultithreadedTraverseWithSortStrategy.DirectoryHelper} for sort-work-dir management APIs</li>
 *     <li>If there is some exception and operation has to be retried, the retry happens in following way -
 *          <ol>
 *              <li>New tasks are created for all the non-completed sort-work-dirs of previous run, in the following way -
 *                  <ol>
 *                      <li>If no state information was saved in the sort-work-dir, that means, no data dump happened for that dir, so we create one task with same
 *                      traversing range information as this sort-work-dir</li>
 *                      <li>
 *                          If some state information is found, that means, some data was already dumped. So to avoid again downloading that data, we create two tasks-
 *                          Suppose the traversing range of this sort-work-dir is - _modified in [10, 100) and the state information is (50, doc_id)
 *                          <ol>
 *                              <li>First task will download docs with _modified = 50 and _id < doc_id</li>
 *                              <li>Second task downloads docs with _modified in [51, 100) </li>
 *                          </ol>
 *                      </li>
 *                  </ol>
 *              </li>
 *              <li>If multiple retries have happened, there would be a list of sort-work-dirs from each of the runs i.e. a List<List<File>>.</li>
 *              <li>The list should be sorted in order of run i.e. first list would be the list of dirs from first run and last list would be the list of files from the most recent run
 *              which failed.</li>
 *              <li>The data dump files from each of sort-work-dirs of every run would be considered, but for creating further tasks for downloading remaining data
 *              only the sort-work-dirs from most recent run would be considered.</li>
 *          </ol>
 *     </li>
 * </ol>
 */
public class MultithreadedTraverseWithSortStrategy implements SortStrategy {

    private static final Logger log = LoggerFactory.getLogger(MultithreadedTraverseWithSortStrategy.class);
    /**
     * Directory where sorted files will be created.
     */
    private final File storeDir;
    private final File mergeDir;
    private final String mergeDirName = "merge";

    /**
     * Comparator used for comparing node states for creating sorted files.
     */
    private final Comparator<NodeStateHolder> comparator;
    private final BlockingQueue<File> sortedFiles;
    private final ConcurrentLinkedQueue<Throwable> throwables;
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

    private final Phaser mergePhaser;
    /**
     * This poison pill is added to {@link #taskQueue} to indicate that no more new tasks would be submitted to this queue.
     */
    private static final Callable<List<File>> POISON_PILL = () -> null;

    private final MemoryManager memoryManager;

    private final long dumpThreshold;
    private final Compression algorithm;

    private Predicate<String> pathPredicate = path -> true;

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
     *                                a traverser whose lower limit (inclusive) is the last modified value at index i and upper
     *                                limit (exclusive) is the last modified value at index i+1. For the last entry of this list,
     *                                we create a traverser with lower limit equal to that value and upper limit equal to that
     *                                value + 1. NOTE- This parameter is only read when {@code existingDataDumpDir} parameter is null.
     * @param pathComparator comparator used to help with sorting of node state entries.
     * @param blobStore blob store
     * @param storeDir Directory where sorted files will be created.
     * @param existingDataDumpDirs iterable over directories containing files from previous incomplete runs (which need to
     *                             be merged with the result from current run, if the current run has resumed from the point where
     *                             previous runs stopped). If this is not null and not empty, the {@code lastModifiedBreakPoints} parameter is ignored.
     * @param algorithm string representation of the compression algorithm, use "none" for disable compression.
     */
    MultithreadedTraverseWithSortStrategy(NodeStateEntryTraverserFactory nodeStateEntryTraverserFactory,
                                          List<Long> lastModifiedBreakPoints, PathElementComparator pathComparator,
                                          BlobStore blobStore, File storeDir, List<File> existingDataDumpDirs,
                                          Compression algorithm, MemoryManager memoryManager, long dumpThreshold,
                                          Predicate<String> pathPredicate) throws IOException {
        this.storeDir = storeDir;
        this.mergeDir = new File(storeDir, mergeDirName);
        this.algorithm = algorithm;
        this.sortedFiles = new LinkedBlockingQueue<>();
        this.throwables = new ConcurrentLinkedQueue<>();
        this.comparator = (e1, e2) -> pathComparator.compare(e1.getPathElements(), e2.getPathElements());
        this.pathPredicate = pathPredicate;
        taskQueue = new LinkedBlockingQueue<>();
        phaser = new Phaser() {
            @Override
            protected boolean onAdvance(int phase, int registeredParties) {
                //terminate phaser only if it is advancing from last phase and there are no registered parties
                return phase == Phases.WAITING_FOR_RESULTS.value && registeredParties == 0;
            }
        };
        // arrives on final file sorted
        mergePhaser = new Phaser(1);
        this.memoryManager = memoryManager;
        this.dumpThreshold = dumpThreshold;
        createInitialTasks(nodeStateEntryTraverserFactory, lastModifiedBreakPoints, blobStore, existingDataDumpDirs);
    }

    void createInitialTasks(NodeStateEntryTraverserFactory nodeStateEntryTraverserFactory,
                            List<Long> lastModifiedBreakPoints, BlobStore blobStore, List<File> existingDataDumpDirs)
            throws IOException {
        ConcurrentLinkedQueue<String> completedTasks = new ConcurrentLinkedQueue<>();
        if (existingDataDumpDirs != null && existingDataDumpDirs.size() > 0) {
            //include all sorted files from previous incomplete runs
            for (int i = 0; i < existingDataDumpDirs.size(); i++) {
                File existingDataDumpDir = existingDataDumpDirs.get(i);
                File [] existingWorkDirs = existingDataDumpDir.listFiles();
                if (existingWorkDirs == null) {
                    throw new IllegalArgumentException("Could not obtain file from " + existingDataDumpDir.getPath());
                }
                for (File existingSortWorkDir : existingWorkDirs) {
                    if (!existingSortWorkDir.isDirectory()) {
                        log.info("Not a directory {}. Skipping it.", existingSortWorkDir.getAbsolutePath());
                        continue;
                    } else if (existingSortWorkDir.getName().equals(mergeDirName)) {
                        log.info("Intermediate Merge Directory {}. Skipping it.", existingSortWorkDir.getAbsolutePath());
                        DirectoryHelper.getDataFiles(existingSortWorkDir).forEach(file -> {
                            log.debug("Including existing intermediate merged file {}", file.getPath());
                            sortedFiles.add(file);
                        });
                        continue;
                    }
                    boolean downloadCompleted = DirectoryHelper.hasCompleted(existingSortWorkDir);
                    if (!downloadCompleted && i == existingDataDumpDirs.size() - 1) {
                        long start = DirectoryHelper.getLastModifiedLowerLimit(existingSortWorkDir);
                        long end = DirectoryHelper.getLastModifiedUpperLimit(existingSortWorkDir);
                        DirectoryHelper.SavedState savedState = DirectoryHelper.getIdOfLastDownloadedDocument(existingSortWorkDir);
                        if (savedState == null) {
                            addTask(new TraversingRange(new LastModifiedRange(start, end), null), nodeStateEntryTraverserFactory,
                                    blobStore, completedTasks);
                        } else {
                            start = savedState.lastModified;
                            addTask(new TraversingRange(new LastModifiedRange(start, start + 1), savedState.id), nodeStateEntryTraverserFactory,
                                    blobStore, completedTasks);
                            if (end > start + 1) {
                                addTask(new TraversingRange(new LastModifiedRange(start + 1, end), null), nodeStateEntryTraverserFactory,
                                        blobStore, completedTasks);
                            }
                        }
                    }
                    log.info("Including existing sorted files from directory {} (hasCompleted={})",
                            existingSortWorkDir.getAbsolutePath(), downloadCompleted);
                    DirectoryHelper.getDataFiles(existingSortWorkDir).forEach(file -> {
                        log.debug("Including existing sorted file {}", file.getName());
                        sortedFiles.add(file);
                    });
                }
            }
        } else {
            for (int i = 0; i < lastModifiedBreakPoints.size(); i++) {
                long start = lastModifiedBreakPoints.get(i);
                long end = i < lastModifiedBreakPoints.size() - 1 ? lastModifiedBreakPoints.get(i + 1) : lastModifiedBreakPoints.get(i) + 1;
                addTask(new TraversingRange(new LastModifiedRange(start, end), null),
                        nodeStateEntryTraverserFactory, blobStore, completedTasks);
            }
        }
    }

    void addTask(TraversingRange range, NodeStateEntryTraverserFactory nodeStateEntryTraverserFactory, BlobStore blobStore,
                         ConcurrentLinkedQueue<String> completedTasks) throws IOException {
        taskQueue.add(new TraverseAndSortTask(range, comparator, blobStore, storeDir,
                algorithm, completedTasks, taskQueue, phaser, nodeStateEntryTraverserFactory,
                memoryManager, dumpThreshold, sortedFiles, pathPredicate));
    }

    @Override
    public File createSortedStoreFile() throws CompositeException {
        String watcherThreadName = "watcher";
        String mergerThreadName = "merger";
        Thread watcher = new Thread(new TaskRunner(), watcherThreadName);
        watcher.setDaemon(true);
        watcher.start();
        File sortedFile = new File(storeDir, getSortedStoreFileName(algorithm));
        int threadPoolSize = Integer.getInteger(PROP_MERGE_THREAD_POOL_SIZE, DEFAULT_NUMBER_OF_MERGE_TASK_THREADS);
        int batchMergeSize = Integer.getInteger(PROP_MERGE_TASK_BATCH_SIZE, DEFAULT_NUMBER_OF_FILES_PER_MERGE_TASK);
        Runnable mergeRunner = new MergeRunner(sortedFile, sortedFiles, mergeDir, comparator, mergePhaser, batchMergeSize, threadPoolSize, algorithm);
        Thread merger = new Thread(mergeRunner, mergerThreadName);
        merger.setDaemon(true);
        merger.start();
        phaser.awaitAdvance(Phases.WAITING_FOR_TASK_SPLITS.value);
        log.debug("All tasks completed. Signalling {} to proceed to result collection.", watcherThreadName);
        taskQueue.add(POISON_PILL);
        phaser.awaitAdvance(Phases.WAITING_FOR_RESULTS.value);
        if (!throwables.isEmpty()) {
            CompositeException exception = new CompositeException();
            for (Throwable throwable : throwables) {
                exception.addSuppressed(throwable);
            }
            sortedFiles.add(MergeRunner.MERGE_FORCE_STOP_POISON_PILL);
            mergePhaser.awaitAdvance(0);
            throw exception;
        }
        log.debug("Result collection complete. Proceeding to final merge.");
        Stopwatch w = Stopwatch.createStarted();
        sortedFiles.add(MergeRunner.MERGE_POISON_PILL);
        mergePhaser.awaitAdvance(0);
        log.info("Merging of sorted files completed in {}", w);
        return sortedFile;
    }

    @Override
    public long getEntryCount() {
        //todo - get actual entry count for correct progress estimation
        return 0;
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
        private final int threadPoolSize = Integer.getInteger(PROP_THREAD_POOL_SIZE, DEFAULT_NUMBER_OF_DATA_DUMP_THREADS);

        public TaskRunner() {
            this.executorService = Executors.newFixedThreadPool(threadPoolSize);
        }

        @Override
        public void run() {
            try {
                log.info("Using a thread pool of size {}", threadPoolSize);
                List<Future<List<File>>> results = Lists.newArrayList();
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
                try {
                    boolean exceptionsCaught = false;
                    for (Future<List<File>> result : results) {
                        try {
                            result.get();
                        } catch (Throwable e) {
                            throwables.add(e);
                            exceptionsCaught = true;
                        }
                    }
                    log.debug("Completed result collection {}. Arriving at phaser now.", !exceptionsCaught ? "fully" : "partially");
                } finally {
                    phaser.arrive();
                }
            } catch (InterruptedException e) {
                log.error("Could not complete task submissions", e);
            }
            executorService.shutdown();
        }
    }

    static class DirectoryHelper {

        private static final String PREFIX = "sort-work-dir-";
        private static final String LAST_MODIFIED_TIME_DELIMITER = "-from-";
        /**
         * File name for file which indicates the _id and _modified of last processed document. Let's say we saved the following
         * values {_id=saved_id, _modified=saved_modified}, then while resuming download we need to download :
         * 1. All documents with _modified = saved_modified which have _id > saved_id
         * 2. All documents with _modified > saved_modified and _modified < LAST_MODIFIED_UPPER_LIMIT for this sort work dir
         */
        private static final String STATUS_FILE_NAME = "last-saved";
        /**
         * File name for file which indicates the upper limit (exclusive) of last modified time of the range of documents which this
         * sort work dir is meant to have data for.
         */
        private static final String LAST_MODIFIED_UPPER_LIMIT = "last-modified-upper-limit";
        /**
         * If this file is present, that means all the documents meant for this sort work dir have been processed.
         */
        private static final String COMPLETION_MARKER_FILE_NAME = "completed";
        private static final Logger log = LoggerFactory.getLogger(DirectoryHelper.class);

        static File createdSortWorkDir(File storeDir, String taskID, long lastModifiedLowerBound,
                                       long lastModifiedUpperBound) throws IOException {
            File sortedFileDir = new File(storeDir, PREFIX + taskID + LAST_MODIFIED_TIME_DELIMITER + lastModifiedLowerBound);
            FileUtils.forceMkdir(sortedFileDir);
            setLastModifiedUpperLimit(sortedFileDir, lastModifiedUpperBound);
            return sortedFileDir;
        }

        static long getLastModifiedLowerLimit(File dir) {
            if (!dir.isDirectory()) {
                throw new IllegalArgumentException(dir.getAbsolutePath() + " is not a directory");
            }
            return Long.parseLong(dir.getName().substring(dir.getName().lastIndexOf(LAST_MODIFIED_TIME_DELIMITER) +
                    LAST_MODIFIED_TIME_DELIMITER.length()));
        }

        static void setLastModifiedUpperLimit(File sortWorkDir, long lastModifiedUpperLimit) throws IOException {
            Files.write(Paths.get(sortWorkDir.getAbsolutePath() + "/" + LAST_MODIFIED_UPPER_LIMIT), ("" + lastModifiedUpperLimit).getBytes(),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        }

        static long getLastModifiedUpperLimit(File sortWorkDir) throws IOException {
            File lastModifiedUpperLimitFile = new File(sortWorkDir.getAbsolutePath() + "/" + LAST_MODIFIED_UPPER_LIMIT);
            if (!lastModifiedUpperLimitFile.exists()) {
                throw new IOException("Could not find file containing last modified upper limit in " + sortWorkDir.getAbsolutePath());
            }
            return Long.parseLong(Files.readAllLines(lastModifiedUpperLimitFile.toPath()).get(0));
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

        static void markLastProcessedStatus(File sortWorkDir, long lastDownloadedDocLastModified, String lastDownloadedDocID) {
            try {
                Files.write(Paths.get(sortWorkDir.getAbsolutePath() + "/" + STATUS_FILE_NAME),
                        new SavedState(lastDownloadedDocLastModified, lastDownloadedDocID).serialize().getBytes(),
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
            } catch (IOException e) {
                log.warn("Resuming download will not be accurate. Could not save last processed status = " + lastDownloadedDocID
                        + " in " + sortWorkDir.getAbsolutePath(), e);
            }
        }

        static SavedState getIdOfLastDownloadedDocument(File sortWorkDir) throws IOException {
            File statusFile = new File(sortWorkDir.getAbsolutePath() + "/" + STATUS_FILE_NAME);
            if (!statusFile.exists()) {
                return null;
            }
            return SavedState.deserialize(Files.readAllLines(statusFile.toPath()).get(0));
        }

        static Stream<File> getDataFiles(File sortWorkDir) {
            return Arrays.stream(sortWorkDir.listFiles()).filter(f -> !STATUS_FILE_NAME.equals(f.getName()) &&
                    !COMPLETION_MARKER_FILE_NAME.equals(f.getName()) && !LAST_MODIFIED_UPPER_LIMIT.equals(f.getName()));
        }

        static class SavedState {
            long lastModified;
            String id;

            public SavedState(long lastModified, String id) {
                this.lastModified = lastModified;
                this.id = id;
            }

            String serialize() {
                return lastModified + ":" + id;
            }

            static SavedState deserialize(String s) {
                int colonIndex = s.indexOf(":");
                if (colonIndex == -1) {
                    throw new IllegalArgumentException("Invalid serialized string " + s);
                }
                long lastMod = Long.parseLong(s.substring(0, colonIndex));
                String id = s.substring(colonIndex + 1);
                return new SavedState(lastMod, id);
            }
        }

    }
}
