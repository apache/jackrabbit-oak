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
import org.apache.jackrabbit.oak.index.indexer.document.CompositeException;
import org.apache.jackrabbit.oak.index.indexer.document.LastModifiedRange;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverser;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverserFactory;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.DEFAULT_NUMBER_OF_DATA_DUMP_THREADS;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.PROP_THREAD_POOL_SIZE;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.createWriter;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.getSortedStoreFileName;
import static org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentTraverser.TraversingRange;

/**
 * This class implements a sort strategy where node store is concurrently traversed for downloading node states by
 * multiple threads (number of threads is configurable via java system property {@link FlatFileNodeStoreBuilder#PROP_THREAD_POOL_SIZE}.
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
    private final Charset charset = UTF_8;
    private final boolean compressionEnabled;
    private final NodeStateEntryWriter entryWriter;
    /**
     * Directory where sorted files will be created.
     */
    private final File storeDir;
    /**
     * Comparator used for comparing node states for creating sorted files.
     */
    private final Comparator<NodeStateHolder> comparator;
    private final Set<String> preferred;
    private final ConcurrentLinkedQueue<File> sortedFiles;
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

    private static final File MERGE_POISON_PILL = new File("");

    private final MemoryManager memoryManager;

    private final long dumpThreshold;

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
     * @param compressionEnabled if true, the created files would be compressed
     */
    MultithreadedTraverseWithSortStrategy(NodeStateEntryTraverserFactory nodeStateEntryTraverserFactory,
                                          List<Long> lastModifiedBreakPoints, PathElementComparator pathComparator,
                                          BlobStore blobStore, File storeDir, List<File> existingDataDumpDirs,
                                          boolean compressionEnabled, MemoryManager memoryManager, long dumpThreshold) throws IOException {
        this.storeDir = storeDir;
        this.compressionEnabled = compressionEnabled;
        this.sortedFiles = new ConcurrentLinkedQueue<>();
        this.throwables = new ConcurrentLinkedQueue<>();
        this.comparator = (e1, e2) -> pathComparator.compare(e1.getPathElements(), e2.getPathElements());
        this.preferred = pathComparator == null ? new HashSet<>() : pathComparator.getPreferred();
        this.entryWriter = new NodeStateEntryWriter(blobStore);
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
        taskQueue.add(new TraverseAndSortTask(range, comparator, preferred, blobStore, storeDir,
                compressionEnabled, completedTasks, taskQueue, phaser, nodeStateEntryTraverserFactory, memoryManager, dumpThreshold, sortedFiles));
    }

    @Override
    public File createSortedStoreFile() throws IOException, CompositeException {
        String watcherThreadName = "watcher";
        String mergerThreadName = "merger";
        Thread watcher = new Thread(new TaskRunner(), watcherThreadName);
        watcher.start();
        File sortedFile = new File(storeDir, getSortedStoreFileName(compressionEnabled));
        Thread merger = new Thread(new MergeRunner(sortedFile), mergerThreadName);
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
            throw exception;
        }
        log.debug("Result collection complete. Proceeding to final merging.");
        Stopwatch w = Stopwatch.createStarted();
        sortedFiles.add(MERGE_POISON_PILL);
        mergePhaser.awaitAdvance(0);
        log.info("Merging of final sorted files completed in {}", w);
//        sortStoreFile();
        return sortedFile;
    }

    @Override
    public long getEntryCount() {
        //todo - get actual entry count for correct progress estimation
        return 0;
    }

    private File sortStoreFile() throws IOException {
        log.info("Proceeding to perform merge of {} sorted files", sortedFiles.size());
        Stopwatch w = Stopwatch.createStarted();
        File sortedFile = new File(storeDir, "original"+getSortedStoreFileName(compressionEnabled));
//        File serializedSortedFile = new File(storeDir, String.format("serialized-%s", getSortedStoreFileName(false)));

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

//        List<String> commands = new ArrayList<String>();
//        Collections.addAll(commands, "/usr/bin/sort");
//        Collections.addAll(commands, "-T", storeDir.getAbsolutePath());
////        Collections.addAll(commands, "-S", "2G");
//        Collections.addAll(commands, "--parallel", "8");
//        Collections.addAll(commands, "-o", sortedFile.getAbsolutePath());
////        Collections.addAll(commands, "-t", "/");
////        IntStream.range(1, 50).forEach(i -> Collections.addAll(commands, String.format("-k%s,%s", i, i)));
//        if (compressionEnabled) {
//            Collections.addAll(commands, "--compress-program", "gzip");
//            Collections.addAll(commands, "-m");
//            sortedFiles.forEach(f -> commands.add(String.format("<(gunzip -c %s)", f.getAbsolutePath())));
//        } else {
//            Collections.addAll(commands, "-m");
//            sortedFiles.forEach(f -> commands.add(f.getAbsolutePath()));
//        }
//
//        ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", String.join(" ", commands));
//        log.info("Running merge command {}", pb.command());
//        Process p = pb.start();
//        try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
//             BufferedReader errReader = new BufferedReader(new InputStreamReader(p.getErrorStream()))) {
//            String line;
//            p.waitFor();
//            log.info("Merging of sorted files completed in {}", w);
//            while ((line = reader.readLine()) != null) log.info(line);
//            Boolean hasError = false;
//            while ((line = errReader.readLine()) != null)  {
//                log.error(line);
//                hasError = true;
//            }
//            if (hasError) throw new Exception("command execution fail");
//            log.info("Sort command executed successfully");
//        } catch (Exception e) {
//            throw new RuntimeException(String.format("Error while running command %s", pb.command()));
//        }

//        Stopwatch wDeserialize = Stopwatch.createStarted();
//        try (BufferedReader reader = FlatFileStoreUtils.createReader(serializedSortedFile, false);
//             BufferedWriter writer = FlatFileStoreUtils.createWriter(sortedFile, compressionEnabled)) {
//            String line = reader.readLine();
//            while (line != null) {
//                String deserializeLine = entryWriter.deserialize(line);
//                writer.write(deserializeLine);
//                writer.newLine();
//                line = reader.readLine();
//            }
//        }
//        log.info("Deserialize of sorted file completed in {}", wDeserialize);


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
        private final int threadPoolSize = Integer.parseInt(System.getProperty(PROP_THREAD_POOL_SIZE, DEFAULT_NUMBER_OF_DATA_DUMP_THREADS));

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
                try {
                    boolean exceptionsCaught = false;
                    for (Future<List<File>> result : results) {
                        try {
                            result.get();
//                            sortedFiles.addAll(result.get());
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



    private class MergeRunner implements Runnable {
        private final ArrayList<File> lastMergedFiles = new ArrayList<File>();
        private final ArrayList<File> mergedFiles = new ArrayList<File>();
        private final File sortedFile;
        private final File mergeDir = new File(storeDir, "merge");
        private final int failureThreshold = 100;
        private File lastMergedFile = null;
        private final int batchMergeSize = 63;
        private int nextMergedLength = 0;


        public MergeRunner(File sortedFile) throws IOException {
            FileUtils.forceMkdir(mergeDir);
            this.sortedFile = sortedFile;
        }

        private boolean merge(ArrayList<File> files, File outputFile) {
            try (BufferedWriter writer = createWriter(outputFile, compressionEnabled)) {
                Function<String, NodeStateHolder> func1 = (line) -> line == null ? null : new SimpleNodeStateHolder(line);
                Function<NodeStateHolder, String> func2 = holder -> holder == null ? null : holder.getLine();
                ExternalSort.mergeSortedFiles(files,
                        writer,
                        comparator,
                        charset,
                        true, //distinct
                        compressionEnabled, //useZip
                        func2,
                        func1
                );
            } catch (IOException e) {
                log.error("Merge failed with IOException", e);
                return false;
            }
            return true;
        }

        // this might not always be 63 files since the sortedList thread is always adding
        // but as long as we keep track of files that are already merged we won't merge a file twice
        private ArrayList<File> getUnmergedFiles(int size) {
            ArrayList<File> unmergedFiles = new ArrayList<File>();
            sortedFiles.forEach(file -> {
                if (!mergedFiles.contains(file) && unmergedFiles.size() < size) {
                    unmergedFiles.add(file);
                }
            });
            return unmergedFiles;
        }
//
//        private void mergeFromLastPoint(File mergedFile) {
//            ArrayList<File> unmergedFiles = getUnmergedFiles();
//
//            ArrayList<File> mergeTarget = new ArrayList<File>();
//            mergeTarget.addAll(unmergedFiles);
//            if (lastMergedFile != null && lastMergedFile.exists()) {
//                mergeTarget.add(lastMergedFile);
//            }
//
//            boolean success = merge(mergeTarget, mergedFile);
//            if (success) {
//                mergedFiles.addAll(unmergedFiles);
//                nextMergedLength += unmergedFiles.size();
//                lastMergedFile = mergedFile;
//            }
//        }

        @Override
        public void run() {
            // 1. wait for 64 files (compare with merged list)
            //    construct new list of files to be merged by checking if its already merged
            // 2. merge with lastMergedFile
            // 3. add all merged files to merged list
            // 4. update last merged file to new one
            int mergeFailureCount = 0;
            nextMergedLength += batchMergeSize;

            while (true) {
                log.info("==========x in loop");
                if (mergeFailureCount >= failureThreshold) {
                    log.error("give up merging due to failure occurs more than %s times", failureThreshold);
                    break;
                }

                boolean isFinal = false;
                File mergedFile = null;
                int tmp = sortedFiles.size() ;
                while (sortedFiles.size() < nextMergedLength) {
                    int size = sortedFiles.size();
                    if (tmp != size) {
                        log.info("==========x waiting with current count {} nextMergedLength {}", sortedFiles.size(), nextMergedLength);
                        tmp = size;
                    }
                    // if poison pill appears while waiting, break the loop and to final merge
                    if (sortedFiles.contains(MERGE_POISON_PILL)) {
                        log.error("==========x stop waiting because poison pill found");
                        isFinal = true;
                        break;
                    }
                }

                if (mergedFile == null) {
                    mergedFile = new File(mergeDir, String.format("intermediate-%s", nextMergedLength));
                }

                ArrayList<File> unmergedFiles = getUnmergedFiles(isFinal ? Integer.MAX_VALUE : batchMergeSize);
                if (unmergedFiles.contains(MERGE_POISON_PILL)) {
                    unmergedFiles.remove(MERGE_POISON_PILL);
                    mergedFile = sortedFile;
                    log.info("==========x Find poison pill");
                }

                ArrayList<File> mergeTarget = new ArrayList<File>();
                mergeTarget.addAll(unmergedFiles);

                if (lastMergedFile != null && lastMergedFile.exists()) {
                    log.info("==========x add last merged file {}", lastMergedFile.getName());
                    mergeTarget.add(lastMergedFile);
                }

                log.info("==========x Performing merge with size {}", mergeTarget.size());
                if (merge(mergeTarget, mergedFile)) {
                    log.info("==========x merge success");
                    mergedFiles.addAll(unmergedFiles);
                    nextMergedLength += unmergedFiles.size();
                    lastMergedFile = mergedFile;
                    lastMergedFiles.add(mergedFile);
                    log.info("==========x nextMergedLength {}", nextMergedLength);
                    nextMergedLength += batchMergeSize;
                    if (isFinal) {
                        break;
                    }
                } else {
                    log.info("==========x Merge failed");
                    mergeFailureCount += 1;
                }
            }

            log.info("=========x lastMergedFiles size: {}", lastMergedFiles.size());
            for (File f : lastMergedFiles) {
                log.info("=========x mergedFile name: {}", f.getName());
            }
            log.info("==========x merge failure {}", mergeFailureCount);
            log.info("==========x mergePhaser.arriveAndDeregister");

            mergePhaser.arriveAndDeregister();
            // Final Merge
//            ArrayList<File> mergeTarget = new ArrayList<File>();
//            mergeTarget.addAll(getUnmergedFiles());
//            if (lastMergedFile != null && lastMergedFile.exists()) {
//                mergeTarget.add(lastMergedFile);
//            }

//            while (!sortedFiles.contains(MERGE_POISON_PILL)) {
//                File mergedFile;
//
//                ArrayList<File> unmergedFiles = new ArrayList<File>();
//                sortedFiles.forEach(file -> {
//                    if (file == MERGE_POISON_PILL) {
//                        finalSort.set(true);
//                    } else if (!mergedFiles.contains(file)) {
//                        unmergedFiles.add(file);
//                    }
//                });
//
//                if (finalSort.get()) {
//                    mergedFile = sortedFile;
//                } else {
//                    mergedFile = new File(mergeDir, String.format("intermediate-%s", mergeCount));
//                }
//                log.info("batch merging files {} to {}", unmergedFiles, mergedFile.getName());
//
//
//                // merge logic
//                List<File> inputSortedFilesToMerge = new ArrayList<>(unmergedFiles);
//                try (BufferedWriter writer = createWriter(mergedFile, compressionEnabled)) {
//                    Function<String, NodeStateHolder> func1 = (line) -> line == null ? null : new SimpleNodeStateHolder(line);
//                    Function<NodeStateHolder, String> func2 = holder -> holder == null ? null : holder.getLine();
//                    ExternalSort.mergeSortedFiles(inputSortedFilesToMerge,
//                            writer,
//                            comparator,
//                            charset,
//                            true, //distinct
//                            compressionEnabled, //useZip
//                            func2,
//                            func1
//                    );
//                    mergeCount++;
//                    mergedFiles.addAll(unmergedFiles);
//                    if (finalSort.get()) {
//                        break;
//                    }
//                    // add to sortedFiles to be merge in next iteration
//                    sortedFiles.add(mergedFile);
//                } catch (IOException e) {
//                    log.error("Merge failed with IOException", e);
//                }
//            }
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
