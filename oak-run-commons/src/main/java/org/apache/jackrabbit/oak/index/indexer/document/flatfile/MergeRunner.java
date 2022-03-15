package org.apache.jackrabbit.oak.index.indexer.document.flatfile;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.comparator.SizeFileComparator;
import org.apache.jackrabbit.oak.commons.sort.ExternalSort;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Phaser;
import java.util.function.Function;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.DEFAULT_NUMBER_OF_MERGE_TASK_THREADS;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.PROP_MERGE_THREAD_POOL_SIZE;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.DEFAULT_NUMBER_OF_FILES_PER_MERGE_TASK;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.PROP_MERGE_TASK_BATCH_SIZE;
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
 */
public class MergeRunner implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(MergeRunner.class);
    private final Charset charset = UTF_8;
    private final boolean compressionEnabled;
    private final ArrayList<File> mergedFiles = new ArrayList<File>();
    private final ArrayList<File> unmergedFiles = new ArrayList<File>();
    private final ExecutorService executorService;
    private final int threadPoolSize = Integer.getInteger(PROP_MERGE_THREAD_POOL_SIZE, DEFAULT_NUMBER_OF_MERGE_TASK_THREADS);
    private final int batchMergeSize = Integer.getInteger(PROP_MERGE_TASK_BATCH_SIZE, DEFAULT_NUMBER_OF_FILES_PER_MERGE_TASK);
    private final Comparator fileSizeComparator = new SizeFileComparator();

    /**
     * The end result file after merging all sorted files.
     */
    private final File sortedFile;

    /**
     * Directory where intermediate merged files will be created.
     */
    private final File mergeDir;
    private final String mergeDirName = "merge";

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
     * This poison pill is added to {@link #sortedFiles} to indicate that download phase has completed.
     */
    public static final File MERGE_POISON_PILL = new File("");

    /**
     * Constructor.
     * @param sortedFiles thread safe list containing files to be merged.
     * @param comparator comparator used to help with sorting of node state entries.
     * @param baseStoreDir base directory where sorted files will be created.
     * @param compressionEnabled if true, the created files would be compressed
     */
    MergeRunner(File sortedFile, BlockingQueue<File> sortedFiles, File baseStoreDir, Comparator comparator,
                Phaser phaser, boolean compressionEnabled) throws IOException {
        this.mergeDir = new File(baseStoreDir, mergeDirName);
        FileUtils.forceMkdir(mergeDir);
        this.compressionEnabled = compressionEnabled;
        this.sortedFiles = sortedFiles;
        this.sortedFile = sortedFile;
        this.throwables = new ConcurrentLinkedQueue<>();
        this.comparator = comparator;
        this.phaser = phaser;
        this.executorService = Executors.newFixedThreadPool(threadPoolSize);
    }

    private boolean merge(List<File> files, File outputFile) {
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


    private List<File> getSmallestUnmergedFiles(int size) {
        ArrayList<File> result = new ArrayList<File>(unmergedFiles);
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
        Phaser mergeTaskPhaser = new Phaser(1);
        List<Future<File>> results = new ArrayList<>();
        List<File> mergeTarget = new ArrayList<>();
        int count = 0;

        while (true) {
            try {
                unmergedFiles.add(sortedFiles.take());
                if (sortedFiles.contains(MERGE_POISON_PILL) || unmergedFiles.contains(MERGE_POISON_PILL)) {
                    break;
                }
                // add another batchMergeSize so that we choose the smallest of a larger range
                if (unmergedFiles.size() >= 2*batchMergeSize) {
                    count++;
                    mergeTarget.clear();
                    mergeTarget = getSmallestUnmergedFiles(batchMergeSize);
                    Callable<File> mergeTask = new Task(mergeTarget, mergeTaskPhaser,
                            new File(mergeDir, String.format("intermediate-%s", count)));
                    markAsMerged(mergeTarget);
                    results.add(executorService.submit(mergeTask));
                }
            } catch (InterruptedException e) {
                log.error("Failed while draining from sortedFiles", e);
            }
        }

        log.info("Waiting for batch sorting tasks completion");
        mergeTaskPhaser.arriveAndAwaitAdvance();
        executorService.shutdown();

        // final merge
        mergeTarget.clear();
        sortedFiles.drainTo(unmergedFiles);
        unmergedFiles.remove(MERGE_POISON_PILL);
        mergeTarget.addAll(unmergedFiles);
        markAsMerged(mergeTarget);
        log.info("There are still {} sorted files not merged yet", mergeTarget.size());
        try {
            boolean exceptionsCaught = false;
            for (Future<File> result : results) {
                try {
                    mergeTarget.add(result.get());
                } catch (Throwable e) {
                    throwables.add(e);
                    exceptionsCaught = true;
                }
            }
            log.debug("Completed merge result collection {}. Arriving at phaser now.", exceptionsCaught ? "partially" : "fully");
        } finally {
            mergeTaskPhaser.arrive();
        }

        log.info("All batch sorting tasks have completed, total of {}", count);
        log.info("Proceeding to perform merge of {} sorted files", mergeTarget.size());
        if (!merge(mergeTarget, sortedFile)) {
            log.error("merge failed for {}", sortedFile.getName());
        } else {
            log.info("merge complete for {}", sortedFile.getName());
        }
        log.info("Total batch sorted file merged is {}", mergedFiles.size());

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
        public File call() {
            log.info("performing merge for {} with size {}", mergedFile.getName(), mergeTarget.size());
            try {
                if (merge(mergeTarget, mergedFile)) {
                    log.info("merge complete for {}", mergedFile.getName());
                    return mergedFile;
                }
                log.error("merge failed for {}", mergedFile.getName());
            } finally {
                mergeTaskPhaser.arriveAndDeregister();
            }

            return mergedFile;
        }
    }
}
