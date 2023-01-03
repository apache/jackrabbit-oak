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
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.index.indexer.document.LastModifiedRange;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverser;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverserFactory;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentTraverser;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.sizeOf;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.MultithreadedTraverseWithSortStrategy.DirectoryHelper;

/**
 * A callable representing a task for traversing/downloading the nodes states from the node store and creating sorted files
 * based on the downloaded data.
 */
class TraverseAndSortTask implements Callable<List<File>>, MemoryManagerClient {

    /**
     * An prefix for ID of this task (value is acronym for TraverseAndSortTask).
     */
    private static final String ID_PREFIX = "TAST-";
    private final Logger log = LoggerFactory.getLogger(getClass());
    /**
     * Iterable over the nodeStates this task is supposed to traverse. Note that the iteration could stop without traversing
     * all the node states from this iterable, if this task decides to split up and offer some of its work to another
     * {@link TraverseAndSortTask}
     */
    private final NodeStateEntryTraverser nodeStates;
    private final NodeStateEntryWriter entryWriter;
    private final File storeDir;
    private final Compression algorithm;
    private final Comparator<NodeStateHolder> comparator;
    private long entryCount;
    private long memoryUsed;
    private final File sortWorkDir;
    private final List<File> sortedFiles = new ArrayList<>();
    private final BlockingQueue<File> parentSortedFiles;
    private final LinkedList<NodeStateHolder> entryBatch = new LinkedList<>();
    private NodeStateEntry lastSavedNodeStateEntry;
    private final String taskID;
    private final AtomicReference<Phaser> dataDumpNotifyingPhaserRef = new AtomicReference<>();
    /**
     * Queue to which the {@link #taskID} of completed tasks is added.
     */
    private final Queue<String> completedTasks;
    /**
     * Queue to which any newly created tasks should be added. New tasks are created when this task decides it needs to split up.
     */
    private final Queue<Callable<List<File>>> newTasksQueue;
    /**
     * {@link Phaser} used to signal the creation and completion events of this task. Task is registered to this phaser inside the
     * constructor itself, so that it is not possible to have any created task which is not registered to this phaser.
     */
    private final Phaser phaser;
    /**
     * The node states which have a last modified time less than this would not be considered by this task.
     */
    final long lastModifiedLowerBound;
    /**
     * The node states which have a last modified time greater than or equal to this would not be considered by this task.
     */
    private long lastModifiedUpperBound;
    private final BlobStore blobStore;
    private final NodeStateEntryTraverserFactory nodeStateEntryTraverserFactory;
    private final MemoryManager memoryManager;
    private String registrationID;
    private final long dumpThreshold;
    private Predicate<String> pathPredicate = path -> true;

    TraverseAndSortTask(MongoDocumentTraverser.TraversingRange range, Comparator<NodeStateHolder> comparator,
                        BlobStore blobStore, File storeDir, Compression algorithm,
                        Queue<String> completedTasks, Queue<Callable<List<File>>> newTasksQueue,
                        Phaser phaser, NodeStateEntryTraverserFactory nodeStateEntryTraverserFactory,
                        MemoryManager memoryManager, long dumpThreshold, BlockingQueue parentSortedFiles,
                        Predicate<String> pathPredicate) throws IOException {
        this.nodeStates = nodeStateEntryTraverserFactory.create(range);
        this.taskID = ID_PREFIX + nodeStates.getId();
        this.lastModifiedLowerBound = nodeStates.getDocumentTraversalRange().getLastModifiedRange().getLastModifiedFrom();
        this.lastModifiedUpperBound = nodeStates.getDocumentTraversalRange().getLastModifiedRange().getLastModifiedTo();
        this.blobStore = blobStore;
        this.entryWriter = new NodeStateEntryWriter(blobStore);
        this.storeDir = storeDir;
        this.algorithm = algorithm;
        this.comparator = comparator;
        this.completedTasks = completedTasks;
        this.newTasksQueue = newTasksQueue;
        this.phaser = phaser;
        this.nodeStateEntryTraverserFactory = nodeStateEntryTraverserFactory;
        this.memoryManager = memoryManager;
        this.dumpThreshold = dumpThreshold;
        this.parentSortedFiles = parentSortedFiles;
        this.pathPredicate = pathPredicate;
        sortWorkDir = DirectoryHelper.createdSortWorkDir(storeDir, taskID, lastModifiedLowerBound, lastModifiedUpperBound);
        if (range.getStartAfterDocumentID() != null) {
            DirectoryHelper.markLastProcessedStatus(sortWorkDir, lastModifiedLowerBound, range.getStartAfterDocumentID());
        }
        phaser.register();
        log.debug("Task {} with traversing range {} registered to phaser", taskID, range);
    }

    @Override
    public void memoryLow(Phaser phaser) {
        if (dataDumpNotifyingPhaserRef.compareAndSet(null, phaser)) {
            log.info("{} registering to low memory notification phaser", taskID);
            phaser.register();
        } else {
            log.warn("{} already has a low memory notification phaser.", taskID);
        }
    }

    private boolean registerWithMemoryManager() {
        Optional<String> registrationIDOptional = memoryManager.registerClient(this);
        registrationIDOptional.ifPresent(s -> {
            registrationID = s;
            log.debug("{} Registered with memory manager with registration ID {}", taskID, registrationID);
        });
        return registrationIDOptional.isPresent();
    }

    @Override
    public List<File> call() throws EOFException {
        try {
            Random random = new Random();
            while (MemoryManager.Type.JMX_BASED.equals(memoryManager.getType()) && !registerWithMemoryManager()) {
                int sleepDuration = 1000 + random.nextInt(500);
                log.info("{} Could not register to memory manager. Sleeping {} ms before retrying", taskID, sleepDuration);
                try {
                    Thread.sleep(sleepDuration);
                } catch (InterruptedException e) {
                    log.warn(taskID + " Interrupted while sleeping while trying to register to memory manager", e);
                }
            }
            logFlags();
            writeToSortedFiles();
            log.info("Completed task {}", taskID);
            completedTasks.add(taskID);
            DirectoryHelper.markCompleted(sortWorkDir);
            parentSortedFiles.addAll(sortedFiles);

            return sortedFiles;
        } catch (RuntimeException e) {
            log.error(taskID + " dumping existing progress because it could not complete download with ", e);
            try {
                sortAndSaveBatch();
                reset();
                log.debug(taskID + " complete dumping existing progress");
            } catch (Exception dumpErr) {
                log.warn(taskID + " failed to dump existing progress with ", dumpErr);
            }
            throw e;
        } catch (IOException e) {
            log.error(taskID + " could not complete download with ", e);
        } finally {
            phaser.arriveAndDeregister();
            log.info("{} entered finally block.", taskID);
            Phaser dataDumpPhaser = dataDumpNotifyingPhaserRef.get();
            if (dataDumpPhaser != null) {
                log.info("{} Data dump phaser not null after task completion. Notifying memory listener.", taskID);
                dataDumpPhaser.arriveAndDeregister();
            }
            if (MemoryManager.Type.JMX_BASED.equals(memoryManager.getType())) {
                memoryManager.deregisterClient(registrationID);
            }
            try {
                nodeStates.close();
            } catch (IOException e) {
                log.error(taskID + " could not close NodeStateEntryTraverser", e);
            }
        }
        return Collections.emptyList();
    }

    private void writeToSortedFiles() throws IOException {
        Stopwatch w = Stopwatch.createStarted();
        for (NodeStateEntry e : nodeStates) {
            if (e.getLastModified() >= lastModifiedUpperBound) {
                break;
            }
            entryCount++;
            addEntry(e);
        }

        //Save the last batch
        sortAndSaveBatch();
        reset();

        log.info("{} Dumped {} nodestates in json format in {}",taskID, entryCount, w);
        log.info("{} Created {} sorted files of size {} to merge", taskID,
                sortedFiles.size(), humanReadableByteCount(sizeOf(sortedFiles)));
    }

    File getSortWorkDir() {
        return sortWorkDir;
    }

    long getLastModifiedLowerBound() {
        return lastModifiedLowerBound;
    }

    long getLastModifiedUpperBound() {
        return lastModifiedUpperBound;
    }

    void addEntry(NodeStateEntry e) throws IOException {
        if (memoryManager.isMemoryLow()) {
            if (memoryUsed >= dumpThreshold) {
                sortAndSaveBatch();
                reset();
            } else {
                log.trace("{} Memory manager reports low memory but there is not enough data ({}) to dump.", taskID, humanReadableByteCount(memoryUsed));
            }
        }

        long remainingNumberOfTimestamps = lastModifiedUpperBound - e.getLastModified();
        // check if this task can be split
        if (remainingNumberOfTimestamps > 1) {
            /*
              If there is a completed task, there is a chance of some worker thread being idle, so we create a new task from
              the current task. To split, we reduce the traversal upper bound for this task and pass on the node states from
              the new upper bound to the original upper bound to a new task.
             */
            if (completedTasks.poll() != null) {
                long splitPoint = e.getLastModified() + (long)Math.ceil((lastModifiedUpperBound - e.getLastModified())/2.0);
                log.info("Splitting task {}. New Upper limit for this task {}. New task range - {} to {}", taskID, splitPoint, splitPoint, this.lastModifiedUpperBound);
                newTasksQueue.add(new TraverseAndSortTask(new MongoDocumentTraverser.TraversingRange(
                        new LastModifiedRange(splitPoint, this.lastModifiedUpperBound), null),
                        comparator, blobStore, storeDir, algorithm, completedTasks,
                        newTasksQueue, phaser, nodeStateEntryTraverserFactory, memoryManager,
                        dumpThreshold, parentSortedFiles, pathPredicate));
                this.lastModifiedUpperBound = splitPoint;
                DirectoryHelper.setLastModifiedUpperLimit(sortWorkDir, lastModifiedUpperBound);
            }
        }

        String path = e.getPath();
        if (!NodeStateUtils.isHiddenPath(path) && pathPredicate.test(path)) {
            log.debug("Adding to entry, path={} hidden={} predicate={}", path, NodeStateUtils.isHiddenPath(path), pathPredicate.test(path));
            String jsonText = entryWriter.asJson(e.getNodeState());
            //Here logic differs from NodeStateEntrySorter in sense that
            //Holder line consist only of json and not 'path|json'
            NodeStateHolder h = new StateInBytesHolder(path, jsonText);
            entryBatch.add(h);
            updateMemoryUsed(h);
        }

        lastSavedNodeStateEntry = e;

    }

    /*
     * For explanation regarding synchronization see {@link #memoryLow(Phaser)}
     */
    private synchronized void reset() {
        log.trace("{} Reset called ", taskID);
        if (MemoryManager.Type.SELF_MANAGED.equals(memoryManager.getType())) {
            memoryManager.changeMemoryUsedBy(-1 * memoryUsed);
        }
        Phaser phaser = dataDumpNotifyingPhaserRef.get();
        if (phaser != null) {
            log.info("{} Finished saving data to disk. Notifying memory listener.", taskID);
            phaser.arriveAndDeregister();
            dataDumpNotifyingPhaserRef.compareAndSet(phaser, null);
        }
        memoryUsed = 0;
    }

    private void sortAndSaveBatch() throws IOException {
        if (entryBatch.isEmpty()) {
            return;
        }
        entryBatch.sort(comparator);
        Stopwatch w = Stopwatch.createStarted();
        File newtmpfile = File.createTempFile("sortInBatch", "flatfile", sortWorkDir);
        long textSize = 0;
        long size = entryBatch.size();
        try (BufferedWriter writer = FlatFileStoreUtils.createWriter(newtmpfile, algorithm)) {
            // no concurrency issue with this traversal because addition to this list is only done in #addEntry which, for
            // a given TraverseAndSortTask object will only be called from same thread
            while (!entryBatch.isEmpty()) {
                NodeStateHolder h = entryBatch.removeFirst();
                //Here holder line only contains nodeState json
                String text = entryWriter.toString(h.getPathElements(), h.getLine());
                writer.write(text);
                writer.newLine();
                textSize += text.length() + 1;
            }
        }

        log.info("{} Sorted and stored batch of size {} (uncompressed {}) with {} entries in {}. Last entry id = {}", taskID,
                humanReadableByteCount(newtmpfile.length()), humanReadableByteCount(textSize), size, w,
                lastSavedNodeStateEntry.getId());
        DirectoryHelper.markLastProcessedStatus(sortWorkDir, lastSavedNodeStateEntry.getLastModified(),
                lastSavedNodeStateEntry.getId());
        sortedFiles.add(newtmpfile);
    }

    private void logFlags() {
        log.info("Starting task {}", taskID);
    }

    private void updateMemoryUsed(NodeStateHolder h) {
        memoryUsed += h.getMemorySize();
        if (MemoryManager.Type.SELF_MANAGED.equals(memoryManager.getType())) {
            memoryManager.changeMemoryUsedBy(h.getMemorySize());
        }
    }

}
