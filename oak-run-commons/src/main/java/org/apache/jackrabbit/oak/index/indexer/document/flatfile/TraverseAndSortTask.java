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
import org.apache.jackrabbit.oak.index.indexer.document.LastModifiedRange;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverser;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverserFactory;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private final Iterable<NodeStateEntry> nodeStates;
    private final NodeStateEntryWriter entryWriter;
    private final File storeDir;
    private final boolean compressionEnabled;
    private final Comparator<NodeStateHolder> comparator;
    private long entryCount;
    private long memoryUsed;
    private final File sortWorkDir;
    private final List<File> sortedFiles = new ArrayList<>();
    private final ArrayList<NodeStateHolder> entryBatch = new ArrayList<>();
    private NodeStateEntry lastSavedNodeStateEntry;
    private final String taskID;
    private final AtomicBoolean dumpData = new AtomicBoolean(false);
    private volatile Phaser dataDumpNotifyingPhaser;
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
    private final long lastModifiedLowerBound;
    /**
     * The node states which have a last modified time greater than or equal to this would not be considered by this task.
     */
    private long lastModifiedUpperBound;
    private final BlobStore blobStore;
    private final NodeStateEntryTraverserFactory nodeStateEntryTraverserFactory;
    private final MemoryManager memoryManager;
    private String registrationID;

    TraverseAndSortTask(NodeStateEntryTraverser nodeStates, Comparator<NodeStateHolder> comparator,
                        BlobStore blobStore, File storeDir, boolean compressionEnabled,
                        Queue<String> completedTasks, Queue<Callable<List<File>>> newTasksQueue,
                        Phaser phaser, NodeStateEntryTraverserFactory nodeStateEntryTraverserFactory,
                                MemoryManager memoryManager) throws IOException {
        this.taskID = ID_PREFIX + nodeStates.getId();
        this.nodeStates = nodeStates;
        this.lastModifiedLowerBound = nodeStates.getDocumentModificationRange().getLastModifiedFrom();
        this.lastModifiedUpperBound = nodeStates.getDocumentModificationRange().getLastModifiedTo();
        this.blobStore = blobStore;
        this.entryWriter = new NodeStateEntryWriter(blobStore);
        this.storeDir = storeDir;
        this.compressionEnabled = compressionEnabled;
        this.comparator = comparator;
        this.completedTasks = completedTasks;
        this.newTasksQueue = newTasksQueue;
        this.phaser = phaser;
        this.nodeStateEntryTraverserFactory = nodeStateEntryTraverserFactory;
        this.memoryManager = memoryManager;
        sortWorkDir = DirectoryHelper.createdSortWorkDir(storeDir, taskID, lastModifiedLowerBound);
        phaser.register();
        log.debug("Task {} registered to phaser", taskID);
    }

    /*
     * There is a race condition between this method and {@link #reset()} (because this method checks entry batch size
     * and {@link #reset()} clears it). To ensure, {@link #dataDumpNotifyingPhaser} is always arrived upon by this task,
     * we need to make these methods atomic wrt to one another.
     * @param phaser phaser used to coordinate with {@link MemoryManager}
     */
    @Override
    public synchronized void memoryLow(Phaser phaser) {
        if (entryBatch.isEmpty()) {
            log.info("{} No data to save. Immediately signalling memory manager.", taskID);
            phaser.register();
            phaser.arriveAndDeregister();
            return;
        }
        dataDumpNotifyingPhaser = phaser;
        dataDumpNotifyingPhaser.register();
        log.info("{} Setting dumpData to true", taskID);
        dumpData.set(true);
    }

    private boolean registerWithMemoryManager() {
        Optional<String> registrationIDOptional = memoryManager.registerClient(this);
        registrationIDOptional.ifPresent(s -> {
            registrationID = s;
            log.debug("{} Registered with memory manager with registration ID {}", taskID, registrationID);
        });
        return registrationIDOptional.isPresent();
    }

    public List<File> call() {
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
            if (MemoryManager.Type.JMX_BASED.equals(memoryManager.getType())) {
                memoryManager.deregisterClient(registrationID);
            }
            return sortedFiles;
        } catch (IOException e) {
            log.error(taskID + " could not complete download ", e);
        } finally {
            phaser.arriveAndDeregister();
        }
        return Collections.emptyList();
    }

    public long getEntryCount() {
        return entryCount;
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

        //Free up the batch
        entryBatch.clear();
        entryBatch.trimToSize();

        log.info("{} Dumped {} nodestates in json format in {}",taskID, entryCount, w);
        log.info("{} Created {} sorted files of size {} to merge", taskID,
                sortedFiles.size(), humanReadableByteCount(sizeOf(sortedFiles)));
    }

    private void addEntry(NodeStateEntry e) throws IOException {
        if ((MemoryManager.Type.SELF_MANAGED.equals(memoryManager.getType()) && memoryManager.isMemoryLow()) || dumpData.get()) {
            sortAndSaveBatch();
            reset();
        }

        long remainingNumberOfTimestamps = lastModifiedUpperBound - e.getLastModified();
        // check if this task can be split
        if (remainingNumberOfTimestamps > 1) {
            long splitPoint = e.getLastModified() + (long)Math.ceil((lastModifiedUpperBound - e.getLastModified())/2.0);
            /*
              If there is a completed task, there is a chance of some worker thread being idle, so we create a new task from
              the current task. To split, we reduce the traversal upper bound for this task and pass on the node states from
              the new upper bound to the original upper bound to a new task.
             */
            if (completedTasks.poll() != null) {
                log.info("Splitting task {}. New Upper limit for this task {}. New task range - {} to {}", taskID, splitPoint, splitPoint, this.lastModifiedUpperBound);
                newTasksQueue.add(new TraverseAndSortTask(nodeStateEntryTraverserFactory.create(new LastModifiedRange(splitPoint,
                        this.lastModifiedUpperBound)), comparator, blobStore, storeDir, compressionEnabled, completedTasks,
                        newTasksQueue, phaser, nodeStateEntryTraverserFactory, memoryManager));
                this.lastModifiedUpperBound = splitPoint;
            }
        }

        String jsonText = entryWriter.asJson(e.getNodeState());
        //Here logic differs from NodeStateEntrySorter in sense that
        //Holder line consist only of json and not 'path|json'
        NodeStateHolder h = new StateInBytesHolder(e.getPath(), jsonText);
        entryBatch.add(h);
        lastSavedNodeStateEntry = e;
        updateMemoryUsed(h);

    }

    /*
     * For explanation regarding synchronization see {@link #memoryLow(Phaser)}
     */
    private synchronized void reset() {
        entryBatch.clear();
        if (MemoryManager.Type.SELF_MANAGED.equals(memoryManager.getType())) {
            memoryManager.changeMemoryUsedBy(-1 * memoryUsed);
        }
        dumpData.set(false);
        if (dataDumpNotifyingPhaser != null) {
            log.info("{} Finished saving data to disk. Notifying memory listener.", taskID);
            dataDumpNotifyingPhaser.arriveAndDeregister();
            dataDumpNotifyingPhaser = null;
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
        try (BufferedWriter writer = FlatFileStoreUtils.createWriter(newtmpfile, compressionEnabled)) {
            for (NodeStateHolder h : entryBatch) {
                //Here holder line only contains nodeState json
                String text = entryWriter.toString(h.getPathElements(), h.getLine());
                writer.write(text);
                writer.newLine();
                textSize += text.length() + 1;
            }
        }
        log.info("{} Sorted and stored batch of size {} (uncompressed {}) with {} entries in {}. Last entry lastModified = {}", taskID,
                humanReadableByteCount(newtmpfile.length()), humanReadableByteCount(textSize),entryBatch.size(), w,
                lastSavedNodeStateEntry.getLastModified());
        DirectoryHelper.markLastProcessedStatus(sortWorkDir,
                lastSavedNodeStateEntry.getLastModified());
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
