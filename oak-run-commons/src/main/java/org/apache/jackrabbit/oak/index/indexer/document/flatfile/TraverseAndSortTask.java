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

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.lang.management.MemoryNotificationInfo;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import com.google.common.base.Stopwatch;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.index.indexer.document.LastModifiedRange;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverser;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverserFactory;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.management.ManagementFactory.getMemoryMXBean;
import static java.lang.management.ManagementFactory.getMemoryPoolMXBeans;
import static java.lang.management.MemoryType.HEAP;
import static org.apache.commons.io.FileUtils.ONE_GB;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_MAX_SORT_MEMORY_IN_GB;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_MAX_SORT_MEMORY_IN_GB_DEFAULT;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.sizeOf;

/**
 * A callable representing a task for traversing/downloading the nodes states from the node store and creating sorted files
 * based on the downloaded data.
 */
class TraverseAndSortTask implements Callable<List<File>> {
    private static final String OAK_INDEXER_MIN_MEMORY = "oak.indexer.minMemoryForWork";
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final AtomicBoolean sufficientMemory = new AtomicBoolean(true);
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
    private final int maxMemory = Integer.getInteger(OAK_INDEXER_MAX_SORT_MEMORY_IN_GB, OAK_INDEXER_MAX_SORT_MEMORY_IN_GB_DEFAULT);
    private final long minMemory = Integer.getInteger(OAK_INDEXER_MIN_MEMORY, 2);
    private final long maxMemoryBytes = maxMemory * ONE_GB;
    private final long minMemoryBytes = minMemory * ONE_GB;
    private boolean useMaxMemory;
    private long entryCount;
    private long memoryUsed;
    private File sortWorkDir;
    private final List<File> sortedFiles = new ArrayList<>();
    private final ArrayList<NodeStateHolder> entryBatch = new ArrayList<>();
    private NodeStateEntry lastSavedNodeStateEntry;
    private final String taskID;
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

    TraverseAndSortTask(NodeStateEntryTraverser nodeStates, Comparator<NodeStateHolder> comparator,
                        BlobStore blobStore, File storeDir, boolean compressionEnabled,
                        Queue<String> completedTasks, Queue<Callable<List<File>>> newTasksQueue,
                        Phaser phaser, NodeStateEntryTraverserFactory nodeStateEntryTraverserFactory) {
        this.taskID = "TWS-" + nodeStates.getId();
        this.nodeStates = nodeStates;
        this.lastModifiedLowerBound = nodeStates.getDocumentModificationRange().getLastModifiedLowerBound();
        this.lastModifiedUpperBound = nodeStates.getDocumentModificationRange().getLastModifiedUpperBound();
        this.blobStore = blobStore;
        this.entryWriter = new NodeStateEntryWriter(blobStore);
        this.storeDir = storeDir;
        this.compressionEnabled = compressionEnabled;
        this.comparator = comparator;
        this.completedTasks = completedTasks;
        this.newTasksQueue = newTasksQueue;
        this.phaser = phaser;
        this.nodeStateEntryTraverserFactory = nodeStateEntryTraverserFactory;
        phaser.register();
        log.debug("Task {} registered to phaser", taskID);
    }

    public List<File> call() {
        try {
            logFlags();
            configureMemoryListener();
            sortWorkDir = MultithreadedTraversalWithSortStrategy.DirectoryHelper.createdSortWorkDir(storeDir, taskID,
                    lastModifiedLowerBound);
            writeToSortedFiles();
            log.info("Completed task {}", taskID);
            completedTasks.add(taskID);
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

        //Free up the batch
        entryBatch.clear();
        entryBatch.trimToSize();

        log.info("{} Dumped {} nodestates in json format in {}",taskID, entryCount, w);
        log.info("{} Created {} sorted files of size {} to merge", taskID,
                sortedFiles.size(), humanReadableByteCount(sizeOf(sortedFiles)));
    }

    private void addEntry(NodeStateEntry e) throws IOException {
        if (isMemoryLow()) {
            sortAndSaveBatch();
            reset();
        }

        long remainingNumberOfNodeStates = lastModifiedUpperBound - e.getLastModified();
        // check if this task can be split
        if (remainingNumberOfNodeStates > 1) {
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
                        newTasksQueue, phaser, nodeStateEntryTraverserFactory));
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

    private void reset() {
        entryBatch.clear();
        memoryUsed = 0;
        sufficientMemory.set(true);
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
        MultithreadedTraversalWithSortStrategy.DirectoryHelper.markCompletionTill(sortWorkDir,
                lastSavedNodeStateEntry.getLastModified());
        sortedFiles.add(newtmpfile);
    }

    private boolean isMemoryLow() {
        if (useMaxMemory){
            return memoryUsed > maxMemoryBytes;
        }
        return !sufficientMemory.get();
    }

    private void updateMemoryUsed(NodeStateHolder h) {
        memoryUsed += h.getMemorySize();
    }

    private void logFlags() {
        log.info("Starting task {}", taskID);
        log.info("{} Min heap memory (GB) to be required : {} ({})", taskID, minMemory, OAK_INDEXER_MIN_MEMORY);
        log.info("{} Max heap memory (GB) to be used for merge sort : {} ({})", taskID, maxMemory, OAK_INDEXER_MAX_SORT_MEMORY_IN_GB);
    }

    //~-------------------------------------< memory management >

    private void configureMemoryListener() {
        MemoryPoolMXBean pool = getMemoryPool();
        if (pool == null) {
            log.warn("{} Unable to setup monitoring of available memory. " +
                    "Would use configured maxMemory limit of {} GB", taskID, maxMemory);
            useMaxMemory = true;
            return;
        }

        NotificationEmitter emitter = (NotificationEmitter) getMemoryMXBean();
        MemoryListener listener = new MemoryListener();
        emitter.addNotificationListener(listener, null, null);
        MemoryUsage usage = pool.getCollectionUsage();
        long maxMemory = usage.getMax();
        long warningThreshold = minMemory * ONE_GB;
        if (warningThreshold > maxMemory) {
            log.warn("{} Configured minimum memory {} GB more than available memory ({})." +
                    "Overriding configuration accordingly.", taskID, minMemory, humanReadableByteCount(maxMemory));
            warningThreshold = maxMemory;
        }
        log.info("{} Setting up a listener to monitor pool '{}' and trigger batch save " +
                "if memory drop below {} GB (max {})", taskID, pool.getName(), minMemory, humanReadableByteCount(maxMemory));
        pool.setCollectionUsageThreshold(warningThreshold);
        checkMemory(usage);
    }

    private void checkMemory(MemoryUsage usage) {
        long maxMemory = usage.getMax();
        long usedMemory = usage.getUsed();
        long avail = maxMemory - usedMemory;
        if (avail > minMemoryBytes) {
            sufficientMemory.set(true);
            log.info("{} Available memory level {} is good. Current batch size {}", taskID, humanReadableByteCount(avail), entryBatch.size());
        } else {
            sufficientMemory.set(false);
            log.info("{} Available memory level {} (required {}) is low. Enabling flag to trigger batch save",
                    taskID, humanReadableByteCount(avail), minMemory);
        }
    }

    //Taken from GCMemoryBarrier
    private class MemoryListener implements NotificationListener {
        @Override
        public void handleNotification(Notification notification,
                                       Object handback) {
            if (notification
                    .getType()
                    .equals(MemoryNotificationInfo.MEMORY_COLLECTION_THRESHOLD_EXCEEDED)) {
                if (sufficientMemory.get()) {
                    CompositeData cd = (CompositeData) notification
                            .getUserData();
                    MemoryNotificationInfo info = MemoryNotificationInfo
                            .from(cd);
                    checkMemory(info.getUsage());
                }
            }
        }
    }

    private static MemoryPoolMXBean getMemoryPool() {
        long maxSize = 0;
        MemoryPoolMXBean maxPool = null;
        for (MemoryPoolMXBean pool : getMemoryPoolMXBeans()) {
            if (HEAP == pool.getType()
                    && pool.isCollectionUsageThresholdSupported()) {
                // Get usage after a GC, which is more stable, if available
                long poolSize = pool.getCollectionUsage().getMax();
                // Keep the pool with biggest size, by default it should be Old Gen Space
                if (poolSize > maxSize) {
                    maxPool = pool;
                }
            }
        }
        return maxPool;
    }
}
