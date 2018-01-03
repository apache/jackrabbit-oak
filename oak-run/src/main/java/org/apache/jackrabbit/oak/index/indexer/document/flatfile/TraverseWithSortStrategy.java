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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import com.google.common.base.Stopwatch;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.sort.ExternalSort;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Charsets.UTF_8;
import static java.lang.management.ManagementFactory.getMemoryMXBean;
import static java.lang.management.ManagementFactory.getMemoryPoolMXBeans;
import static java.lang.management.MemoryType.HEAP;
import static org.apache.commons.io.FileUtils.ONE_GB;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_MAX_SORT_MEMORY_IN_GB;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_MAX_SORT_MEMORY_IN_GB_DEFAULT;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.createWriter;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.getSortedStoreFileName;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.sizeOf;

class TraverseWithSortStrategy implements SortStrategy {
    private static final String OAK_INDEXER_MIN_MEMORY = "oak.indexer.minMemoryForWork";
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final AtomicBoolean sufficientMemory = new AtomicBoolean(true);
    private final Iterable<NodeStateEntry> nodeStates;
    private final NodeStateEntryWriter entryWriter;
    private final File storeDir;
    private final boolean compressionEnabled;
    private final Charset charset = UTF_8;
    private final Comparator<NodeStateHolder> comparator;
    private NotificationEmitter emitter;
    private MemoryListener listener;
    private final int maxMemory = Integer.getInteger(OAK_INDEXER_MAX_SORT_MEMORY_IN_GB, OAK_INDEXER_MAX_SORT_MEMORY_IN_GB_DEFAULT);
    private final long minMemory = Integer.getInteger(OAK_INDEXER_MIN_MEMORY, 2);
    private final long maxMemoryBytes = maxMemory * ONE_GB;
    private final long minMemoryBytes = minMemory * ONE_GB;
    private boolean useMaxMemory;
    private long entryCount;
    private long memoryUsed;
    private File sortWorkDir;
    private List<File> sortedFiles = new ArrayList<>();
    private ArrayList<NodeStateHolder> entryBatch = new ArrayList<>();


    TraverseWithSortStrategy(Iterable<NodeStateEntry> nodeStates, PathElementComparator pathComparator,
                             NodeStateEntryWriter entryWriter, File storeDir, boolean compressionEnabled) {
        this.nodeStates = nodeStates;
        this.entryWriter = entryWriter;
        this.storeDir = storeDir;
        this.compressionEnabled = compressionEnabled;
        this.comparator = (e1, e2) -> pathComparator.compare(e1.getPathElements(), e2.getPathElements());
    }

    @Override
    public File createSortedStoreFile() throws IOException {
        logFlags();
        configureMemoryListener();
        sortWorkDir = createdSortWorkDir(storeDir);
        writeToSortedFiles();
        return sortStoreFile();
    }

    @Override
    public long getEntryCount() {
        return entryCount;
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

    private void writeToSortedFiles() throws IOException {
        Stopwatch w = Stopwatch.createStarted();
        for (NodeStateEntry e : nodeStates) {
            entryCount++;
            addEntry(e);
        }

        //Save the last batch
        sortAndSaveBatch();

        //Free up the batch
        entryBatch.clear();
        entryBatch.trimToSize();

        log.info("Dumped {} nodestates in json format in {}",entryCount, w);
        log.info("Created {} sorted files of size {} to merge",
                sortedFiles.size(), humanReadableByteCount(sizeOf(sortedFiles)));
    }

    private void addEntry(NodeStateEntry e) throws IOException {
        if (isMemoryLow()) {
            sortAndSaveBatch();
            reset();
        }

        String jsonText = entryWriter.asJson(e.getNodeState());
        //Here logic differs from NodeStateEntrySorter in sense that
        //Holder line consist only of json and not 'path|json'
        NodeStateHolder h = new StateInBytesHolder(e.getPath(), jsonText);
        entryBatch.add(h);
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
        log.info("Sorted and stored batch of size {} (uncompressed {}) with {} entries in {}",
                humanReadableByteCount(newtmpfile.length()), humanReadableByteCount(textSize),entryBatch.size(), w);
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

    private static File createdSortWorkDir(File storeDir) throws IOException {
        File sortedFileDir = new File(storeDir, "sort-work-dir");
        FileUtils.forceMkdir(sortedFileDir);
        return sortedFileDir;
    }

    private void logFlags() {
        log.info("Min heap memory (GB) to be required : {} ({})", minMemory, OAK_INDEXER_MIN_MEMORY);
        log.info("Max heap memory (GB) to be used for merge sort : {} ({})", maxMemory, OAK_INDEXER_MAX_SORT_MEMORY_IN_GB);
    }

    //~-------------------------------------< memory management >

    private void configureMemoryListener() {
        MemoryPoolMXBean pool = getMemoryPool();
        if (pool == null) {
            log.warn("Unable to setup monitoring of available memory. " +
                    "Would use configured maxMemory limit of {} GB", maxMemory);
            useMaxMemory = true;
            return;
        }

        emitter = (NotificationEmitter) getMemoryMXBean();
        listener = new MemoryListener();
        emitter.addNotificationListener(listener, null, null);
        MemoryUsage usage = pool.getCollectionUsage();
        long maxMemory = usage.getMax();
        long warningThreshold = minMemory * ONE_GB;
        if (warningThreshold > maxMemory) {
            log.warn("Configured minimum memory {} GB more than available memory ({})." +
                    "Overriding configuration accordingly.", minMemory, humanReadableByteCount(maxMemory));
            warningThreshold = maxMemory;
        }
        log.info("Setting up a listener to monitor pool '{}' and trigger batch save " +
                "if memory drop below {} GB (max {})", pool.getName(), minMemory, humanReadableByteCount(maxMemory));
        pool.setCollectionUsageThreshold(warningThreshold);
        checkMemory(usage);
    }

    private void checkMemory(MemoryUsage usage) {
        long maxMemory = usage.getMax();
        long usedMemory = usage.getUsed();
        long avail = maxMemory - usedMemory;
        if (avail > minMemoryBytes) {
            sufficientMemory.set(true);
            log.info("Available memory level {} is good. Current batch size {}", humanReadableByteCount(avail), entryBatch.size());
        } else {
            sufficientMemory.set(false);
            log.info("Available memory level {} (required {}) is low. Enabling flag to trigger batch save",
                    humanReadableByteCount(avail), minMemory);
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
