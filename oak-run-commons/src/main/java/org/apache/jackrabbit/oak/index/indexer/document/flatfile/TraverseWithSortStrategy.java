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
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.commons.sort.ExternalSort;
import org.apache.jackrabbit.oak.index.indexer.document.LastModifiedRange;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverser;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverserFactory;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentTraverser;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
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
import java.util.function.Predicate;

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
    private final NodeStateEntryTraverserFactory nodeStatesFactory;
    private final NodeStateEntryWriter entryWriter;
    private final File storeDir;
    private final Charset charset = UTF_8;
    private final Comparator<NodeStateHolder> comparator;
    private NotificationEmitter emitter;
    private MemoryListener listener;
    private final int maxMemory = Integer.getInteger(OAK_INDEXER_MAX_SORT_MEMORY_IN_GB, OAK_INDEXER_MAX_SORT_MEMORY_IN_GB_DEFAULT);
    private final long minMemory = Integer.getInteger(OAK_INDEXER_MIN_MEMORY, 2);
    private final Compression algorithm;
    /**
     * Max memory to be used if jmx based memory monitoring is not available. This value is not considered if jmx based
     * monitoring is available.
     */
    private final long maxMemoryBytes = maxMemory * ONE_GB;
    /**
     * When jmx based memory monitoring is available, this value indicates minimum memory which should be free/available for this
     * task to proceed.
     */
    private final long minMemoryBytes = minMemory * ONE_GB;
    private boolean useMaxMemory;
    private long entryCount;
    private long memoryUsed;
    private File sortWorkDir;
    private List<File> sortedFiles = new ArrayList<>();
    private ArrayList<NodeStateHolder> entryBatch = new ArrayList<>();
    private Predicate<String> pathPredicate;


    TraverseWithSortStrategy(NodeStateEntryTraverserFactory nodeStatesFactory, PathElementComparator pathComparator,
                             NodeStateEntryWriter entryWriter, File storeDir, Compression algorithm, Predicate<String> pathPredicate) {
        this.nodeStatesFactory = nodeStatesFactory;
        this.entryWriter = entryWriter;
        this.storeDir = storeDir;
        this.comparator = (e1, e2) -> pathComparator.compare(e1.getPathElements(), e2.getPathElements());
        this.pathPredicate = pathPredicate;
        this.algorithm = algorithm;
    }

    @Override
    public File createSortedStoreFile() throws IOException {
        try (NodeStateEntryTraverser nodeStates = nodeStatesFactory.create(new MongoDocumentTraverser.TraversingRange(new LastModifiedRange(0,
                Long.MAX_VALUE),null))) {
            logFlags();
            configureMemoryListener();
            sortWorkDir = createdSortWorkDir(storeDir);
            writeToSortedFiles(nodeStates);
            return sortStoreFile();
        }
    }

    @Override
    public long getEntryCount() {
        return entryCount;
    }

    private File sortStoreFile() throws IOException {
        log.info("Proceeding to perform merge of {} sorted files", sortedFiles.size());
        Stopwatch w = Stopwatch.createStarted();
        File sortedFile = new File(storeDir, getSortedStoreFileName(algorithm));
        try(BufferedWriter writer = createWriter(sortedFile, algorithm)) {
            Function<String, NodeStateHolder> func1 = (line) -> line == null ? null : new SimpleNodeStateHolder(line);
            Function<NodeStateHolder, String> func2 = holder -> holder == null ? null : holder.getLine();
            ExternalSort.mergeSortedFiles(sortedFiles,
                    writer,
                    comparator,
                    charset,
                    true, //distinct
                    algorithm,
                    func2,
                    func1
            );
        }
        log.info("Merging of sorted files completed in {}", w);
        return sortedFile;
    }

    private void writeToSortedFiles(NodeStateEntryTraverser nodeStates) throws IOException {
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

        String path = e.getPath();
        if (!NodeStateUtils.isHiddenPath(path) && pathPredicate.test(path)) {
            String jsonText = entryWriter.asJson(e.getNodeState());
            //Here logic differs from NodeStateEntrySorter in sense that
            //Holder line consist only of json and not 'path|json'
            NodeStateHolder h = new StateInBytesHolder(path, jsonText);
            entryBatch.add(h);
            updateMemoryUsed(h);
        }
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
        try (BufferedWriter writer = createWriter(newtmpfile, algorithm)) {
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
