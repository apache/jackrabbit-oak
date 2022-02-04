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

import java.io.*;
import java.lang.management.MemoryNotificationInfo;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.IntStream;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import com.google.common.base.Stopwatch;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.sort.ExternalSort;
import org.apache.jackrabbit.oak.index.indexer.document.LastModifiedRange;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverser;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverserFactory;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentTraverser;
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
    private final NodeStateEntryTraverserFactory nodeStatesFactory;
    private final NodeStateEntryWriter entryWriter;
    private final File storeDir;
    private final boolean compressionEnabled;
    private final Charset charset = UTF_8;
    private final Comparator<NodeStateHolder> comparator;
    private final Set<String> preferred;
    private NotificationEmitter emitter;
    private MemoryListener listener;
    private final int maxMemory = Integer.getInteger(OAK_INDEXER_MAX_SORT_MEMORY_IN_GB, OAK_INDEXER_MAX_SORT_MEMORY_IN_GB_DEFAULT);
    private final long minMemory = Integer.getInteger(OAK_INDEXER_MIN_MEMORY, 2);
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


    TraverseWithSortStrategy(NodeStateEntryTraverserFactory nodeStatesFactory, PathElementComparator pathComparator,
                             NodeStateEntryWriter entryWriter, File storeDir, boolean compressionEnabled) {
        this.nodeStatesFactory = nodeStatesFactory;
        this.entryWriter = entryWriter;
        this.storeDir = storeDir;
        this.compressionEnabled = compressionEnabled;
        this.comparator = (e1, e2) -> pathComparator.compare(e1.getPathElements(), e2.getPathElements());
        this.preferred = pathComparator == null ? new HashSet<>() : pathComparator.getPreferred();
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
        File sortedFile = new File(storeDir, getSortedStoreFileName(compressionEnabled));
        File serializedSortedFile = new File(storeDir, String.format("serialized-%s", getSortedStoreFileName(false)));

//        try(BufferedWriter writer = createWriter(sortedFile, compressionEnabled)) {
//            Function<String, NodeStateHolder> func1 = (line) -> line == null ? null : new SimpleNodeStateHolder(line);
//            Function<NodeStateHolder, String> func2 = holder -> holder == null ? null : holder.getLine();
//            ExternalSort.mergeSortedFiles(sortedFiles,
//                    writer,
//                    comparator,
//                    charset,
//                    true, //distinct
//                    compressionEnabled, //useZip
//                    func2,
//                    func1
//            );
//        }

        List<String> commands = new ArrayList<String>();
        Collections.addAll(commands, "/usr/bin/sort");
        Collections.addAll(commands, "-T", storeDir.getAbsolutePath());
        Collections.addAll(commands, "-S", "2G");
        Collections.addAll(commands, "-o", serializedSortedFile.getAbsolutePath());
        Collections.addAll(commands, "-t", "/");
        // Max depth of 100 level
        IntStream.range(1, 50).forEach(i -> Collections.addAll(commands, String.format("-k%s,%s", i, i)));
        if (compressionEnabled) {
            Collections.addAll(commands, "--compress-program", "gzip");
            Collections.addAll(commands, "-m");
            sortedFiles.forEach(f -> commands.add(String.format("<(gunzip -c %s)", f.getAbsolutePath())));
        } else {
            Collections.addAll(commands, "-m");
            sortedFiles.forEach(f -> commands.add(f.getAbsolutePath()));
        }

        ProcessBuilder pb = new ProcessBuilder("/bin/bash", "-c", String.join(" ", commands));
        log.info("Running merge command {}", pb.command());
        Process p = pb.start();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
             BufferedReader errReader = new BufferedReader(new InputStreamReader(p.getErrorStream()))) {
            String line;
            p.waitFor();
            log.info("Merging of sorted files completed in {}", w);
            while ((line = reader.readLine()) != null) log.info(line);
            Boolean hasError = false;
            while ((line = errReader.readLine()) != null)  {
                log.error(line);
                hasError = true;
            }
            if (hasError) throw new Exception("command execution fail");
            log.info("Sort command executed successfully");
        } catch (Exception e) {
            throw new RuntimeException(String.format("Error while running command %s", pb.command()));
        }

        Stopwatch wDeserialize = Stopwatch.createStarted();
        try (BufferedReader reader = FlatFileStoreUtils.createReader(serializedSortedFile, false);
             BufferedWriter writer = FlatFileStoreUtils.createWriter(sortedFile, compressionEnabled)) {
            String line = reader.readLine();
            while (line != null) {
                String deserializeLine = entryWriter.deserialize(line);
                writer.write(deserializeLine);
                writer.newLine();
                line = reader.readLine();
            }
        }
        log.info("Deserialize of sorted file completed in {}", wDeserialize);
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
                //String text = entryWriter.toString(h.getPathElements(), h.getLine());
                String text = entryWriter.serialize(h.getPathElements(), h.getLine(), preferred);
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
