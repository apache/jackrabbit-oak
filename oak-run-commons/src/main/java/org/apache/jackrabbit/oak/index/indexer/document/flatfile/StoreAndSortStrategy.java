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

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.index.indexer.document.LastModifiedRange;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverser;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverserFactory;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreSortStrategyBase;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreUtils;
import org.apache.jackrabbit.oak.plugins.document.mongo.TraversingRange;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.function.Predicate;

import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_MAX_SORT_MEMORY_IN_GB;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_MAX_SORT_MEMORY_IN_GB_DEFAULT;

/**
 * @deprecated Use {@link PipelinedStrategy} instead
 */
@Deprecated
class StoreAndSortStrategy extends IndexStoreSortStrategyBase {
    private static final String OAK_INDEXER_DELETE_ORIGINAL = "oak.indexer.deleteOriginal";
    private static final int LINE_SEP_LENGTH = System.getProperty("line.separator").length();

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final NodeStateEntryTraverserFactory nodeStatesFactory;
    private final PathElementComparator comparator;
    private final NodeStateEntryWriter entryWriter;
    private long entryCount;
    private final boolean deleteOriginal = Boolean.parseBoolean(System.getProperty(OAK_INDEXER_DELETE_ORIGINAL, "true"));
    private final int maxMemory = Integer.getInteger(OAK_INDEXER_MAX_SORT_MEMORY_IN_GB, OAK_INDEXER_MAX_SORT_MEMORY_IN_GB_DEFAULT);
    private long textSize;

    public StoreAndSortStrategy(NodeStateEntryTraverserFactory nodeStatesFactory, Set<String> preferredPaths,
                                NodeStateEntryWriter entryWriter, File storeDir, Compression algorithm,
                                Predicate<String> pathPredicate, String checkpoint) {
        super(storeDir, algorithm, pathPredicate, preferredPaths, checkpoint);
        this.nodeStatesFactory = nodeStatesFactory;
        this.comparator = new PathElementComparator(preferredPaths);
        this.entryWriter = entryWriter;
    }

    /**
     *
     * @deprecated use {@link StoreAndSortStrategy#StoreAndSortStrategy(NodeStateEntryTraverserFactory, Set, NodeStateEntryWriter, File, Compression, Predicate, String)} instead
     */
    @Deprecated
    public StoreAndSortStrategy(NodeStateEntryTraverserFactory nodeStatesFactory, PathElementComparator comparator,
                                NodeStateEntryWriter entryWriter, File storeDir, Compression algorithm, Predicate<String> pathPredicate) {
        super(storeDir, algorithm, pathPredicate, null, null);
        this.nodeStatesFactory = nodeStatesFactory;
        this.comparator = comparator;
        this.entryWriter = entryWriter;
    }

    @Override
    public File createSortedStoreFile() throws IOException {
        try (NodeStateEntryTraverser nodeStates = nodeStatesFactory.create(
                new TraversingRange(new LastModifiedRange(0, Long.MAX_VALUE), null))
        ) {
            File storeFile = writeToStore(nodeStates, getStoreDir(), IndexStoreUtils.getSortedStoreFileName(getAlgorithm()));
            return sortStoreFile(storeFile);
        }
    }

    @Override
    public long getEntryCount() {
        return entryCount;
    }

    private File sortStoreFile(File storeFile) throws IOException {
        File sortWorkDir = new File(storeFile.getParent(), "sort-work-dir");
        FileUtils.forceMkdir(sortWorkDir);
        File sortedFile = new File(storeFile.getParentFile(), IndexStoreUtils.getSortedStoreFileName(getAlgorithm()));
        NodeStateEntrySorter sorter =
                new NodeStateEntrySorter(comparator, storeFile, sortWorkDir, sortedFile);

        logFlags();

        sorter.setCompressionAlgorithm(getAlgorithm());
        sorter.setMaxMemoryInGB(maxMemory);
        sorter.setDeleteOriginal(deleteOriginal);
        sorter.setActualFileSize(textSize);
        sorter.sort();
        return sorter.getSortedFile();
    }

    private File writeToStore(NodeStateEntryTraverser nodeStates, File dir, String fileName) throws IOException {
        entryCount = 0;
        File file = new File(dir, fileName);
        Stopwatch sw = Stopwatch.createStarted();
        try (BufferedWriter w = IndexStoreUtils.createWriter(file, getAlgorithm())) {
            for (NodeStateEntry e : nodeStates) {
                String path = e.getPath();
                if (!NodeStateUtils.isHiddenPath(path) && getPathPredicate().test(path)) {
                    String line = entryWriter.toString(e);
                    w.append(line);
                    w.newLine();
                    textSize += line.length() + LINE_SEP_LENGTH;
                    entryCount++;
                }
            }
        }
        String sizeStr = !getAlgorithm().equals(Compression.NONE) ? String.format("compressed/%s actual size", humanReadableByteCount(textSize)) : "";
        log.info("Dumped {} nodestates in json format in {} ({} {})", entryCount, sw, humanReadableByteCount(file.length()), sizeStr);
        return file;
    }

    private void logFlags() {
        log.info("Delete original dump from traversal : {} ({})", deleteOriginal, OAK_INDEXER_DELETE_ORIGINAL);
        log.info("Max heap memory (GB) to be used for merge sort : {} ({})", maxMemory, OAK_INDEXER_MAX_SORT_MEMORY_IN_GB);
    }
}
