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
package org.apache.jackrabbit.oak.index.indexer.document.incrementalstore;

import com.google.common.base.Stopwatch;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntrySorter;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.PathElementComparator;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreMetadataOperatorImpl;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.VisibleEditor;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_MAX_SORT_MEMORY_IN_GB;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_MAX_SORT_MEMORY_IN_GB_DEFAULT;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.getSortedStoreFileName;

public class IncrementalFlatFileStoreStrategy implements IncrementalIndexStoreSortStrategy {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private static final String STORE_TYPE = "IncrementalFlatFileStore";
    public static final String OAK_INDEXER_DELETE_ORIGINAL = "oak.indexer.deleteOriginal";
    private final String beforeCheckpoint;
    private final String afterCheckpoint;
    private final PathElementComparator comparator;
    private final IncrementalFlatFileStoreNodeStateEntryWriter entryWriter;
    private final File storeDir;
    private final NodeStore nodeStore;
    private final Compression algorithm;
    private final Predicate<String> pathPredicate;
    private final boolean deleteOriginal = Boolean.parseBoolean(System.getProperty(OAK_INDEXER_DELETE_ORIGINAL, "true"));
    private final int maxMemory = Integer.getInteger(OAK_INDEXER_MAX_SORT_MEMORY_IN_GB, OAK_INDEXER_MAX_SORT_MEMORY_IN_GB_DEFAULT);
    private long textSize = 0;
    private long entryCount = 0;
    private final Set<String> preferredPathElements;
    private final long maxDurationSeconds;

    public IncrementalFlatFileStoreStrategy(NodeStore nodeStore, @NotNull String beforeCheckpoint, @NotNull String afterCheckpoint, File storeDir,
                                            Set<String> preferredPathElements, @NotNull Compression algorithm,
                                            Predicate<String> pathPredicate, IncrementalFlatFileStoreNodeStateEntryWriter entryWriter, long maxDurationSeconds) {
        this.nodeStore = nodeStore;
        this.beforeCheckpoint = beforeCheckpoint;
        this.afterCheckpoint = afterCheckpoint;
        this.storeDir = storeDir;
        this.algorithm = algorithm;
        this.pathPredicate = pathPredicate;
        this.entryWriter = entryWriter;
        this.preferredPathElements = preferredPathElements;
        this.comparator = new PathElementComparator(preferredPathElements);
        this.maxDurationSeconds = maxDurationSeconds;
    }

    @Override
    public File createSortedStoreFile() throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        File file = new File(storeDir, getSortedStoreFileName(algorithm));
        try (BufferedWriter w = FlatFileStoreUtils.createWriter(file, algorithm)) {
            NodeState before = Objects.requireNonNull(nodeStore.retrieve(beforeCheckpoint));
            NodeState after = Objects.requireNonNull(nodeStore.retrieve(afterCheckpoint));
            Exception e = EditorDiff.process(VisibleEditor.wrap(new IncrementalFlatFileStoreEditor(w, entryWriter, pathPredicate, this, maxDurationSeconds)), before, after);
            if (e != null) {
                log.error("Exception while building incremental store for checkpoint before {}, after {}", beforeCheckpoint, afterCheckpoint, e);
                throw new RuntimeException(e);
            }
        }
        String sizeStr = algorithm.equals(Compression.NONE) ? "" : String.format("compressed/%s actual size", humanReadableByteCount(textSize));
        log.info("Dumped {} nodestates in json format in {} ({} {})", entryCount, sw, humanReadableByteCount(file.length()), sizeStr);
        return sortStoreFile(file);
    }

    @Override
    public File createMetadataFile() throws IOException {
        IncrementalIndexStoreMetadata indexStoreMetadata = new IncrementalIndexStoreMetadata(beforeCheckpoint, afterCheckpoint,
                getStoreType(), getStrategyName(), getPreferredPaths());
        File metadataFile = new IndexStoreMetadataOperatorImpl<IncrementalIndexStoreMetadata>().createMetadataFile(indexStoreMetadata, storeDir, algorithm);
        log.info("Created metadataFile:{} with strategy:{} ", metadataFile.getPath(), indexStoreMetadata.getStoreType());
        return metadataFile;
    }

    @Override
    public String getStrategyName() {
        return this.getClass().getSimpleName();
    }

    @Override
    public String getStoreType() {
        return STORE_TYPE;
    }

    @Override
    public String getBeforeCheckpoint() {
        return beforeCheckpoint;
    }

    @Override
    public String getAfterCheckpoint() {
        return afterCheckpoint;
    }

    @Override
    public Set<String> getPreferredPaths() {
        return preferredPathElements;
    }

    public Predicate<String> getPathPredicate() {
        return pathPredicate;
    }


    @Override
    public long getEntryCount() {
        return entryCount;
    }

    public void incrementEntryCount() {
        entryCount++;
    }

    public void setTextSize(long textSize) {
        this.textSize = textSize;
    }

    public long getTextSize() {
        return this.textSize;
    }

    private File sortStoreFile(File storeFile) throws IOException {
        File sortWorkDir = new File(storeFile.getParent(), "sort-work-dir");
        FileUtils.forceMkdir(sortWorkDir);
        File sortedFile = new File(storeFile.getParentFile(), getSortedStoreFileName(algorithm));
        NodeStateEntrySorter sorter =
                new NodeStateEntrySorter(comparator, storeFile, sortWorkDir, sortedFile);
        logFlags();
        sorter.setCompressionAlgorithm(algorithm);
        sorter.setMaxMemoryInGB(maxMemory);
        sorter.setDeleteOriginal(deleteOriginal);
        sorter.setActualFileSize(textSize);
        sorter.sort();
        return sorter.getSortedFile();
    }

    private void logFlags() {
        log.info("Delete original dump from traversal : {} ({})", deleteOriginal, OAK_INDEXER_DELETE_ORIGINAL);
        log.info("Max heap memory (GB) to be used for merge sort : {} ({})", maxMemory, OAK_INDEXER_MAX_SORT_MEMORY_IN_GB);
    }
}
