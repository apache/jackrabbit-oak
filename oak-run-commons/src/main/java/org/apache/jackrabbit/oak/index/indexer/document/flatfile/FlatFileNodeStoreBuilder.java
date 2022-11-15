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

import com.google.common.collect.Iterables;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.index.IndexHelper;
import org.apache.jackrabbit.oak.index.IndexerSupport;
import org.apache.jackrabbit.oak.index.indexer.document.CompositeException;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverserFactory;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.query.NodeStateNodeTypeInfoProvider;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import static java.util.Collections.unmodifiableSet;

/**
 * This class is where the strategy being selected for building FlatFileStore.
 */
public class FlatFileNodeStoreBuilder {

    private static final String FLAT_FILE_STORE_DIR_NAME_PREFIX = "flat-fs-";

    public static final String OAK_INDEXER_USE_ZIP = "oak.indexer.useZip";
    public static final String OAK_INDEXER_USE_LZ4 = "oak.indexer.useLZ4";
    /**
     * System property name for sort strategy. If this is true, we use {@link MultithreadedTraverseWithSortStrategy}, else
     * {@link StoreAndSortStrategy} strategy is used.
     * NOTE - System property {@link #OAK_INDEXER_SORT_STRATEGY_TYPE} takes precedence over this one.
     */
    public static final String OAK_INDEXER_TRAVERSE_WITH_SORT = "oak.indexer.traverseWithSortStrategy";
    /**
     * System property name for sort strategy. This takes precedence over {@link #OAK_INDEXER_TRAVERSE_WITH_SORT}.
     * Allowed values are the values from enum {@link SortStrategyType}
     */
    public static final String OAK_INDEXER_SORT_STRATEGY_TYPE = "oak.indexer.sortStrategyType";
    public static final String OAK_INDEXER_SORTED_FILE_PATH = "oak.indexer.sortedFilePath";

    /**
     * Default value for {@link #PROP_THREAD_POOL_SIZE}
     */
    static final int DEFAULT_NUMBER_OF_DATA_DUMP_THREADS = 8;
    /**
     * System property for specifying number of threads for parallel download when using {@link MultithreadedTraverseWithSortStrategy}
     */
    static final String PROP_THREAD_POOL_SIZE = "oak.indexer.dataDumpThreadPoolSize";

    /**
     * Default value for {@link #PROP_MERGE_THREAD_POOL_SIZE}
     */
    static final int DEFAULT_NUMBER_OF_MERGE_TASK_THREADS = 1;
    /**
     * System property for specifying number of threads for parallel merge when using {@link MultithreadedTraverseWithSortStrategy}
     */
    static final String PROP_MERGE_THREAD_POOL_SIZE = "oak.indexer.mergeTaskThreadPoolSize";

    /**
     * Default value for {@link #PROP_MERGE_TASK_BATCH_SIZE}
     */
    static final int DEFAULT_NUMBER_OF_FILES_PER_MERGE_TASK = 64;
    /**
     * System property for specifying number of files for batch merge task when using {@link MultithreadedTraverseWithSortStrategy}
     */
    static final String PROP_MERGE_TASK_BATCH_SIZE = "oak.indexer.mergeTaskBatchSize";
    
    /**
     * Value of this system property indicates max memory that should be used if jmx based memory monitoring is not available.
     */
    static final String OAK_INDEXER_MAX_SORT_MEMORY_IN_GB = "oak.indexer.maxSortMemoryInGB";
    static final int OAK_INDEXER_MAX_SORT_MEMORY_IN_GB_DEFAULT = 2;
    static final String OAK_INDEXER_DUMP_THRESHOLD_IN_MB = "oak.indexer.dumpThresholdInMB";
    static final int OAK_INDEXER_DUMP_THRESHOLD_IN_MB_DEFAULT = 16;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private List<Long> lastModifiedBreakPoints;
    private final File workDir;
    private final List<File> existingDataDumpDirs = new ArrayList<>();
    private Set<String> preferredPathElements = Collections.emptySet();
    private BlobStore blobStore;
    private PathElementComparator comparator;
    private NodeStateEntryWriter entryWriter;
    private NodeStateEntryTraverserFactory nodeStateEntryTraverserFactory;
    private long entryCount = 0;
    private File flatFileStoreDir;
    private final MemoryManager memoryManager;
    private long dumpThreshold = Integer.getInteger(OAK_INDEXER_DUMP_THRESHOLD_IN_MB, OAK_INDEXER_DUMP_THRESHOLD_IN_MB_DEFAULT) * FileUtils.ONE_MB;
    private Predicate<String> pathPredicate = path -> true;

    private final boolean compressionEnabled = Boolean.parseBoolean(System.getProperty(OAK_INDEXER_USE_ZIP, "true"));
    private final boolean useLZ4 = Boolean.parseBoolean(System.getProperty(OAK_INDEXER_USE_LZ4, "false"));
    private final Compression algorithm = compressionEnabled ? (useLZ4 ? new LZ4Compression() : Compression.GZIP) :
        Compression.NONE;
    private final boolean useTraverseWithSort = Boolean.parseBoolean(System.getProperty(OAK_INDEXER_TRAVERSE_WITH_SORT, "true"));
    private final String sortStrategyTypeString = System.getProperty(OAK_INDEXER_SORT_STRATEGY_TYPE);
    private final SortStrategyType sortStrategyType = sortStrategyTypeString != null ? SortStrategyType.valueOf(sortStrategyTypeString) :
            (useTraverseWithSort ? SortStrategyType.TRAVERSE_WITH_SORT : SortStrategyType.STORE_AND_SORT);

    public enum SortStrategyType {
        /**
         * System property {@link #OAK_INDEXER_SORT_STRATEGY_TYPE} if set to this value would result in {@link StoreAndSortStrategy} being used.
         */
        STORE_AND_SORT,
        /**
         * System property {@link #OAK_INDEXER_SORT_STRATEGY_TYPE} if set to this value would result in {@link TraverseWithSortStrategy} being used.
         */
        TRAVERSE_WITH_SORT,
        /**
         * System property {@link #OAK_INDEXER_SORT_STRATEGY_TYPE} if set to this value would result in {@link MultithreadedTraverseWithSortStrategy} being used.
         */
        MULTITHREADED_TRAVERSE_WITH_SORT
    }

    public FlatFileNodeStoreBuilder(File workDir, MemoryManager memoryManager) {
        this.workDir = workDir;
        this.memoryManager = memoryManager;
    }

    public FlatFileNodeStoreBuilder(File workDir) {
        this.workDir = workDir;
        this.memoryManager = new DefaultMemoryManager();
    }

    public FlatFileNodeStoreBuilder withLastModifiedBreakPoints(List<Long> lastModifiedBreakPoints) {
        this.lastModifiedBreakPoints = lastModifiedBreakPoints;
        return this;
    }

    public FlatFileNodeStoreBuilder withBlobStore(BlobStore blobStore) {
        this.blobStore = blobStore;
        return this;
    }

    public FlatFileNodeStoreBuilder withDumpThreshold(long dumpThreshold) {
        this.dumpThreshold = dumpThreshold;
        return this;
    }

    public FlatFileNodeStoreBuilder withPreferredPathElements(Set<String> preferredPathElements) {
        this.preferredPathElements = preferredPathElements;
        return this;
    }

    public FlatFileNodeStoreBuilder addExistingDataDumpDir(File existingDataDumpDir) {
        if (existingDataDumpDir != null) {
            this.existingDataDumpDirs.add(existingDataDumpDir);
        }
        return this;
    }

    public FlatFileNodeStoreBuilder withNodeStateEntryTraverserFactory(NodeStateEntryTraverserFactory factory) {
        this.nodeStateEntryTraverserFactory = factory;
        return this;
    }

    public FlatFileNodeStoreBuilder withPathPredicate(Predicate<String> pathPredicate) {
        this.pathPredicate = pathPredicate;
        return this;
    }

    public FlatFileStore build() throws IOException, CompositeException {
        logFlags();
        comparator = new PathElementComparator(preferredPathElements);
        entryWriter = new NodeStateEntryWriter(blobStore);
        FlatFileStore store = new FlatFileStore(blobStore, createdSortedStoreFile(), new NodeStateEntryReader(blobStore),
                unmodifiableSet(preferredPathElements), algorithm);
        if (entryCount > 0) {
            store.setEntryCount(entryCount);
        }
        return store;
    }

    public List<FlatFileStore> buildList(IndexHelper indexHelper, IndexerSupport indexerSupport,
            Set<IndexDefinition> indexDefinitions) throws IOException, CompositeException {
        logFlags();
        comparator = new PathElementComparator(preferredPathElements);
        entryWriter = new NodeStateEntryWriter(blobStore);

        File flatStoreFile = createdSortedStoreFile();
        long start = System.currentTimeMillis();
        NodeStore nodeStore = new MemoryNodeStore(indexerSupport.retrieveNodeStateForCheckpoint());
        FlatFileSplitter splitter = new FlatFileSplitter(flatStoreFile, indexHelper.getWorkDir(), new NodeStateNodeTypeInfoProvider(nodeStore.getRoot()), new NodeStateEntryReader(blobStore),
                indexDefinitions);
        List<File> fileList = splitter.split();
        log.info("Split flat file to result files '{}' is done, took {} ms", fileList, System.currentTimeMillis() - start);

        List<FlatFileStore> storeList = new ArrayList<>();
        for (File flatFileItem : fileList) {
            FlatFileStore store = new FlatFileStore(blobStore, flatFileItem, new NodeStateEntryReader(blobStore),
                    unmodifiableSet(preferredPathElements), algorithm);
            storeList.add(store);
        }
        return storeList;
    }

    private File createdSortedStoreFile() throws IOException, CompositeException {
        String sortedFilePath = System.getProperty(OAK_INDEXER_SORTED_FILE_PATH);
        if (sortedFilePath != null) {
            File sortedFile = new File(sortedFilePath);
            if (sortedFile.exists() && sortedFile.isFile() && sortedFile.canRead()) {
                log.info("Reading from provided sorted file [{}] (via system property '{}')",
                        sortedFile.getAbsolutePath(), OAK_INDEXER_SORTED_FILE_PATH);
                return sortedFile;
            } else {
                String msg = String.format("Cannot read sorted file at [%s] configured via system property '%s'",
                        sortedFile.getAbsolutePath(), OAK_INDEXER_SORTED_FILE_PATH);
                throw new IllegalArgumentException(msg);
            }
        } else {
            createStoreDir();
            org.apache.jackrabbit.oak.index.indexer.document.flatfile.SortStrategy strategy = createSortStrategy(flatFileStoreDir);
            File result = strategy.createSortedStoreFile();
            entryCount = strategy.getEntryCount();
            return result;
        }
    }

    SortStrategy createSortStrategy(File dir) throws IOException {
        switch (sortStrategyType) {
            case STORE_AND_SORT:
                log.info("Using StoreAndSortStrategy");
                return new StoreAndSortStrategy(nodeStateEntryTraverserFactory, comparator, entryWriter, dir, algorithm, pathPredicate);
            case TRAVERSE_WITH_SORT:
                log.info("Using TraverseWithSortStrategy");
                return new TraverseWithSortStrategy(nodeStateEntryTraverserFactory, comparator, entryWriter, dir, algorithm, pathPredicate);
            case MULTITHREADED_TRAVERSE_WITH_SORT:
                log.info("Using MultithreadedTraverseWithSortStrategy");
                return new MultithreadedTraverseWithSortStrategy(nodeStateEntryTraverserFactory, lastModifiedBreakPoints, comparator,
                        blobStore, dir, existingDataDumpDirs, algorithm, memoryManager, dumpThreshold, pathPredicate);
        }
        throw new IllegalStateException("Not a valid sort strategy value " + sortStrategyType);
    }

    private void logFlags() {
        log.info("Preferred path elements are {}", Iterables.toString(preferredPathElements));
        log.info("Compression enabled while sorting : {} ({})", compressionEnabled, OAK_INDEXER_USE_ZIP);
        log.info("LZ4 enabled for compression algorithm : {} ({})", useLZ4, OAK_INDEXER_USE_LZ4);
        log.info("Sort strategy : {} ({})", sortStrategyType, OAK_INDEXER_TRAVERSE_WITH_SORT);
    }

    File createStoreDir() throws IOException {
        flatFileStoreDir = Files.createTempDirectory(workDir.toPath(), FLAT_FILE_STORE_DIR_NAME_PREFIX).toFile();
        return flatFileStoreDir;
    }

    /**
     * Returns the flat file store dir.
     * NOTE - Only works after flat file store dir has been built (i.e. after a call to {@link #build()}
     * @return flat file store dir or <code>null</code> if it has not been built
     */
    public File getFlatFileStoreDir() {
        return flatFileStoreDir;
    }
}
