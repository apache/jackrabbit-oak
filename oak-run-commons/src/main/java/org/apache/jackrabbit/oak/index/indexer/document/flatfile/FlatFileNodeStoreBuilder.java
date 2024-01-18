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

import com.mongodb.client.MongoDatabase;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.index.IndexHelper;
import org.apache.jackrabbit.oak.index.IndexerSupport;
import org.apache.jackrabbit.oak.index.indexer.document.CompositeException;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverserFactory;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.PipelinedStrategy;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreSortStrategy;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreUtils;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.query.NodeStateNodeTypeInfoProvider;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableSet;
import static org.apache.jackrabbit.guava.common.base.Preconditions.checkState;
import static org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreUtils.OAK_INDEXER_USE_LZ4;
import static org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreUtils.OAK_INDEXER_USE_ZIP;

/**
 * This class is where the strategy being selected for building FlatFileStore.
 */
public class FlatFileNodeStoreBuilder {

    private static final String FLAT_FILE_STORE_DIR_NAME_PREFIX = "flat-fs-";

    /**
     * System property name for sort strategy. Allowed values are the values from enum {@link SortStrategyType}
     */
    public static final String OAK_INDEXER_SORT_STRATEGY_TYPE = "oak.indexer.sortStrategyType";
    /**
     * System property to define the existing folder containing the flat file store files
     */
    public static final String OAK_INDEXER_SORTED_FILE_PATH = "oak.indexer.sortedFilePath";

    /**
     * Value of this system property indicates max memory that should be used if jmx based memory monitoring is not available.
     */
    public static final String OAK_INDEXER_MAX_SORT_MEMORY_IN_GB = "oak.indexer.maxSortMemoryInGB";
    public static final int OAK_INDEXER_MAX_SORT_MEMORY_IN_GB_DEFAULT = 2;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final File workDir;
    private final List<File> existingDataDumpDirs = new ArrayList<>();
    private Set<String> preferredPathElements = Collections.emptySet();
    private BlobStore blobStore;
    private NodeStateEntryWriter entryWriter;
    private NodeStateEntryTraverserFactory nodeStateEntryTraverserFactory;
    private long entryCount = 0;
    private File flatFileStoreDir;
    private Predicate<String> pathPredicate = path -> true;

    private final Compression algorithm = IndexStoreUtils.compressionAlgorithm();
    private final String sortStrategyTypeString = System.getProperty(OAK_INDEXER_SORT_STRATEGY_TYPE);
    private final SortStrategyType sortStrategyType = sortStrategyTypeString != null ?
            SortStrategyType.valueOf(sortStrategyTypeString) : SortStrategyType.PIPELINED;
    private RevisionVector rootRevision = null;
    private DocumentNodeStore nodeStore = null;
    private MongoDocumentStore mongoDocumentStore = null;
    private MongoDatabase mongoDatabase = null;
    private Set<IndexDefinition> indexDefinitions = null;
    private String checkpoint;
    private StatisticsProvider statisticsProvider = StatisticsProvider.NOOP;

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
         * System property {@link #OAK_INDEXER_SORT_STRATEGY_TYPE} if set to this value would result in {@link PipelinedStrategy} being used.
         */
        PIPELINED
    }

    public FlatFileNodeStoreBuilder(File workDir) {
        this.workDir = workDir;
    }

    public FlatFileNodeStoreBuilder withBlobStore(BlobStore blobStore) {
        this.blobStore = blobStore;
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

    public FlatFileNodeStoreBuilder withIndexDefinitions(Set<IndexDefinition> indexDefinitions) {
        this.indexDefinitions = indexDefinitions;
        return this;
    }


    public FlatFileNodeStoreBuilder withRootRevision(RevisionVector rootRevision) {
        this.rootRevision = rootRevision;
        return this;
    }

    public FlatFileNodeStoreBuilder withNodeStore(DocumentNodeStore nodeStore) {
        this.nodeStore = nodeStore;
        return this;
    }

    public FlatFileNodeStoreBuilder withMongoDocumentStore(MongoDocumentStore mongoDocumentStore) {
        this.mongoDocumentStore = mongoDocumentStore;
        return this;
    }

    public FlatFileNodeStoreBuilder withCheckpoint(String checkpoint) {
        this.checkpoint = checkpoint;
        return this;
    }

    public FlatFileNodeStoreBuilder withMongoDatabase(MongoDatabase mongoDatabase) {
        this.mongoDatabase = mongoDatabase;
        return this;
    }

    public FlatFileNodeStoreBuilder withStatisticsProvider(StatisticsProvider statisticsProvider) {
        this.statisticsProvider = statisticsProvider;
        return this;
    }

    public FlatFileStore build() throws IOException, CompositeException {
        logFlags();
        entryWriter = new NodeStateEntryWriter(blobStore);
        IndexStoreFiles indexStoreFiles = createdSortedStoreFiles();
        File metadataFile = indexStoreFiles.metadataFile;
        FlatFileStore store = new FlatFileStore(blobStore, indexStoreFiles.storeFiles.get(0), metadataFile,
                new NodeStateEntryReader(blobStore),
                unmodifiableSet(preferredPathElements), algorithm);
        if (entryCount > 0) {
            store.setEntryCount(entryCount);
        }
        return store;
    }

    public List<FlatFileStore> buildList(IndexHelper indexHelper, IndexerSupport indexerSupport,
                                         Set<IndexDefinition> indexDefinitions) throws IOException, CompositeException {
        logFlags();
        entryWriter = new NodeStateEntryWriter(blobStore);

        IndexStoreFiles indexStoreFiles = createdSortedStoreFiles();
        List<File> fileList = indexStoreFiles.storeFiles;
        File metadataFile = indexStoreFiles.metadataFile;

        long start = System.currentTimeMillis();
        // If not already split, split otherwise skip splitting
        if (!fileList.stream().allMatch(FlatFileSplitter.IS_SPLIT)) {
            NodeStore nodeStore = new MemoryNodeStore(indexerSupport.retrieveNodeStateForCheckpoint());
            FlatFileSplitter splitter = new FlatFileSplitter(fileList.get(0), indexHelper.getWorkDir(),
                    new NodeStateNodeTypeInfoProvider(nodeStore.getRoot()), new NodeStateEntryReader(blobStore),
                    indexDefinitions);
            fileList = splitter.split();
            log.info("Split flat file to result files '{}' is done, took {} ms", fileList, System.currentTimeMillis() - start);
        }

        List<FlatFileStore> storeList = new ArrayList<>();
        for (File flatFileItem : fileList) {
            FlatFileStore store = new FlatFileStore(blobStore, flatFileItem, metadataFile, new NodeStateEntryReader(blobStore),
                    unmodifiableSet(preferredPathElements), algorithm);
            storeList.add(store);
        }
        return storeList;
    }

    /**
     * Returns the existing list of store files if it can read from system property OAK_INDEXER_SORTED_FILE_PATH which
     * defines the existing folder where the flat file store files are present. Will throw an exception if it cannot
     * read or the path in the system property is not a directory.
     * If the system property OAK_INDEXER_SORTED_FILE_PATH in undefined, or it cannot read relevant files it
     * initializes the flat file store.
     *
     * @return pair of "list of flat files" and metadata file
     * @throws IOException
     * @throws CompositeException
     */
    private IndexStoreFiles createdSortedStoreFiles() throws IOException, CompositeException {
        // Check system property defined path
        String sortedFilePath = System.getProperty(OAK_INDEXER_SORTED_FILE_PATH);
        if (StringUtils.isNotBlank(sortedFilePath)) {
            File sortedDir = new File(sortedFilePath);
            log.info("Attempting to read from provided sorted files directory [{}] (via system property '{}')",
                    sortedDir.getAbsolutePath(), OAK_INDEXER_SORTED_FILE_PATH);
            // List of storefiles, List of metadatafile
            IndexStoreFiles storeFiles = getIndexStoreFiles(sortedDir);
            if (storeFiles != null) {
                return storeFiles;
            }
        }

        // Initialize the flat file store again

        createStoreDir();
        IndexStoreSortStrategy strategy = createSortStrategy(flatFileStoreDir);
        File result = strategy.createSortedStoreFile();
        File metadata = strategy.createMetadataFile();
        entryCount = strategy.getEntryCount();
        return new IndexStoreFiles(Collections.singletonList(result), metadata);
    }

    private static class IndexStoreFiles {
        private final List<File> storeFiles;
        private final File metadataFile;

        public IndexStoreFiles(List<File> storeFiles, File metadataFile) {
            this.storeFiles = storeFiles;
            this.metadataFile = metadataFile;
        }
    }

    private IndexStoreFiles getIndexStoreFiles(File sortedDir) {
        if (sortedDir.exists() && sortedDir.canRead() && sortedDir.isDirectory()) {
            File[] storeFiles = sortedDir.listFiles(
                    (dir, name) -> name.endsWith(IndexStoreUtils.getSortedStoreFileName(algorithm)));
            File[] metadataFiles = sortedDir.listFiles(
                    (dir, name) -> name.endsWith(IndexStoreUtils.getMetadataFileName(algorithm)));

            if (storeFiles != null && storeFiles.length != 0) {
                // Not throwing error for backward compatibility
                if (metadataFiles == null || metadataFiles.length == 0) {
                    log.error("Unable to find metadata file in directory:{}", sortedDir.getAbsolutePath());
                    return new IndexStoreFiles(Arrays.asList(storeFiles), null);
                } else {
                    checkState(metadataFiles.length == 1, "Multiple metadata files available at path:{}, metadataFiles:{}", sortedDir.getAbsolutePath(),
                            Arrays.asList(metadataFiles));
                    return new IndexStoreFiles(Arrays.asList(storeFiles), metadataFiles[0]);
                }
            }
        } else {
            String msg = String.format("Cannot read sorted files directory at [%s]", sortedDir.getAbsolutePath());
            throw new IllegalArgumentException(msg);
        }
        return null;
    }

    IndexStoreSortStrategy createSortStrategy(File dir) {
        switch (sortStrategyType) {
            case STORE_AND_SORT:
                log.info("Using StoreAndSortStrategy.");
                log.warn("StoreAndSortStrategy is deprecated and will be removed in the near future. Use PipelinedStrategy instead.");
                return new StoreAndSortStrategy(nodeStateEntryTraverserFactory, preferredPathElements, entryWriter, dir,
                        algorithm, pathPredicate, checkpoint);
            case TRAVERSE_WITH_SORT:
                log.info("Using TraverseWithSortStrategy");
                log.warn("TraverseWithSortStrategy is deprecated and will be removed in the near future. Use PipelinedStrategy instead.");
                return new TraverseWithSortStrategy(nodeStateEntryTraverserFactory, preferredPathElements, entryWriter, dir,
                        algorithm, pathPredicate, checkpoint);
            case PIPELINED:
                log.info("Using PipelinedStrategy");
                List<PathFilter> pathFilters = indexDefinitions.stream().map(IndexDefinition::getPathFilter).collect(Collectors.toList());
                return new PipelinedStrategy(mongoDocumentStore, mongoDatabase, nodeStore, rootRevision,
                        preferredPathElements, blobStore, dir, algorithm, pathPredicate, pathFilters, checkpoint, statisticsProvider);

        }
        throw new IllegalStateException("Not a valid sort strategy value " + sortStrategyType);
    }

    private void logFlags() {
        log.info("Preferred path elements are {}", Iterables.toString(preferredPathElements));
        log.info("Compression enabled while sorting : {} ({})", IndexStoreUtils.compressionEnabled(), OAK_INDEXER_USE_ZIP);
        log.info("LZ4 enabled for compression algorithm : {} ({})", IndexStoreUtils.useLZ4(), OAK_INDEXER_USE_LZ4);
        log.info("Sort strategy : {} ({})", sortStrategyType, OAK_INDEXER_SORT_STRATEGY_TYPE);
    }

    File createStoreDir() throws IOException {
        flatFileStoreDir = Files.createTempDirectory(workDir.toPath(), FLAT_FILE_STORE_DIR_NAME_PREFIX).toFile();
        return flatFileStoreDir;
    }

    /**
     * Returns the flat file store dir.
     * NOTE - Only works after flat file store dir has been built (i.e. after a call to {@link #build()}
     *
     * @return flat file store dir or <code>null</code> if it has not been built
     */
    public File getFlatFileStoreDir() {
        return flatFileStoreDir;
    }
}
