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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.index.indexer.document.CompositeException;
import org.apache.jackrabbit.oak.index.indexer.document.LastModifiedRange;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntryTraverserFactory;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.unmodifiableSet;

public class FlatFileNodeStoreBuilder {

    private static final String FLAT_FILE_STORE_DIR_NAME_PREFIX = "flat-fs-";

    public static final String OAK_INDEXER_USE_ZIP = "oak.indexer.useZip";
    /**
     * System property name for sort strategy. If this is true, we use {@link MultithreadedTraverseWithSortStrategy}, else
     * {@link StoreAndSortStrategy} strategy is used.
     * NOTE - System property {@link #OAK_INDEXER_SORT_STRATEGY_TYPE} takes precedence over this one.
     */
    static final String OAK_INDEXER_TRAVERSE_WITH_SORT = "oak.indexer.traverseWithSortStrategy";
    /**
     * System property name for sort strategy. This takes precedence over {@link #OAK_INDEXER_TRAVERSE_WITH_SORT}.
     * Allowed values are the values from enum {@link SortStrategyType}
     */
    static final String OAK_INDEXER_SORT_STRATEGY_TYPE = "oak.indexer.sortStrategyType";
    private static final String OAK_INDEXER_SORTED_FILE_PATH = "oak.indexer.sortedFilePath";
    /**
     * Value of this system property indicates max memory that should be used if jmx based memory monitoring is not available.
     */
    static final String OAK_INDEXER_MAX_SORT_MEMORY_IN_GB = "oak.indexer.maxSortMemoryInGB";
    static final int OAK_INDEXER_MAX_SORT_MEMORY_IN_GB_DEFAULT = 2;
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

    private final boolean useZip = Boolean.parseBoolean(System.getProperty(OAK_INDEXER_USE_ZIP, "true"));
    private final boolean useTraverseWithSort = Boolean.parseBoolean(System.getProperty(OAK_INDEXER_TRAVERSE_WITH_SORT, "true"));
    private final String sortStrategyTypeString = System.getProperty(OAK_INDEXER_SORT_STRATEGY_TYPE);
    private final SortStrategyType sortStrategyType = sortStrategyTypeString != null ? SortStrategyType.valueOf(sortStrategyTypeString) :
            (useTraverseWithSort ? SortStrategyType.MULTITHREADED_TRAVERSE_WITH_SORT : SortStrategyType.STORE_AND_SORT);

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

    public FlatFileNodeStoreBuilder(File workDir) {
        this.workDir = workDir;
    }

    public FlatFileNodeStoreBuilder withLastModifiedBreakPoints(List<Long> lastModifiedBreakPoints) {
        this.lastModifiedBreakPoints = lastModifiedBreakPoints;
        return this;
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

    public FlatFileStore build() throws IOException, CompositeException {
        logFlags();
        comparator = new PathElementComparator(preferredPathElements);
        entryWriter = new NodeStateEntryWriter(blobStore);
        FlatFileStore store = new FlatFileStore(blobStore, createdSortedStoreFile(), new NodeStateEntryReader(blobStore),
                unmodifiableSet(preferredPathElements), useZip);
        if (entryCount > 0) {
            store.setEntryCount(entryCount);
        }
        return store;
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
                return new StoreAndSortStrategy(nodeStateEntryTraverserFactory.create(new LastModifiedRange(0,
                        Long.MAX_VALUE)), comparator, entryWriter, dir, useZip);
            case TRAVERSE_WITH_SORT:
                log.info("Using TraverseWithSortStrategy");
                return new TraverseWithSortStrategy(nodeStateEntryTraverserFactory.create(new LastModifiedRange(0,
                        Long.MAX_VALUE)), comparator, entryWriter, dir, useZip);
            default:
                log.info("Using MultithreadedTraverseWithSortStrategy");
                return new MultithreadedTraverseWithSortStrategy(nodeStateEntryTraverserFactory, lastModifiedBreakPoints, comparator,
                        blobStore, dir, existingDataDumpDirs, useZip, getMemoryManager());
        }
    }

    MemoryManager getMemoryManager() {
        return new DefaultMemoryManager();
    }

    private void logFlags() {
        log.info("Preferred path elements are {}", Iterables.toString(preferredPathElements));
        log.info("Compression enabled while sorting : {} ({})", useZip, OAK_INDEXER_USE_ZIP);
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
