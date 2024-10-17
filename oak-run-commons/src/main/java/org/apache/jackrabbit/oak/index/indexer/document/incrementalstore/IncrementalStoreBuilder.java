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

import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.index.IndexHelper;
import org.apache.jackrabbit.oak.index.indexer.document.CompositeException;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStore;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreUtils;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import static java.util.Collections.unmodifiableSet;
import static org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreUtils.OAK_INDEXER_USE_LZ4;
import static org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreUtils.OAK_INDEXER_USE_ZIP;

public class IncrementalStoreBuilder {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private static final String INCREMENTAL_STORE_DIR_NAME_PREFIX = "inc-store";

    private final File workDir;
    private final IndexHelper indexHelper;
    private final String initialCheckpoint;
    private final String finalCheckpoint;
    private Predicate<String> pathPredicate = path -> true;
    private Set<String> preferredPathElements = Collections.emptySet();
    private BlobStore blobStore;

    private final Compression algorithm = IndexStoreUtils.compressionAlgorithm();

    /**
     * System property name for sort strategy. This takes precedence over {@link #INCREMENTAL_SORT_STRATEGY_TYPE}.
     * Allowed values are the values from enum {@link IncrementalStoreBuilder.IncrementalSortStrategyType}
     */
    public static final String INCREMENTAL_SORT_STRATEGY_TYPE = "oak.indexer.incrementalSortStrategyType";


    private final String sortStrategyTypeString = System.getProperty(INCREMENTAL_SORT_STRATEGY_TYPE);
    private IncrementalStoreBuilder.IncrementalSortStrategyType sortStrategyType = sortStrategyTypeString != null
            ? IncrementalStoreBuilder.IncrementalSortStrategyType.valueOf(sortStrategyTypeString)
            : IncrementalSortStrategyType.INCREMENTAL_FFS_STORE;


    public enum IncrementalSortStrategyType {
        /**
         * Incremental store having nodes updated between initial and final checkpoint
         */

        INCREMENTAL_FFS_STORE,

        INCREMENTAL_TREE_STORE
    }

    public IncrementalStoreBuilder(File workDir, IndexHelper indexHelper,
                                   @NotNull String initialCheckpoint, @NotNull String finalCheckpoint) {
        this.workDir = workDir;
        this.indexHelper = indexHelper;
        this.initialCheckpoint = Objects.requireNonNull(initialCheckpoint);
        this.finalCheckpoint = Objects.requireNonNull(finalCheckpoint);
    }

    public IncrementalStoreBuilder withPreferredPathElements(Set<String> preferredPathElements) {
        this.preferredPathElements = preferredPathElements;
        return this;
    }

    public IncrementalStoreBuilder withSortStrategyType(IncrementalStoreBuilder.IncrementalSortStrategyType sortStrategyType) {
        this.sortStrategyType = sortStrategyType;
        return this;
    }

    public IncrementalStoreBuilder withPathPredicate(Predicate<String> pathPredicate) {
        this.pathPredicate = pathPredicate;
        return this;
    }

    public IncrementalStoreBuilder withBlobStore(BlobStore blobStore) {
        this.blobStore = blobStore;
        return this;
    }


    public IndexStore build() throws IOException, CompositeException {
        logFlags();
        File dir = createStoreDir();
        Objects.requireNonNull(sortStrategyType);
        if (sortStrategyType == IncrementalSortStrategyType.INCREMENTAL_FFS_STORE ||
                sortStrategyType == IncrementalSortStrategyType.INCREMENTAL_TREE_STORE) {
            IncrementalFlatFileStoreNodeStateEntryWriter entryWriter = new IncrementalFlatFileStoreNodeStateEntryWriter(blobStore);
            IncrementalIndexStoreSortStrategy strategy = new IncrementalFlatFileStoreStrategy(
                    indexHelper.getNodeStore(),
                    initialCheckpoint,
                    finalCheckpoint,
                    dir, preferredPathElements, algorithm, pathPredicate, entryWriter);
            File metadataFile = strategy.createMetadataFile();
            File incrementalStoreFile = strategy.createSortedStoreFile();
            long entryCount = strategy.getEntryCount();
            IndexStore store = new IncrementalFlatFileStore(blobStore, incrementalStoreFile, metadataFile,
                    new IncrementalFlatFileStoreNodeStateEntryReader(blobStore),
                    unmodifiableSet(preferredPathElements), algorithm);
            if (entryCount > 0) {
                store.setEntryCount(entryCount);
            }
            return store;
        }
        throw new IllegalStateException("Not a valid sort strategy value " + sortStrategyType);
    }

    private File createStoreDir() throws IOException {
        return Files.createTempDirectory(workDir.toPath(), getDirNamePrefix()).toFile();
    }

    private String getDirNamePrefix() {
        return INCREMENTAL_STORE_DIR_NAME_PREFIX + sortStrategyType;
    }

    private void logFlags() {
        log.info("Preferred path elements are {}", Iterables.toString(preferredPathElements));
        log.info("Compression enabled while sorting : {} ({})", IndexStoreUtils.compressionEnabled(), OAK_INDEXER_USE_ZIP);
        log.info("LZ4 enabled for compression algorithm : {} ({})", IndexStoreUtils.useLZ4(), OAK_INDEXER_USE_LZ4);
    }
}
