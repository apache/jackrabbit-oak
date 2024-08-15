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

import static org.apache.jackrabbit.guava.common.base.Preconditions.checkState;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryReader;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreMetadata;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreMetadataOperatorImpl;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreUtils;
import org.apache.jackrabbit.oak.index.indexer.document.tree.TreeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;


public class MergeIncrementalTreeStore implements MergeIncrementalStore {

    private static final String MERGE_BASE_AND_INCREMENTAL_TREE_STORE = "MergeBaseAndIncrementalTreeStore";
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(MergeIncrementalTreeStore.class);

    private final File baseDir;
    private final File incrementalFile;
    private final File mergedDir;
    private final Compression algorithm;

    private final static Map<String, IncrementalStoreOperand> OPERATION_MAP = Arrays.stream(IncrementalStoreOperand.values())
            .collect(Collectors.toUnmodifiableMap(IncrementalStoreOperand::toString, k -> IncrementalStoreOperand.valueOf(k.name())));


    public MergeIncrementalTreeStore(File baseDir, File incrementalFile, File mergedDir, Compression algorithm) throws IOException {
        this.baseDir = baseDir;
        this.incrementalFile = incrementalFile;
        this.mergedDir = mergedDir;
        this.algorithm = algorithm;
        if (mergedDir.exists()) {
            LOG.warn("merged dir {} exists; it will be replaced", mergedDir.getAbsolutePath());
        } else {
            Files.createDirectories(mergedDir.toPath());
        }
    }

    @Override
    public void doMerge() throws IOException {
        LOG.info("base dir " + baseDir.getAbsolutePath());
        LOG.info("incremental file " + incrementalFile.getAbsolutePath());
        LOG.info("merged dir " + mergedDir.getAbsolutePath());
        mergeMetadataFiles();
        mergeIndexStore();
    }

    @Override
    public String getStrategyName() {
        return MERGE_BASE_AND_INCREMENTAL_TREE_STORE;
    }

    /**
     * Merges multiple index store files.
     *
     * This method is a little verbose, but I think this is fine
     * as we are not getting consistent data from checkpoint diff
     * and we need to handle cases differently.
     */
    private void mergeIndexStore() throws IOException {
        TreeStore baseStore = new TreeStore("base", baseDir, new NodeStateEntryReader(null), 10);
        TreeStore mergedStore = new TreeStore("merged", mergedDir, new NodeStateEntryReader(null), 10);
        mergedStore.getSession().init();
        Iterator<Map.Entry<String, String>> baseIt = baseStore.getSession().iterator();
        try (BufferedReader incrementalReader = IndexStoreUtils.createReader(incrementalFile, algorithm)) {
            StoreEntry base = StoreEntry.readFromTreeStore(baseIt);
            StoreEntry increment = StoreEntry.readFromReader(incrementalReader);
            while (base != null || increment != null) {
                // which one to advance at the end of the loop
                boolean advanceBase, advanceIncrement;
                // the entry to write (or null, in case of a delete)
                StoreEntry write;
                if (base == null) {
                    // base EOF: we expect ADD
                    if (increment.operation != IncrementalStoreOperand.ADD) {
                        LOG.warn(
                                "Expected ADD but got {} for incremental path {} value {}. "
                                        + "Merging will proceed, but this is unexpected.",
                                increment.operation, increment.path, increment.value);
                    }
                    write = increment;
                    advanceBase = false;
                    advanceIncrement = true;
                } else if (increment == null) {
                    // increment EOF: copy from base
                    write = base;
                    advanceBase = true;
                    advanceIncrement = false;
                } else {
                    // both base and increment (normal case)
                    int compare = base.path.compareTo(increment.path);
                    if (compare < 0) {
                        // base path is smaller
                        write = base;
                        advanceBase = true;
                        advanceIncrement = false;
                    } else if (compare > 0) {
                        // increment path is smaller: we expect ADD
                        if (increment.operation != IncrementalStoreOperand.ADD) {
                            LOG.warn("Expected ADD but got {} for incremental path {} value {}. " +
                                    "Merging will proceed, but this is unexpected.",
                                    increment.operation, increment.path, increment.value);
                          }
                        write = increment;
                        advanceBase = false;
                        advanceIncrement = true;
                    } else {
                        // both paths are the same: we expect modify or delete
                        write = increment;
                        advanceBase = true;
                        advanceIncrement = true;
                        switch (increment.operation) {
                        case ADD:
                            LOG.warn("Expected MODIFY/DELETE but got {} for incremental path {} value {}. " +
                                    "Merging will proceed, but this is unexpected.",
                                    increment.operation, increment.path, increment.value);
                            break;
                        case MODIFY:
                            break;
                        case DELETE:
                            write = null;
                        }
                    }
                }
                if (write != null) {
                    mergedStore.getSession().put(write.path, write.value);
                }
                if (advanceBase) {
                    base = StoreEntry.readFromTreeStore(baseIt);
                }
                if (advanceIncrement) {
                    increment = StoreEntry.readFromReader(incrementalReader);
                }
            }
        }
        baseStore.close();
        mergedStore.getSession().flush();
        mergedStore.close();
    }

        static class StoreEntry {
            final String path;
            final String value;
            final IncrementalStoreOperand operation;

            StoreEntry(String path, String value, IncrementalStoreOperand operation) {
                this.path = path;
                this.value = value;
                this.operation = operation;
            }

            static StoreEntry readFromTreeStore(Iterator<Map.Entry<String, String>> it) {
                while (it.hasNext()) {
                    Map.Entry<String, String> e = it.next();
                    if (!e.getValue().isEmpty()) {
                        return new StoreEntry(e.getKey(), e.getValue(), null);
                    }
                }
                return null;
            }

            static StoreEntry readFromReader(BufferedReader reader) throws IOException {
                String line = reader.readLine();
                if (line == null) {
                    return null;
                }
                String[] parts = IncrementalFlatFileStoreNodeStateEntryWriter.getParts(line);
                return new StoreEntry(parts[0], parts[1], OPERATION_MAP.get(parts[3]));
            }
        }


    private IndexStoreMetadata getIndexStoreMetadataForMergedFile() throws IOException {
        File baseFFSMetadataFile = IndexStoreUtils.getMetadataFile(baseDir, algorithm);
        File incrementalMetadataFile = IndexStoreUtils.getMetadataFile(incrementalFile, algorithm);

        if (baseFFSMetadataFile.exists() && incrementalMetadataFile.exists()) {
            IndexStoreMetadata indexStoreMetadata = new IndexStoreMetadataOperatorImpl<IndexStoreMetadata>()
                    .getIndexStoreMetadata(baseFFSMetadataFile, algorithm, new TypeReference<>() {
                    });
            IncrementalIndexStoreMetadata incrementalIndexStoreMetadata = new IndexStoreMetadataOperatorImpl<IncrementalIndexStoreMetadata>()
                    .getIndexStoreMetadata(incrementalMetadataFile, algorithm, new TypeReference<>() {
                    });
            return mergeIndexStores(indexStoreMetadata, incrementalIndexStoreMetadata);
        } else {
            throw new RuntimeException("either one or both metadataFiles don't exist at path: " +
                    baseFFSMetadataFile.getAbsolutePath() + ", " + incrementalMetadataFile.getAbsolutePath());
        }
    }

    private void mergeMetadataFiles() throws IOException {
        try (BufferedWriter writer = IndexStoreUtils.createWriter(IndexStoreUtils.getMetadataFile(mergedDir, algorithm), algorithm)) {
            JSON_MAPPER.writeValue(writer, getIndexStoreMetadataForMergedFile());
        }
    }

    /**
     * We only merge indexStore and incrementalStore if:
     * 1. IndexStore's checkpoint equals incrementalStore's before checkpoint.
     * 2. IndexStore's preferredPaths equals incrementalStore's preferredPaths.
     */
    private IndexStoreMetadata mergeIndexStores(IndexStoreMetadata indexStoreMetadata,
                                                IncrementalIndexStoreMetadata incrementalIndexStoreMetadata) {
        checkState(indexStoreMetadata.getCheckpoint().equals(incrementalIndexStoreMetadata.getBeforeCheckpoint()));
        return new IndexStoreMetadata(incrementalIndexStoreMetadata.getAfterCheckpoint(), indexStoreMetadata.getStoreType(),
                getStrategyName(), Collections.emptySet());
    }

}