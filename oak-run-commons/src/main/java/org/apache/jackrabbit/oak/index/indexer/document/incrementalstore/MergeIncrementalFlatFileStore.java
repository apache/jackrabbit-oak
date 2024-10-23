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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateHolder;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.PathElementComparator;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.SimpleNodeStateHolder;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreMetadata;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreMetadataOperatorImpl;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class MergeIncrementalFlatFileStore implements MergeIncrementalStore {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final static String MERGE_BASE_AND_INCREMENTAL_FLAT_FILE_STORE = "MergeBaseAndIncrementalFlatFileStore";
    private final File baseFFS;
    private final File incrementalFFS;
    private final File merged;
    private final Compression algorithm;
    private final Comparator<NodeStateHolder> comparator;
    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    public MergeIncrementalFlatFileStore(Set<String> preferredPathElements, File baseFFS, File incrementalFFS,
                                         File merged, Compression algorithm) throws IOException {
        this.baseFFS = baseFFS;
        this.incrementalFFS = incrementalFFS;
        this.merged = merged;
        this.algorithm = algorithm;
        comparator = (e1, e2) -> new PathElementComparator(preferredPathElements).compare(e1.getPathElements(), e2.getPathElements());

        if (merged.exists()) {
            log.warn("merged file:{} exists, this file will be replaced with new mergedFFS file", merged.getAbsolutePath());
        } else {
            Files.createDirectories(merged.getParentFile().toPath());
        }
        IndexStoreUtils.validateFlatFileStoreFileName(merged, algorithm);
        basicFileValidation(algorithm, baseFFS, incrementalFFS);
    }

    @Override
    public void doMerge() throws IOException {
        log.info("base FFS " + baseFFS.getAbsolutePath());
        log.info("incremental FFS " + incrementalFFS.getAbsolutePath());
        log.info("merged FFS " + merged.getAbsolutePath());
        mergeMetadataFiles();
        mergeIndexStoreFiles();
    }

    @Override
    public String getStrategyName() {
        return MERGE_BASE_AND_INCREMENTAL_FLAT_FILE_STORE;
    }

    private void basicFileValidation(Compression algorithm, File... files) {
        for (File file : files) {
            Validate.checkState(file.isFile(), "File doesn't exist {}", file.getAbsolutePath());
            IndexStoreUtils.validateFlatFileStoreFileName(file, algorithm);
            Validate.checkState(IndexStoreUtils.getMetadataFile(file, algorithm).exists(),
                    "metadata file is not present in same directory as indexStore. indexStoreFile:{}, metadataFile should be available at:{}",
                    file.getAbsolutePath(), IndexStoreUtils.getMetadataFile(file, algorithm));
        }
    }

    /**
     * Merges multiple index store files.
     *
     * This method is a little verbose, but I think this is fine
     * as we are not getting consistent data from checkpoint diff
     * and we need to handle cases differently.
     */
    private void mergeIndexStoreFiles() throws IOException {
        Map<String, IncrementalStoreOperand> enumMap = Arrays.stream(IncrementalStoreOperand.values())
                .collect(Collectors.toUnmodifiableMap(IncrementalStoreOperand::toString, k -> IncrementalStoreOperand.valueOf(k.name())));

        try (BufferedWriter writer = IndexStoreUtils.createWriter(merged, algorithm);
             BufferedReader baseFFSBufferedReader = IndexStoreUtils.createReader(baseFFS, algorithm);
             BufferedReader incrementalFFSBufferedReader = IndexStoreUtils.createReader(incrementalFFS, algorithm)) {
            String baseFFSLine = baseFFSBufferedReader.readLine();
            String incrementalFFSLine = incrementalFFSBufferedReader.readLine();

            int compared;
            while (baseFFSLine != null || incrementalFFSLine != null) {
                if (baseFFSLine != null && incrementalFFSLine != null) {
                    compared = comparator.compare(new SimpleNodeStateHolder(baseFFSLine), new SimpleNodeStateHolder(incrementalFFSLine));
                    if (compared < 0) { // write baseFFSLine in merged file and advance line in baseFFS
                        baseFFSLine = writeAndAdvance(writer, baseFFSBufferedReader, baseFFSLine);
                    }
                    // We are adding warn logs instead of checkState e.g.
                    // 1- delete a node
                    // 2- add node at same path.
                    // The incremental FFS with above operations are dumped as node added instead of modified.
                    else if (compared > 0) { // write incrementalFFSline and advance line in incrementalFFS
                        incrementalFFSLine = processIncrementalFFSLine(enumMap, writer, incrementalFFSBufferedReader, incrementalFFSLine);
                    } else {
                        String[] incrementalFFSParts = IncrementalFlatFileStoreNodeStateEntryWriter.getParts(incrementalFFSLine);
                        String operand = getOperand(incrementalFFSParts);
                        switch (enumMap.get(operand)) {
                            case ADD:
                                log.warn("Expected operand {} or {} but got {} for incremental line {}. " +
                                                "Merging will proceed, but this is unexpected.",
                                        IncrementalStoreOperand.MODIFY, IncrementalStoreOperand.DELETE, getOperand(incrementalFFSParts), incrementalFFSLine);
                                incrementalFFSLine = writeAndAdvance(writer, incrementalFFSBufferedReader,
                                        getFFSLineFromIncrementalFFSParts(incrementalFFSParts));
                                break;
                            case MODIFY:
                                incrementalFFSLine = writeAndAdvance(writer, incrementalFFSBufferedReader,
                                        getFFSLineFromIncrementalFFSParts(incrementalFFSParts));
                                break;
                            case DELETE:
                                incrementalFFSLine = incrementalFFSBufferedReader.readLine();
                                break;
                            default:
                                log.error("wrong operand in incremental ffs: operand:{}, line:{}", operand, incrementalFFSLine);
                                throw new RuntimeException("wrong operand in incremental ffs: operand:" + operand + ", line:" + incrementalFFSLine);
                        }
                        baseFFSLine = baseFFSBufferedReader.readLine();
                    }
                } else {
                    if (incrementalFFSLine == null) {
                        baseFFSLine = writeRestOfFFSFileAndAdvance(writer, baseFFSBufferedReader, baseFFSLine);
                    } else {
                        incrementalFFSLine = writeRestOfIncrementalFileAndAdvance(writer, incrementalFFSBufferedReader, incrementalFFSLine);
                    }
                }
            }
        }
    }

    private String processIncrementalFFSLine(Map<String, IncrementalStoreOperand> enumMap, BufferedWriter writer,
                                             BufferedReader incrementalFFSBufferedReader, String incrementalFFSLine) throws IOException {
        String[] incrementalFFSParts = IncrementalFlatFileStoreNodeStateEntryWriter.getParts(incrementalFFSLine);
        String operand = getOperand(incrementalFFSParts);
        switch (enumMap.get(operand)) {
            case ADD:
                incrementalFFSLine = writeAndAdvance(writer, incrementalFFSBufferedReader,
                        getFFSLineFromIncrementalFFSParts(incrementalFFSParts));
                break;
            case MODIFY:
                // this case should not happen. But in case this happens we consider modify as addition of node
                // this implies node is not present in older FFS and in checkpointdiff this came as modified instead of
                // node addition.
                log.warn("Expected operand {} but got {} for incremental line {}. " +
                                "Merging will proceed, but this is unexpected.",
                        IncrementalStoreOperand.ADD, operand, incrementalFFSLine);
                incrementalFFSLine = writeAndAdvance(writer, incrementalFFSBufferedReader,
                        getFFSLineFromIncrementalFFSParts(incrementalFFSParts));
                break;
            case DELETE:
                // This case should not happen. If this happens, it means we don't have any such node in baseFFS
                // but this node came as deletion of node for an already non-existing node.
                // we just skip this node in this case.
                log.warn("Expected operand {} but got {} for incremental line {}. Merging will proceed as usual, but this needs to be looked into.",
                        IncrementalStoreOperand.ADD, operand, incrementalFFSLine);
                incrementalFFSLine = incrementalFFSBufferedReader.readLine();
                break;
            default:
                log.error("Unsupported operand in incremental ffs: operand:{}, line:{}", operand, incrementalFFSLine);
                throw new RuntimeException("wrong operand in incremental ffs: operand:" + operand + ", line:" + incrementalFFSLine);
        }
        return incrementalFFSLine;
    }

    private IndexStoreMetadata getIndexStoreMetadataForMergedFile() throws IOException {
        File baseFFSMetadataFile = IndexStoreUtils.getMetadataFile(baseFFS, algorithm);
        File incrementalMetadataFile = IndexStoreUtils.getMetadataFile(incrementalFFS, algorithm);

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
        try (BufferedWriter writer = IndexStoreUtils.createWriter(IndexStoreUtils.getMetadataFile(merged, algorithm), algorithm)) {
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
        Validate.checkState(indexStoreMetadata.getCheckpoint().equals(incrementalIndexStoreMetadata.getBeforeCheckpoint()));
        Validate.checkState(indexStoreMetadata.getPreferredPaths().equals(incrementalIndexStoreMetadata.getPreferredPaths()));
        return new IndexStoreMetadata(incrementalIndexStoreMetadata.getAfterCheckpoint(), indexStoreMetadata.getStoreType(),
                getStrategyName(), indexStoreMetadata.getPreferredPaths());
    }

    private String writeAndAdvance(BufferedWriter writer, BufferedReader bufferedReader, String stringToWriteInMergeFile) throws IOException {
        writer.write(stringToWriteInMergeFile);
        writer.write("\n");
        stringToWriteInMergeFile = bufferedReader.readLine();
        return stringToWriteInMergeFile;
    }

    private String writeRestOfFFSFileAndAdvance(BufferedWriter writer, BufferedReader bufferedReader,
                                                String baseFFSLine) throws IOException {
        do {
            baseFFSLine = writeAndAdvance(writer, bufferedReader, baseFFSLine);
        } while (baseFFSLine != null);
        return bufferedReader.readLine();
    }

    private String writeRestOfIncrementalFileAndAdvance(BufferedWriter writer, BufferedReader bufferedReader, String incrementalFFSLine) throws IOException {
        Map<String, IncrementalStoreOperand> enumMap = Arrays.stream(IncrementalStoreOperand.values())
                .collect(Collectors.toUnmodifiableMap(IncrementalStoreOperand::toString, k -> IncrementalStoreOperand.valueOf(k.name())));
        do {
            incrementalFFSLine = processIncrementalFFSLine(enumMap, writer, bufferedReader, incrementalFFSLine);
        } while (incrementalFFSLine != null);
        return bufferedReader.readLine();
    }

    private String getOperand(String[] incrementalFFSParts) {
        return incrementalFFSParts[3];
    }

    private String getFFSLineFromIncrementalFFSParts(String[] incrementalFFSParts) {
        return incrementalFFSParts[0] + "|" + incrementalFFSParts[1];
    }
}