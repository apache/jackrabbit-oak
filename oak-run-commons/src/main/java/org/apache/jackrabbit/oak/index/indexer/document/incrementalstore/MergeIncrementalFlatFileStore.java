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
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateHolder;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.PathElementComparator;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.SimpleNodeStateHolder;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreMetadata;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreMetadataOperatorImpl;
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

import static org.apache.jackrabbit.guava.common.base.Preconditions.checkState;


public class MergeIncrementalFlatFileStore {

    private final Logger log = LoggerFactory.getLogger(getClass());
    public final static String MERGE_BASE_AND_INCREMENTAL_FLAT_FILE_STORE = "MergeBaseAndIncrementalFlatFileStore";
    private final File baseFFS;
    private final File incrementalFFS;
    private final File merged;
    private final Compression algorithm;
    private final Comparator<NodeStateHolder> comparator;

    public MergeIncrementalFlatFileStore(Set<String> preferredPathElements, File baseFFS, File incrementalFFS,
                                         File merged, Compression algorithm) throws IOException {
        this.baseFFS = baseFFS;
        this.incrementalFFS = incrementalFFS;
        this.merged = merged;
        this.algorithm = algorithm;
        comparator = (e1, e2) -> new PathElementComparator(preferredPathElements).compare(e1.getPathElements(), e2.getPathElements());

        if (merged.exists()) {
            log.warn("merged file:{} exists, this file will be replaced with new mergedFFS file", merged.getAbsolutePath());
        }
        Files.createDirectories(merged.getParentFile().toPath());
        basicFileValidation(algorithm, baseFFS, incrementalFFS, merged);
    }

    public void doMerge() throws IOException {
        log.info("base FFS " + baseFFS.getAbsolutePath());
        log.info("incremental FFS " + incrementalFFS.getAbsolutePath());
        log.info("merged FFS " + merged.getAbsolutePath());
        mergeMetadataFiles();
        mergeIndexStoreFiles();
    }

    private void basicFileValidation(Compression algorithm, File... files) {
        for (File file : files) {
            checkState(file.isFile(), "File doesn't exist {}", file.getAbsolutePath());
            FlatFileStoreUtils.validateFlatFileStoreFileName(file, algorithm);
        }
    }

    private void mergeIndexStoreFiles() throws IOException {
        Map<String, IncrementalStoreOperand> enumMap = Arrays.stream(IncrementalStoreOperand.values())
                .collect(Collectors.toUnmodifiableMap(IncrementalStoreOperand::toString, k -> IncrementalStoreOperand.valueOf(k.name())));

        try (BufferedWriter writer = FlatFileStoreUtils.createWriter(merged, algorithm);
             BufferedReader baseFFSBufferedReader = FlatFileStoreUtils.createReader(baseFFS, algorithm);
             BufferedReader incrementalFFSBufferedReader = FlatFileStoreUtils.createReader(incrementalFFS, algorithm)) {
            String baseFFSLine = baseFFSBufferedReader.readLine();
            String incrementalFFSLine = incrementalFFSBufferedReader.readLine();


            int compared;
            while (baseFFSLine != null || incrementalFFSLine != null) {
                if (baseFFSLine != null && incrementalFFSLine != null) {
                    compared = comparator.compare(new SimpleNodeStateHolder(baseFFSLine), new SimpleNodeStateHolder(incrementalFFSLine));
                    if (compared < 0) { // write baseFFSLine in merged file and advance line in baseFFS
                        baseFFSLine = writeAndAdvance(writer, baseFFSBufferedReader, baseFFSLine);
                    } else if (compared > 0) { // write incrementalFFSline and advance line in incrementalFFS
                        String[] incrementalFFSParts = IncrementalFlatFileStoreNodeStateEntryWriter.getParts(incrementalFFSLine);
                        checkState(IncrementalStoreOperand.ADD.toString().equals(getOperand(incrementalFFSParts)));
                        incrementalFFSLine = writeAndAdvance(writer, incrementalFFSBufferedReader,
                                getFFSLineFromIncrementalFFSParts(incrementalFFSParts));
                    } else {
                        String[] incrementalFFSParts = IncrementalFlatFileStoreNodeStateEntryWriter.getParts(incrementalFFSLine);
                        String operand = getOperand(incrementalFFSParts);
                        switch (enumMap.get(operand)) {
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

    private IndexStoreMetadata getIndexStoreMetadataForMergedFile() throws IOException {
        File baseFFSMetadataFile = FlatFileStoreUtils.getMetadataFile(baseFFS, algorithm);
        File incrementalMetadataFile = FlatFileStoreUtils.getMetadataFile(incrementalFFS, algorithm);

        if (baseFFSMetadataFile.exists() && incrementalMetadataFile.exists()) {
            IndexStoreMetadata indexStoreMetadata = new IndexStoreMetadataOperatorImpl<IndexStoreMetadata>()
                    .getIndexStoreMetadata(baseFFSMetadataFile, algorithm, new TypeReference<IndexStoreMetadata>() {
                    });
            IncrementalIndexStoreMetadata incrementalIndexStoreMetadata = new IndexStoreMetadataOperatorImpl<IncrementalIndexStoreMetadata>()
                    .getIndexStoreMetadata(incrementalMetadataFile, algorithm, new TypeReference<IncrementalIndexStoreMetadata>() {
                    });
            return mergeIndexStores(indexStoreMetadata, incrementalIndexStoreMetadata);
        } else {
            throw new RuntimeException("either one or both metadataFiles donot exist at path: " +
                    baseFFSMetadataFile.getAbsolutePath() + ", " + incrementalMetadataFile.getAbsolutePath());
        }
    }

    private void mergeMetadataFiles() throws IOException {
        try (BufferedWriter writer = FlatFileStoreUtils.createWriter(FlatFileStoreUtils.getMetadataFile(merged, algorithm), algorithm)) {
            ObjectMapper mapper = new ObjectMapper();
            mapper.writeValue(writer, getIndexStoreMetadataForMergedFile());
        }
    }

    private IndexStoreMetadata mergeIndexStores(IndexStoreMetadata indexStoreMetadata,
                                                IncrementalIndexStoreMetadata incrementalIndexStoreMetadata) {
        checkState(indexStoreMetadata.getCheckpoint().equals(incrementalIndexStoreMetadata.getBeforeCheckpoint()));
        checkState(indexStoreMetadata.getPreferredPaths().equals(incrementalIndexStoreMetadata.getPreferredPaths()));
        return new IndexStoreMetadata(incrementalIndexStoreMetadata.getAfterCheckpoint(), indexStoreMetadata.getStoreType(),
                MERGE_BASE_AND_INCREMENTAL_FLAT_FILE_STORE, indexStoreMetadata.getPreferredPaths());
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
        do {
            String[] incrementalFFSParts = IncrementalFlatFileStoreNodeStateEntryWriter.getParts(incrementalFFSLine);
            String operand = getOperand(incrementalFFSParts);
            checkState(!IncrementalStoreOperand.MODIFY.toString().equals(operand)
                            && !IncrementalStoreOperand.DELETE.toString().equals(operand),
                    "incremental ffs should not have modify or delete operands: {}", incrementalFFSLine);
            incrementalFFSLine = writeAndAdvance(writer, bufferedReader, getFFSLineFromIncrementalFFSParts(incrementalFFSParts));
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