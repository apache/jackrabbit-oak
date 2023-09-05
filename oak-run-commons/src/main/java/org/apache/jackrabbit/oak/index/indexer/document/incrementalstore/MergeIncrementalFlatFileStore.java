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

import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryWriter;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateHolder;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.PathElementComparator;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.SimpleNodeStateHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.Set;

import static org.apache.jackrabbit.guava.common.base.Preconditions.checkState;


public class MergeIncrementalFlatFileStore {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final File baseFFS;
    private final File incrementalFFS;
    private final File merged;
    private final Set<String> preferredPathElements;
    private final Compression algorithm;

    public MergeIncrementalFlatFileStore(Set<String> preferredPathElements, File baseFFS, File incrementalFFS,
                                         File merged, Compression algorithm) {
        this.preferredPathElements = preferredPathElements;
        this.baseFFS = baseFFS;
        this.incrementalFFS = incrementalFFS;
        this.merged = merged;
        this.algorithm = algorithm;
    }

    public void doMerge() throws IOException {

        log.info("base FFS " + baseFFS.getAbsolutePath());
        log.info("incremental FFS " + incrementalFFS.getAbsolutePath());
        log.info("merged FFS " + merged.getAbsolutePath());

        try (BufferedWriter writer = FlatFileStoreUtils.createWriter(merged, algorithm);
             BufferedReader baseFFSBufferedReader = FlatFileStoreUtils.createReader(baseFFS, algorithm);
             BufferedReader incrementalFFSBufferedReader = FlatFileStoreUtils.createReader(incrementalFFS, algorithm)) {
            String baseLine = baseFFSBufferedReader.readLine();
            String incLine = incrementalFFSBufferedReader.readLine();

            Comparator<NodeStateHolder> comparator = (e1, e2) -> new PathElementComparator(preferredPathElements).compare(e1.getPathElements(), e2.getPathElements());
            int compared;
            while (baseLine != null || incLine != null) {
                if (baseLine != null && incLine != null) {
                    compared = comparator.compare(new SimpleNodeStateHolder(baseLine), new SimpleNodeStateHolder(incLine));
                    if (compared < 0) {
                        writer.write(baseLine);
                        writer.write("\n");
                        baseLine = baseFFSBufferedReader.readLine();
                    } else if (compared > 0) {
                        writer.write(removeOperand(incLine));
                        writer.write("\n");
                        incLine = incrementalFFSBufferedReader.readLine();
                    } else {
                        String operand = NodeStateEntryWriter.getParts(incLine)[3];
                        checkState(IncrementalStoreOperand.DELETE.toString().equals(operand)
                                        || IncrementalStoreOperand.ADD.toString().equals(operand)
                                        || IncrementalStoreOperand.MODIFY.toString().equals(operand),
                                "wrong operand in incremental ffs: {} ", operand);
                        if (!(IncrementalStoreOperand.DELETE.toString().equals(operand))) {
                            writer.write(removeOperand(incLine));
                            writer.write("\n");
                        }
                        baseLine = baseFFSBufferedReader.readLine();
                        incLine = incrementalFFSBufferedReader.readLine();
                    }
                } else {
                    if (incLine == null) {
                        writer.write(baseLine);
                        writer.write("\n");
                        writeRestOfFile(writer, baseFFSBufferedReader, false);
                        baseLine = baseFFSBufferedReader.readLine();
                    } else {
                        String operand = getOperand(incLine);
                        if (IncrementalStoreOperand.MODIFY.toString().equals(operand) || IncrementalStoreOperand.DELETE.toString().equals(operand)) {
                            log.error("incremental ffs should not have modify and delete operands: {}", incLine);
                            throw new RuntimeException("incremental ffs should not have modify and delete operands" + incLine);
                        }
                        writer.write(removeOperand(incLine));
                        writer.write("\n");
                        writeRestOfFile(writer, incrementalFFSBufferedReader, true);
                        incLine = incrementalFFSBufferedReader.readLine();
                    }
                }
            }
        }
    }

    private void writeRestOfFile(BufferedWriter writer, BufferedReader bufferedReader, boolean isIncrementalFile) {
        if (isIncrementalFile) {
            bufferedReader.lines().forEach((n) -> {
                String operand = getOperand(n);
                if (IncrementalStoreOperand.MODIFY.toString().equals(operand) || IncrementalStoreOperand.DELETE.toString().equals(operand)) {
                    log.error("incremental ffs should not have modify and delete operands: {}", n);
                    throw new RuntimeException("incremental ffs should not have modify and delete operands" + n);
                }
                try {
                    writer.write(removeOperand(n));
                    writer.write("\n");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } else {
            bufferedReader.lines().forEach(n -> {
                try {
                    writer.write(n);
                    writer.write("\n");
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private static String getOperand(String incLine) {
        String[] parts = NodeStateEntryWriter.getParts(incLine);
        String operand = parts[2];
        return operand;
    }

    private String removeOperand(String line) {
        String[] parts = NodeStateEntryWriter.getParts(line);
        return new StringBuilder().append(parts[0]).append("|").append(parts[1]).toString();
    }
}