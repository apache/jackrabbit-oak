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

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;

import com.google.common.base.Stopwatch;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.StandardSystemProperty.LINE_SEPARATOR;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_MAX_SORT_MEMORY_IN_GB;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileNodeStoreBuilder.OAK_INDEXER_MAX_SORT_MEMORY_IN_GB_DEFAULT;
import static org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils.getSortedStoreFileName;

class StoreAndSortStrategy implements SortStrategy {
    private static final String OAK_INDEXER_DELETE_ORIGINAL = "oak.indexer.deleteOriginal";
    private static final int LINE_SEP_LENGTH = LINE_SEPARATOR.value().length();

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Iterable<NodeStateEntry> nodeStates;
    private final PathElementComparator comparator;
    private final NodeStateEntryWriter entryWriter;
    private final File storeDir;
    private final boolean compressionEnabled;
    private long entryCount;
    private boolean deleteOriginal = Boolean.parseBoolean(System.getProperty(OAK_INDEXER_DELETE_ORIGINAL, "true"));
    private int maxMemory = Integer.getInteger(OAK_INDEXER_MAX_SORT_MEMORY_IN_GB, OAK_INDEXER_MAX_SORT_MEMORY_IN_GB_DEFAULT);
    private long textSize;


    public StoreAndSortStrategy(Iterable<NodeStateEntry> nodeStates, PathElementComparator comparator,
                                NodeStateEntryWriter entryWriter, File storeDir, boolean compressionEnabled) {
        this.nodeStates = nodeStates;
        this.comparator = comparator;
        this.entryWriter = entryWriter;
        this.storeDir = storeDir;
        this.compressionEnabled = compressionEnabled;
    }

    @Override
    public File createSortedStoreFile() throws IOException {
        File storeFile = writeToStore(storeDir, getStoreFileName());
        return sortStoreFile(storeFile);
    }

    @Override
    public long getEntryCount() {
        return entryCount;
    }

    private File sortStoreFile(File storeFile) throws IOException {
        File sortWorkDir = new File(storeFile.getParent(), "sort-work-dir");
        FileUtils.forceMkdir(sortWorkDir);
        File sortedFile = new File(storeFile.getParentFile(), getSortedStoreFileName(compressionEnabled));
        NodeStateEntrySorter sorter =
                new NodeStateEntrySorter(comparator, storeFile, sortWorkDir, sortedFile);

        logFlags();

        sorter.setUseZip(compressionEnabled);
        sorter.setMaxMemoryInGB(maxMemory);
        sorter.setDeleteOriginal(deleteOriginal);
        sorter.setActualFileSize(textSize);
        sorter.sort();
        return sorter.getSortedFile();
    }

    private File writeToStore(File dir, String fileName) throws IOException {
        entryCount = 0;
        File file = new File(dir, fileName);
        Stopwatch sw = Stopwatch.createStarted();
        try (BufferedWriter w = FlatFileStoreUtils.createWriter(file, compressionEnabled)) {
            for (NodeStateEntry e : nodeStates) {
                String line = entryWriter.toString(e);
                w.append(line);
                w.newLine();
                textSize += line.length() + LINE_SEP_LENGTH;
                entryCount++;
            }
        }
        String sizeStr = compressionEnabled ? String.format("compressed/%s actual size", humanReadableByteCount(textSize)) : "";
        log.info("Dumped {} nodestates in json format in {} ({} {})",entryCount, sw, humanReadableByteCount(file.length()), sizeStr);
        return file;
    }

    private void logFlags() {
        log.info("Delete original dump from traversal : {} ({})", deleteOriginal, OAK_INDEXER_DELETE_ORIGINAL);
        log.info("Max heap memory (GB) to be used for merge sort : {} ({})", maxMemory, OAK_INDEXER_MAX_SORT_MEMORY_IN_GB);
    }

    private String getStoreFileName() {
        return compressionEnabled ? "store.json.gz" : "store.json";
    }
}
