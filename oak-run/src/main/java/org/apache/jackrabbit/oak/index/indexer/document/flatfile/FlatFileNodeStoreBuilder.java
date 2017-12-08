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
import java.io.Writer;
import java.util.Collections;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlatFileNodeStoreBuilder {
    private static final String OAK_INDEXER_USE_ZIP = "oak.indexer.useZip";
    private static final String OAK_INDEXER_DELETE_ORIGINAL = "oak.indexer.deleteOriginal";
    private static final String OAK_INDEXER_MAX_SORT_MEMORY_IN_GB = "oak.indexer.maxSortMemoryInGB";
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Iterable<NodeStateEntry> nodeStates;
    private final File workDir;
    private Iterable<String> preferredPathElements = Collections.emptySet();
    private BlobStore blobStore;

    private boolean useZip = Boolean.getBoolean(OAK_INDEXER_USE_ZIP);
    private boolean deleteOriginal = Boolean.parseBoolean(System.getProperty(OAK_INDEXER_DELETE_ORIGINAL, "true"));
    private int maxMemory = Integer.getInteger(OAK_INDEXER_MAX_SORT_MEMORY_IN_GB, 5);

    public FlatFileNodeStoreBuilder(Iterable<NodeStateEntry> nodeStates, File workDir) {
        this.nodeStates = nodeStates;
        this.workDir = workDir;
    }

    public FlatFileNodeStoreBuilder withBlobStore(BlobStore blobStore) {
        this.blobStore = blobStore;
        return this;
    }

    public FlatFileNodeStoreBuilder withPreferredPathElements(Iterable<String> preferredPathElements) {
        this.preferredPathElements = preferredPathElements;
        return this;
    }

    public FlatFileStore build() throws IOException {
        //TODO Check not null blobStore
        File flatFileStoreDir = createStoreDir();
        File storeFile = writeToStore(flatFileStoreDir, "store.json");
        File sortedFile = sortStoreFile(storeFile);
        return new FlatFileStore(sortedFile, new NodeStateEntryReader(blobStore));
    }

    private File sortStoreFile(File storeFile) throws IOException {
        File sortWorkDir = new File(storeFile.getParent(), "sort-work-dir");
        FileUtils.forceMkdir(sortWorkDir);
        NodeStateEntrySorter sorter =
                new NodeStateEntrySorter(new PathElementComparator(preferredPathElements), storeFile, sortWorkDir);

        logFlags();

        sorter.setUseZip(useZip);
        sorter.setMaxMemoryInGB(maxMemory);
        sorter.setDeleteOriginal(deleteOriginal);
        sorter.sort();
        return sorter.getSortedFile();
    }

    private void logFlags() {
        log.info("Compression enabled while sorting : {} ({})", useZip, OAK_INDEXER_USE_ZIP);
        log.info("Delete original dump from traversal : {} ({})", deleteOriginal, OAK_INDEXER_DELETE_ORIGINAL);
        log.info("Max heap memory (GB) to be used for merge sort : {} ({})", maxMemory, OAK_INDEXER_MAX_SORT_MEMORY_IN_GB);
    }

    private File writeToStore(File dir, String fileName) throws IOException {
        File file = new File(dir, fileName);
        try (
                Writer w = Files.newWriter(file, Charsets.UTF_8);
                NodeStateEntryWriter entryWriter = new NodeStateEntryWriter(blobStore, w)
        ) {
            for (NodeStateEntry e : nodeStates) {
                entryWriter.write(e);
            }
        }
        return file;
    }

    private File createStoreDir() throws IOException {
        File dir = new File(workDir, "flat-file-store");
        FileUtils.forceMkdir(dir);
        return dir;
    }
}
