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
import java.util.Collections;

import com.google.common.collect.Iterables;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.collect.Iterables.size;

public class FlatFileNodeStoreBuilder {
    public static final String OAK_INDEXER_USE_ZIP = "oak.indexer.useZip";
    private static final String OAK_INDEXER_TRAVERSE_WITH_SORT = "oak.indexer.traverseWithSortStrategy";
    private static final String OAK_INDEXER_SORTED_FILE_PATH = "oak.indexer.sortedFilePath";
    static final String OAK_INDEXER_MAX_SORT_MEMORY_IN_GB = "oak.indexer.maxSortMemoryInGB";
    static final int OAK_INDEXER_MAX_SORT_MEMORY_IN_GB_DEFAULT = 2;
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final Iterable<NodeStateEntry> nodeStates;
    private final File workDir;
    private Iterable<String> preferredPathElements = Collections.emptySet();
    private BlobStore blobStore;
    private PathElementComparator comparator;
    private NodeStateEntryWriter entryWriter;
    private long entryCount = 0;

    private boolean useZip = Boolean.valueOf(System.getProperty(OAK_INDEXER_USE_ZIP, "true"));
    private boolean useTraverseWithSort = Boolean.valueOf(System.getProperty(OAK_INDEXER_TRAVERSE_WITH_SORT, "true"));

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
        logFlags();
        comparator = new PathElementComparator(preferredPathElements);
        entryWriter = new NodeStateEntryWriter(blobStore);
        FlatFileStore store = new FlatFileStore(createdSortedStoreFile(), new NodeStateEntryReader(blobStore),
                size(preferredPathElements), useZip);
        if (entryCount > 0) {
            store.setEntryCount(entryCount);
        }
        return store;
    }

    private File createdSortedStoreFile() throws IOException {
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
            File flatFileStoreDir = createStoreDir();
            SortStrategy strategy = createSortStrategy(flatFileStoreDir);
            File result = strategy.createSortedStoreFile();
            entryCount = strategy.getEntryCount();
            return result;
        }
    }

    private SortStrategy createSortStrategy(File dir){
        if (useTraverseWithSort) {
            log.info("Using TraverseWithSortStrategy");
            return new TraverseWithSortStrategy(nodeStates, comparator, entryWriter, dir, useZip);
        } else {
            log.info("Using StoreAndSortStrategy");
            return new StoreAndSortStrategy(nodeStates, comparator, entryWriter, dir, useZip);
        }
    }

    private void logFlags() {
        log.info("Preferred path elements are {}", Iterables.toString(preferredPathElements));
        log.info("Compression enabled while sorting : {} ({})", useZip, OAK_INDEXER_USE_ZIP);

        String strategy = useTraverseWithSort ?
                TraverseWithSortStrategy.class.getSimpleName() : StoreAndSortStrategy.class.getSimpleName();
        log.info("Sort strategy : {} ({})", strategy, OAK_INDEXER_TRAVERSE_WITH_SORT);
    }

    private File createStoreDir() throws IOException {
        File dir = new File(workDir, "flat-file-store");
        FileUtils.forceMkdir(dir);
        return dir;
    }
}
