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

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.index.IndexHelper;
import org.apache.jackrabbit.oak.index.indexer.document.CompositeIndexer;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateIndexer;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.ConfigHelper;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStore;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AheadOfTimeBlobDownloadingFlatFileStore implements IndexStore {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final FlatFileStore ffs;
    private final CompositeIndexer indexer;
    private final IndexHelper indexHelper;

    public static final String BLOB_PREFETCH_ENABLE_FOR_INDEXES_PREFIXES = "oak.indexer.blobPrefetch.enableForIndexesPrefixes";
    public static final String BLOB_PREFETCH_BINARY_NODES_SUFFIX = "oak.indexer.blobPrefetch.binaryNodesSuffix";
    public static final String BLOB_PREFETCH_DOWNLOAD_THREADS = "oak.indexer.blobPrefetch.downloadThreads";
    public static final String BLOB_PREFETCH_DOWNLOAD_AHEAD_WINDOW_MB = "oak.indexer.blobPrefetch.downloadAheadWindowMB";
    public static final String BLOB_PREFETCH_DOWNLOAD_AHEAD_WINDOW_SIZE = "oak.indexer.blobPrefetch.downloadAheadWindowSize";
    private final String blobPrefetchEnableForIndexes = ConfigHelper.getSystemPropertyAsString(BLOB_PREFETCH_ENABLE_FOR_INDEXES_PREFIXES, "");
    private final String blobPrefetchBinaryNodeSuffix = ConfigHelper.getSystemPropertyAsString(BLOB_PREFETCH_BINARY_NODES_SUFFIX, "");
    private final int nDownloadThreads = ConfigHelper.getSystemPropertyAsInt(BLOB_PREFETCH_DOWNLOAD_THREADS, 4);
    private final int maxPrefetchWindowMB = ConfigHelper.getSystemPropertyAsInt(BLOB_PREFETCH_DOWNLOAD_AHEAD_WINDOW_MB, 32);
    private final int maxPrefetchWindowSize = ConfigHelper.getSystemPropertyAsInt(BLOB_PREFETCH_DOWNLOAD_AHEAD_WINDOW_SIZE, 4096);

    public static AheadOfTimeBlobDownloadingFlatFileStore wrap(FlatFileStore ffs, CompositeIndexer indexer, IndexHelper indexHelper) {
        return new AheadOfTimeBlobDownloadingFlatFileStore(ffs, indexer, indexHelper);
    }

    private AheadOfTimeBlobDownloadingFlatFileStore(FlatFileStore ffs, CompositeIndexer indexer, IndexHelper indexHelper) {
        this.ffs = ffs;
        this.indexer = indexer;
        this.indexHelper = indexHelper;
    }

    private @NotNull AheadOfTimeBlobDownloader createAheadOfTimeBlobDownloader(CompositeIndexer indexer, IndexHelper indexHelper) {
        if (blobPrefetchBinaryNodeSuffix == null || blobPrefetchBinaryNodeSuffix.isBlank()) {
            log.info("Ahead of time blob downloader is disabled, no binary node suffix provided");
            return AheadOfTimeBlobDownloader.NOOP;
        } else {
            List<String> enableIndexesPrefix = splitAndTrim(blobPrefetchEnableForIndexes);
            List<NodeStateIndexer> enabledIndexers = filterEnabledIndexes(enableIndexesPrefix, indexer.getIndexers());
            if (enabledIndexers.isEmpty()) {
                log.info("Ahead of time blob downloader is disabled, not enabled for any indexes: {}", indexHelper.getIndexPaths());
                return AheadOfTimeBlobDownloader.NOOP;
            } else {
                return new DefaultAheadOfTimeBlobDownloader(
                        blobPrefetchBinaryNodeSuffix,
                        ffs.getStoreFile(),
                        ffs.getAlgorithm(),
                        indexHelper.getGCBlobStore(),
                        enabledIndexers,
                        nDownloadThreads,
                        maxPrefetchWindowSize,
                        maxPrefetchWindowMB);
            }
        }
    }

    /**
     * Returns the indexes for which AOT blob downloading is enabled, that is,
     * for which the index name starts with any of the prefixes in the enabledForIndexes list.
     *
     * @param enabledIndexesPrefixes list of prefixes of the index definitions that benefit from the download
     * @param indexers          the index paths
     * @return the indexers for which AOT blob download is enabled, or empty list if it is not enabled for any
     */
    public static <T extends NodeStateIndexer> List<T> filterEnabledIndexes(List<String> enabledIndexesPrefixes, List<T> indexers) {
        return indexers.stream()
                .filter(indexer -> enabledIndexesPrefixes.stream().anyMatch(prefix -> indexer.getIndexName().startsWith(prefix)))
                .collect(Collectors.toList());
    }

    /**
     * Whether blob downloading is needed for any the given indexes.
     *
     * @param enableIndexesPrefix list of prefixes of the index definitions that benefit from the download
     * @param indexPaths       the index paths
     * @return true if any of the indexes start with any of the prefixes
     */
    public static boolean isEnabledForAnyOfIndexes(List<String> enableIndexesPrefix, List<String> indexPaths) {
        for (String indexPath : indexPaths) {
            if (enableIndexesPrefix.stream().anyMatch(indexPath::startsWith)) {
                return true;
            }
        }
        return false;
    }

    public static List<String> splitAndTrim(String str) {
        if (str == null || str.isBlank()) {
            return List.of();
        } else {
            return Arrays.stream(str.split(",")).map(String::trim).collect(Collectors.toList());
        }
    }

    @Override
    public @NotNull Iterator<NodeStateEntry> iterator() {
        final AheadOfTimeBlobDownloader aheadOfTimeBlobDownloader = createAheadOfTimeBlobDownloader(indexer, indexHelper);
        aheadOfTimeBlobDownloader.start();
        return new Iterator<>() {

            final Iterator<NodeStateEntry> it = ffs.iterator();
            long entriesRead;

            @Override
            public boolean hasNext() {
                boolean result = it.hasNext();
                if (!result) {
                    aheadOfTimeBlobDownloader.updateIndexed(entriesRead);
                    try {
                        aheadOfTimeBlobDownloader.close();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                return result;
            }

            @Override
            public NodeStateEntry next() {
                entriesRead++;
                // No need to update the progress reporter for each entry. This should reduce a bit the
                // overhead of updating the AOT downloader, which sets a volatile field internally.
                if (entriesRead % 128 == 0) {
                    aheadOfTimeBlobDownloader.updateIndexed(entriesRead);
                }
                return it.next();
            }
        };
    }

    @Override
    public String getStorePath() {
        return ffs.getStorePath();
    }

    @Override
    public long getEntryCount() {
        return ffs.getEntryCount();
    }

    @Override
    public void setEntryCount(long entryCount) {
        ffs.setEntryCount(entryCount);
    }

    @Override
    public void close() throws IOException {
        ffs.close();
    }

    @Override
    public String getIndexStoreType() {
        return ffs.getIndexStoreType();
    }

    @Override
    public boolean isIncremental() {
        return ffs.isIncremental();
    }

}
