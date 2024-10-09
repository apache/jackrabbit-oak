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

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateIndexer;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreUtils;
import org.apache.jackrabbit.oak.json.JsonDeserializer;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.blob.serializer.BlobIdSerializer;
import org.apache.jackrabbit.oak.plugins.index.FormattingUtils;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

/**
 * Scans a FlatFileStore for non-inlined blobs in nodes matching a given pattern and downloads them from the blob store.
 * The goal of this class is to populate the local data store cache with the non-inlined blobs that are required by the
 * indexer, so that when the indexing thread tries to retrieve the blob, it will find it locally, thereby avoiding an
 * expensive call to the blob store. When indexing repositories with many non-inlined renditions, pre-populating the
 * cache can cut the indexing time by more than half.
 * <p>
 * This AOT download is intended to run asynchronously with the indexing thread. It starts the following threads:
 * <ul>
 * <li>[scanner] - scans the FFS, searching for blobs to download. A blob is selected for download if it is a binary property
 * in a node whose name matches the suffix given as parameter to this class, and is non-inlined.</li>
 * <li>[downloader-n] - a configurable number of threads that download the blobs that were discovered by the scanner thread.</li>
 * </ul>
 * The indexer should periodically call {@link #updateIndexed(long)} to inform the AOT downlaoder of the last line
 * indexed. This is necessary to keep the AOT downloader more or less in sync with the indexer, that is, to prevent it
 * from falling behind and to prevent it from going to far ahead.
 * <p>
 * This AOT downloader should be configured with enough threads that it is able to stay ahead of the indexer. Whether it
 * can remain ahead or not, will depend on the number of blobs to download and the speed of the connection to the blob
 * store. As a rough guide, on a cloud environment with blob stored in Azure Blob Store or Amazon S3, 4 download threads
 * should be enough. If the AOT downloader falls behind the indexer, it will skip any nodes that are behind the last known
 * indexing position, to try to catchup.
 * <p>
 * The AOT downlaoder will also try not to be too far ahead of the indexer. This is done to avoid filling up the local
 * blob store cache, which would cause blobs to be evicted before the indexer gets around to use them. In this case, the
 * indexer would have to download again the blob, which would negate the benefits of using this AOT downloader.
 * The AOT downlaoder takes as parameter the maximum amount of data that it is allowed to prefetch (<code>maxPrefetchWindowMB</code>).
 * It will them try to not download more than this data, pausing its progress whenever the prefect window is full.
 *  For details on how this implemented, see {@link AheadOfTimeBlobDownloaderThrottler}.
 */
public class DefaultAheadOfTimeBlobDownloader implements AheadOfTimeBlobDownloader {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultAheadOfTimeBlobDownloader.class);
    // Stops the downloader threads.
    private static final Blob SENTINEL = new BlobStoreBlob(null, null);
    private final static AtomicInteger threadNameCounter = new AtomicInteger(0);

    // Set with the ids of blobs that were already enqueued for download. Avoids creating a large number of download
    // requests for the blobs that are referenced by many nodes. Although this is a HashMap it is used as a set. The
    // value is just a placeholder, we use Boolean.TRUE for no particular reason.
    private static final int DOWNLOADED_BLOB_IDS_CACHE_SIZE = 1024;
    private final LinkedHashMap<String, Boolean> downloadedBlobs = new LinkedHashMap<>(DOWNLOADED_BLOB_IDS_CACHE_SIZE, 0.75f, true) {
        // Avoid resizing operations
        private final static int MAX_ENTRIES = (int) (DOWNLOADED_BLOB_IDS_CACHE_SIZE * 0.70);

        protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > MAX_ENTRIES;
        }
    };

    private final String binaryBlobsPathSuffix;
    private final File ffsPath;
    private final Compression algorithm;
    private final GarbageCollectableBlobStore blobStore;
    private final List<NodeStateIndexer> indexers;

    // Statistics
    private final LongAdder totalBytesDownloaded = new LongAdder();
    private final LongAdder totalTimeDownloadingNanos = new LongAdder();
    private final LongAdder totalBlobsDownloaded = new LongAdder();
    private long blobsEnqueuedForDownload = 0;
    private long skippedLinesDueToLaggingIndexing = 0;

    // Download threads plus thread that scans the FFS
    private ExecutorService executor;
    private ScanTask scanTask;
    private Future<?> scanFuture;
    private ArrayList<Future<?>> downloadFutures;
    private final int nDownloadThreads;
    private final AheadOfTimeBlobDownloaderThrottler throttler;
    private volatile long indexerLastKnownPosition = -1;

    /**
     * @param binaryBlobsPathSuffix Suffix of nodes that are to be considered for AOT download. Any node that does not match this suffix is ignored.
     * @param ffsPath               Flat file store path.
     * @param algorithm             Compression algorithm of the flat file store.
     * @param blobStore             The blob store. This should be the same blob store used by the indexer and its cache should be
     *                              large enough to hold <code>maxPrefetchWindowMB</code> of data.
     * @param indexers              The indexeres for which AOT blob download is enabled.
     * @param nDownloadThreads      Number of download threads.
     * @param maxPrefetchWindowMB   Size of the prefetch window, that is, how much data the downlaoder will retrieve ahead of the indexer.
     */
    public DefaultAheadOfTimeBlobDownloader(@NotNull String binaryBlobsPathSuffix,
                                            @NotNull File ffsPath, @NotNull Compression algorithm,
                                            @NotNull GarbageCollectableBlobStore blobStore,
                                            @NotNull List<NodeStateIndexer> indexers,
                                            int nDownloadThreads, int maxPrefetchWindowSize, int maxPrefetchWindowMB) {
        if (nDownloadThreads < 1) {
            throw new IllegalArgumentException("nDownloadThreads must be greater than 0. Was: " + nDownloadThreads);
        }
        if (maxPrefetchWindowMB < 1) {
            throw new IllegalArgumentException("maxPrefetchWindowMB must be greater than 0. Was: " + maxPrefetchWindowMB);
        }
        this.binaryBlobsPathSuffix = binaryBlobsPathSuffix;
        this.ffsPath = ffsPath;
        this.algorithm = algorithm;
        this.blobStore = blobStore;
        this.indexers = indexers;
        this.nDownloadThreads = nDownloadThreads;
        this.throttler = new AheadOfTimeBlobDownloaderThrottler(maxPrefetchWindowSize, maxPrefetchWindowMB * FileUtils.ONE_MB);
        LOG.info("Created AheadOfTimeBlobDownloader. downloadThreads: {}, prefetchMB: {}, enabledIndexes: {}",
                nDownloadThreads, maxPrefetchWindowMB, indexers.stream().map(NodeStateIndexer::getIndexName).collect(Collectors.toList()));
    }

    public void start() {
        executor = Executors.newFixedThreadPool(nDownloadThreads + 1);
        ArrayBlockingQueue<Blob> queue = new ArrayBlockingQueue<>(nDownloadThreads * 2);

        downloadFutures = new ArrayList<>();
        for (int i = 0; i < nDownloadThreads; i++) {
            DownloadTask downloadTask = new DownloadTask(queue);
            downloadFutures.add(executor.submit(downloadTask));
        }
        scanTask = new ScanTask(queue);
        scanFuture = executor.submit(scanTask);
    }

    public void join() throws ExecutionException, InterruptedException {
        scanFuture.get();
        for (Future<?> downloadFuture : downloadFutures) {
            downloadFuture.get();
        }
    }

    public void updateIndexed(long positionIndexed) {
        this.indexerLastKnownPosition = positionIndexed;
        throttler.advanceIndexer(positionIndexed);
    }

    public void close() {
        stop();
    }

    public void stop() {
        if (executor == null) {
            return;
        }
        LOG.info("Stopping AheadOfTimeBlobDownloader. Statistics: {}", formatAggregateStatistics());
        scanFuture.cancel(true);
        for (Future<?> downloadFuture : downloadFutures) {
            downloadFuture.cancel(true);
        }
        LOG.info("Waiting for download tasks to finish");
        new ExecutorCloser(executor).close();
        executor = null;
        LOG.info("All download tasks finished");
    }

    public String formatAggregateStatistics() {
        long totalBytesDownloadedSum = totalBytesDownloaded.sum();
        return String.format(
                "Downloaded %d blobs, %d bytes (%s). aggregatedDownloadTime: %s, cacheHits: %d, linesScanned: %d, " +
                        "notIncludedInIndex: %d, doesNotMatchPattern: %d, inlinedBlobsSkipped: %d, " +
                        "skippedForOtherReasons: %d, skippedLinesDueToLaggingIndexing: %d",
                totalBlobsDownloaded.sum(), totalBytesDownloadedSum, IOUtils.humanReadableByteCountBin(totalBytesDownloadedSum),
                FormattingUtils.formatNanosToSeconds(totalTimeDownloadingNanos.sum()),
                scanTask.blobCacheHit, scanTask.linesScanned, scanTask.notIncludedInIndex, scanTask.doesNotMatchPattern,
                scanTask.inlinedBlobsSkipped, scanTask.skippedForOtherReasons, skippedLinesDueToLaggingIndexing);
    }

    /**
     * Scans the FFS, searching for binary properties that are not inlined and enqueues them for download.
     */
    private class ScanTask implements Runnable {
        private final JsonDeserializer jsonDeserializer = new JsonDeserializer(new BlobIdSerializer(blobStore));
        private final ArrayBlockingQueue<Blob> queue;

        long linesScanned = 0;
        long blobCacheHit = 0;
        long notIncludedInIndex = 0;
        long doesNotMatchPattern = 0;
        long inlinedBlobsSkipped = 0;
        long skippedForOtherReasons = 0;

        public ScanTask(ArrayBlockingQueue<Blob> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            try (LineIterator ffsLineIterator = new LineIterator(IndexStoreUtils.createReader(ffsPath, algorithm))) {
                String oldName = Thread.currentThread().getName();
                Thread.currentThread().setName("scanner");
                LOG.info("Starting scanning FFS for blobs to download, matching suffix: {}", binaryBlobsPathSuffix);
                try {
                    while (ffsLineIterator.hasNext()) {
                        String ffsLine = ffsLineIterator.next();
                        // Do not parse the json with the node state yet, first check if the path is a possible candidate
                        // for download. Most of the lines in the FFS will be of lines that do not contain blobs to download,
                        // so it would be wasteful to parse the json for all of them.
                        int pipeIndex = ffsLine.indexOf(NodeStateEntryWriter.DELIMITER_CHAR);
                        String entryPath = ffsLine.substring(0, pipeIndex);
                        if (!isCandidatePath(entryPath)) {
                            doesNotMatchPattern++;
                        } else if (indexers.stream().noneMatch(indexer -> indexer.shouldInclude(entryPath))) {
                            notIncludedInIndex++;
                        } else if (isBehindIndexer(linesScanned)) {
                            LOG.debug("Skipping blob at position {} because it was already indexed", linesScanned);
                            skippedLinesDueToLaggingIndexing++;
                        } else {
                            // Now we need to parse the json to check if there are any blobs to download
                            NodeState nodeState = jsonDeserializer.deserialize(ffsLine, pipeIndex + 1);
                            processEntry(entryPath, nodeState);
                        }
                        linesScanned++;
                        if (linesScanned % 1_000_000 == 0) {
                            LOG.info("[{}] Last path scanned: {}. Aggregated statistics: {}", linesScanned, entryPath, formatAggregateStatistics());
                        }
                    }
                } catch (InterruptedException e) {
                    queue.clear();
                    LOG.info("Scan task interrupted, exiting");
                } finally {
                    LOG.info("Scanner reached end of FFS, stopping download threads. Statistics: {} {}", formatAggregateStatistics(), throttler.formatStats());
                    Thread.currentThread().setName(oldName);
                    queue.put(SENTINEL);
                }
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException(e);
            }
        }

        private boolean isCandidatePath(String path) {
            return path.endsWith(binaryBlobsPathSuffix);
        }

        private boolean isBehindIndexer(long scannerPosition) {
            // Always try to be ahead of the indexer
            return scannerPosition <= indexerLastKnownPosition;
        }

        private void processEntry(String entryPath, NodeState nodeState) throws InterruptedException {
            PropertyState ps = nodeState.getProperty("jcr:data");
            if (ps == null || ps.isArray() || ps.getType() != Type.BINARY) {
                skippedForOtherReasons++;
                LOG.info("Skipping node: {}. Property \"jcr:data\": {}", entryPath, ps);
                return;
            }
            for (Blob blob : ps.getValue(Type.BINARIES)) {
                if (blob.isInlined()) {
                    inlinedBlobsSkipped++;
                    continue;
                }
                if (blob.getContentIdentity() == null) {
                    LOG.info("[{}] Skipping blob with null content identity: {}", linesScanned, blob.getContentIdentity());
                    continue;
                }
                // Check if we have recently downloaded this blob. This is just an optimization. Without this cache,
                // if a blob had been downloaded recently, further attempts to download it would hit the blob store cache
                // and complete quickly. But as it is common for the same blob to be referenced by many entries, this simple
                // check here avoids the extra work of enqueuing the blob for download and reading it from the cache.
                boolean present = downloadedBlobs.containsKey(blob.getContentIdentity());
                if (present) {
                    blobCacheHit++;
                    LOG.debug("[{}] Blob already downloaded or enqueued for download: {}", linesScanned, blob.getContentIdentity());
                    continue;
                }
                if (!throttler.reserveSpaceForBlob(linesScanned, blob.length())) {
                    skippedLinesDueToLaggingIndexing++;
                    continue;
                }
                downloadedBlobs.put(blob.getContentIdentity(), Boolean.TRUE);
                queue.put(blob);
                blobsEnqueuedForDownload++;
                // Log progress
                if (blobsEnqueuedForDownload % 1000 == 0) {
                    LOG.info("[{}] Enqueued blob for download: {}, size: {}, Statistics: {}, {}",
                            linesScanned, blob.getContentIdentity(), blob.length(),
                            formatAggregateStatistics(), throttler.formatStats());
                }
            }
        }
    }

    /**
     * Downloads blobs from the blob store.
     */
    private class DownloadTask implements Runnable {
        private final ArrayBlockingQueue<Blob> queue;

        private long blobsDownloaded = 0;
        private long bytesDownloaded = 0;
        private long timeDownloadingNanos = 0;

        /**
         * @param queue The queue from which to take blobs to download.
         */
        public DownloadTask(ArrayBlockingQueue<Blob> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            String oldName = Thread.currentThread().getName();
            Thread.currentThread().setName("downloader-" + threadNameCounter.getAndIncrement());
            byte[] buffer = new byte[4096];
            try {
                while (true) {
                    Blob blob = queue.take();
                    if (blob == SENTINEL) {
                        LOG.info("Sentinel received, exiting. Statistics: {}", formatDownloaderStats());
                        queue.put(SENTINEL);
                        break;
                    }

                    long startNanos = System.nanoTime();
                    InputStream stream = blob.getNewStream();
                    int blobSize = 0;
                    try {
                        while (true) {
                            int bytesRead = stream.read(buffer);
                            if (bytesRead == -1) {
                                break;
                            }
                            blobSize += bytesRead;
                        }
                        if (blobSize != blob.length()) {
                            LOG.error("Blob size mismatch: blob.length(): {}, bytesRead: {}", blob.length(), blobSize);
                        }
                        long elapsedNanos = System.nanoTime() - startNanos;
                        // Local stats
                        bytesDownloaded += blobSize;
                        blobsDownloaded++;
                        timeDownloadingNanos += elapsedNanos;
                        // Aggregated stats across all download threads.
                        totalBytesDownloaded.add(blobSize);
                        totalTimeDownloadingNanos.add(elapsedNanos);
                        totalBlobsDownloaded.increment();
                        // Log progress
                        if (blobsDownloaded % 500 == 0) {
                            LOG.info("Retrieved blob: {}, size: {}, in {} ms. Downloader thread statistics: {}",
                                    blob.getContentIdentity(), blob.length(), elapsedNanos / 1_000_000, formatDownloaderStats());
                        }
                    } catch (IOException e) {
                        LOG.error("Error downloading blob: {}", blob.getContentIdentity(), e);
                    }
                }
            } catch (InterruptedException e) {
                LOG.info("Download task interrupted, exiting. Statistics: {}", formatDownloaderStats());
            } finally {
                Thread.currentThread().setName(oldName);
            }
        }

        private String formatDownloaderStats() {
            return String.format("Downloaded %d blobs, %d bytes (%s) in %s",
                    blobsDownloaded, bytesDownloaded, IOUtils.humanReadableByteCountBin(bytesDownloaded),
                    FormattingUtils.formatNanosToSeconds(timeDownloadingNanos));
        }
    }


    public long getBlobsEnqueuedForDownload() {
        return blobsEnqueuedForDownload;
    }

    public long getTotalBlobsDownloaded() {
        return totalBlobsDownloaded.sum();
    }


    public long getLinesScanned() {
        return scanTask.linesScanned;
    }

    public long getNotIncludedInIndex() {
        return scanTask.notIncludedInIndex;
    }
}
