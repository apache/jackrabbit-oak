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
package org.apache.jackrabbit.oak.index.indexer.document;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.commons.IOUtils;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryReader;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryWriter;
import org.apache.jackrabbit.oak.index.indexer.document.indexstore.IndexStoreUtils;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

/**
 * Downloads blobs ahead of time to avoid blocking the indexing process.
 */
public class AheadOfTimeBlobDownloader implements AheadOfTimeBlobDownloaderInterface {

    private static final Logger LOG = LoggerFactory.getLogger(AheadOfTimeBlobDownloader.class);
    private static final Blob SENTINEL = new BlobStoreBlob(null, null);
    private final static AtomicInteger threadNameCounter = new AtomicInteger(0);

    // Set with the ids of blobs that were already enqueued for download. Avoids creating a large number of download
    // requests for the blobs that are referenced by many nodes. Although this is a HashMap it is used as a set. The
    // value is just a placeholder, we use Boolean.TRUE for no particular reason.
    private static final int DOWNLOADED_BLOB_IDS_CACHE_SIZE = 4096;
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
    private final CompositeIndexer indexer;

    // Statistics
    private final LongAdder totalBytesDownloaded = new LongAdder();
    private final LongAdder totalTimeDownloadingNanos = new LongAdder();
    private final LongAdder totalBlobsDownloaded = new LongAdder();
    private long skippedLinesDueToLaggingIndexing = 0;

    // Download threads plus thread that scans the FFS
    private ExecutorService executor;
    private ScanTask scanTask;
    private Future<?> scanFuture;
    private ArrayList<Future<?>> downloadFutures;
    private final int nDownloadThreads;
    private final AheadOfTimeBlobDownloaderThrottler throttler;
    private volatile long indexerLastKnownPosition;

    public AheadOfTimeBlobDownloader(String binaryBlobsPathSuffix, File ffsPath, Compression algorithm, GarbageCollectableBlobStore blobStore, CompositeIndexer indexer, int nDownloadThreads, int maxPrefetchWindowMB) {
        this.binaryBlobsPathSuffix = binaryBlobsPathSuffix;
        this.ffsPath = ffsPath;
        this.algorithm = algorithm;
        this.blobStore = blobStore;
        this.indexer = indexer;
        this.nDownloadThreads = nDownloadThreads;
        this.throttler = new AheadOfTimeBlobDownloaderThrottler(maxPrefetchWindowMB * FileUtils.ONE_MB);
        LOG.info("Created AheadOfTimeBlobDownloader. downloadThreads: {}, prefetchMB: {}", nDownloadThreads, maxPrefetchWindowMB);
    }

    private boolean isCandidatePath(String path) {
        return path.endsWith(binaryBlobsPathSuffix);
    }

    private class ScanTask implements Runnable {
        private final NodeStateEntryReader nodeStateEntryReader = new NodeStateEntryReader(blobStore);
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
                // Scan for renditions
                String oldName = Thread.currentThread().getName();
                Thread.currentThread().setName("scanner");
                try {
                    while (ffsLineIterator.hasNext()) {
                        String ffsLine = ffsLineIterator.next();
                        int pipeIndex = ffsLine.indexOf(NodeStateEntryWriter.DELIMITER_CHAR);
                        String entryPath = ffsLine.substring(0, pipeIndex);
                        if (!isCandidatePath(entryPath)) {
                            doesNotMatchPattern++;
                        } else if (!indexer.shouldInclude(entryPath)) {
                            notIncludedInIndex++;
                        } else if (isBehindIndexer(linesScanned)) {
                            LOG.debug("Skipping blob at position {} because it was already indexed", linesScanned);
                            skippedLinesDueToLaggingIndexing++;
                        } else {
                            processEntry(entryPath, ffsLine.substring(pipeIndex + 1));
                        }
                        linesScanned++;
                        if (linesScanned % 100_000 == 0) {
                            LOG.info("[{}] Last path scanned: {}. Aggregated statistics: {}", linesScanned, entryPath, formatStatistics());
                        }
                    }
                } catch (InterruptedException e) {
                    queue.clear();
                    LOG.info("Scan task interrupted, exiting");
                } finally {
                    LOG.info("Scanner reached end of FFS, stopping download threads. Statistics: {}", formatStatistics());
                    Thread.currentThread().setName(oldName);
                    queue.put(SENTINEL);
                }
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException(e);
            }
        }

        private boolean isBehindIndexer(long scannerPosition) {
            // Always try to be ahead of the indexer
            return scannerPosition <= indexerLastKnownPosition;
        }

        private void processEntry(String entryPath, String entryStateAsJson) throws InterruptedException {
            NodeState nodeState = nodeStateEntryReader.parseState(entryStateAsJson);
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
                // Add to a set with all blobs that are already downloaded or enqueued for download
                boolean present = downloadedBlobs.containsKey(blob.getContentIdentity());
                if (present) {
                    blobCacheHit++;
                    LOG.debug("[{}] Blob already downloaded or enqueued for download: {}", linesScanned, blob.getContentIdentity());
                    continue;
                }
                throttler.reserveSpaceForBlob(linesScanned, blob.length());
                downloadedBlobs.put(blob.getContentIdentity(), Boolean.TRUE);
                queue.put(blob);
            }
        }
    }

    private class DownloadTask implements Runnable {
        private final ArrayBlockingQueue<Blob> queue;

        private long blobsDownloaded = 0;
        private long bytesDownloaded = 0;
        private long timeDownloadingNanos = 0;

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
                        LOG.info("Sentinel received, exiting. Statistics: {}", formatStats());
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
                        if (totalBlobsDownloaded.sum() % 1000 == 0) {
                            LOG.info("Retrieved blob: {}, size: {}, in {} ms. Global statistics: {}",
                                    blob.getContentIdentity(), blob.length(), elapsedNanos / 1_000_000, formatStatistics());
                        }
                    } catch (IOException e) {
                        LOG.error("Error downloading blob: {}", blob.getContentIdentity(), e);
                    }
                }
            } catch (InterruptedException e) {
                LOG.info("Download task interrupted, exiting. Statistics: {}", formatStats());
            } finally {
                Thread.currentThread().setName(oldName);
            }
        }

        String formatStats() {
            return String.format("Downloaded %d blobs, %d bytes (%s) in %d seconds",
                    blobsDownloaded, bytesDownloaded, IOUtils.humanReadableByteCountBin(bytesDownloaded),
                    TimeUnit.NANOSECONDS.toSeconds(timeDownloadingNanos));
        }
    }

    public void start() {
        executor = Executors.newFixedThreadPool(nDownloadThreads + 1);
        ArrayBlockingQueue<Blob> queue = new ArrayBlockingQueue<>(nDownloadThreads * 4);

        downloadFutures = new ArrayList<>();
        for (int i = 0; i < nDownloadThreads; i++) {
            DownloadTask downloadTask = new DownloadTask(queue);
            downloadFutures.add(executor.submit(downloadTask));
        }
        scanTask = new ScanTask(queue);
        scanFuture = executor.submit(scanTask);
    }

    public void close() {
        stop();
    }

    public void stop() {
        if (executor == null) {
            return;
        }
        LOG.info("Stopping AheadOfTimeBlobDownloader. Statistics: {}", formatStatistics());
        scanFuture.cancel(true);
        for (Future<?> downloadFuture : downloadFutures) {
            downloadFuture.cancel(true);
        }
        LOG.info("Waiting for download tasks to finish");
        new ExecutorCloser(executor).close();
        executor = null;
        LOG.info("All download tasks finished");
    }

    public void updateIndexed(long positionIndexed) {
        this.indexerLastKnownPosition = positionIndexed;
        if (!scanFuture.isDone()) {
            throttler.advanceIndexer(positionIndexed);
        }
    }

    public String formatStatistics() {
        long totalBytesDownloadedSum = totalBytesDownloaded.sum();
        return String.format(
                "Downloaded %d blobs, %d bytes (%s). Aggregated download time across threads: %d seconds. " +
                        "cacheHits: %d, linesScanned: %d, notIncludedInIndex: %d, " +
                        "doesNotMatchPattern: %d, inlinedBlobsSkipped: %d, skippedForOtherReasons: %d, skippedLinesDueToLaggingIndexing: %d",
                totalBlobsDownloaded.sum(), totalBytesDownloadedSum, IOUtils.humanReadableByteCountBin(totalBytesDownloadedSum),
                TimeUnit.NANOSECONDS.toSeconds(totalTimeDownloadingNanos.sum()),
                scanTask.blobCacheHit, scanTask.linesScanned, scanTask.notIncludedInIndex, scanTask.doesNotMatchPattern,
                scanTask.inlinedBlobsSkipped, scanTask.skippedForOtherReasons, skippedLinesDueToLaggingIndexing);
    }
}
