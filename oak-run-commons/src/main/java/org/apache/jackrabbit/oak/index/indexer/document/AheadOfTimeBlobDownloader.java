package org.apache.jackrabbit.oak.index.indexer.document;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryReader;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryWriter;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined.ConfigHelper;
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


public class AheadOfTimeBlobDownloader {
    private static final Logger LOG = LoggerFactory.getLogger(AheadOfTimeBlobDownloader.class);
    private static final Blob SENTINEL = new BlobStoreBlob(null, null);
    private final static AtomicInteger threadNameCounter = new AtomicInteger(0);

    public final static String BINARY_BLOB_SUFFIX = "jcr:content/renditions/cqdam.text.txt/jcr:content";

    private final int nDownloadThreads = ConfigHelper.getSystemPropertyAsInt("oak.indexer.document.prefetchBlobsDownloadThreads", 8);
    private final int prefetchWindowSizeMB = ConfigHelper.getSystemPropertyAsInt("oak.indexer.document.prefetchWindowSizeMB", 8);

    // Download threads plus thread that scans the FFS
    private final ExecutorService executor = Executors.newFixedThreadPool(nDownloadThreads + 1);
    // Set with the ids of blobs that were already enqueued for download. Avoids creating a large number of download
    // requests for the blobs that are referenced by many nodes. Although this is a HashMap it is used as a set. The
    // value is just a placeholder, we use Boolean.TRUE for no particular reason.
    private final LinkedHashMap<String, Boolean> downloadedBlobs = new LinkedHashMap<>(4096, 0.75f, true) {
        final int MAX_ENTRIES = 3 * 1000;

        protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > MAX_ENTRIES;
        }
    };

    private final File ffsPath;
    private final Compression algorithm;
    private final GarbageCollectableBlobStore blobStore;
    private final CompositeIndexer indexer;

    // Statistics
    private final LongAdder totalBytesDownloaded = new LongAdder();
    private final LongAdder totalTimeDownloadingNanos = new LongAdder();
    private final LongAdder totalBlobsDownloaded = new LongAdder();
    private long skippedLinesDueToLaggingIndexing = 0;

    private ScanTask scanTask;
    private Future<?> scanFuture;
    private ArrayList<DownloadTask> downloadTasks;
    private ArrayList<Future<?>> downloadFutures;

    private final AheadOfTimeBlobDownloaderThrottler throttler;
    private volatile long indexerLastKnownPosition;
    private volatile boolean finished = false;


    /**
     * @param ffsPath
     * @param algorithm
     * @param blobStore
     * @param indexer
     */
    public AheadOfTimeBlobDownloader(File ffsPath, Compression algorithm, GarbageCollectableBlobStore blobStore, CompositeIndexer indexer) {
        this.ffsPath = ffsPath;
        this.algorithm = algorithm;
        this.blobStore = blobStore;
        this.indexer = indexer;

        this.throttler = new AheadOfTimeBlobDownloaderThrottler(prefetchWindowSizeMB * FileUtils.ONE_MB);
        LOG.info("Created AheadOfTimeBlobDownloader. downloadThreads: {}, prefetchMB: {}",
                nDownloadThreads, prefetchWindowSizeMB);
    }

    public void updateIndexed(long positionIndexed) {
        this.indexerLastKnownPosition = positionIndexed;
        if (!finished) {
            throttler.advanceIndexer(positionIndexed);
        }
    }

    private class ScanTask implements Runnable {
        private final NodeStateEntryReader nodeStateEntryReader = new NodeStateEntryReader(blobStore);
        private final ArrayBlockingQueue<Blob> queue;

        long blobCacheHit = 0;
        long linesScanned = 0;
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
                        if (!entryPath.endsWith(BINARY_BLOB_SUFFIX)) {
                            doesNotMatchPattern++;
                        } else if (!indexer.shouldInclude(entryPath)) {
                            notIncludedInIndex++;
                            LOG.info("Skipping node: {}. Not included in index", entryPath);
                        } else if (isBehindIndexer(linesScanned)) {
                            LOG.warn("Skipping blob at position {} because it was already indexed", linesScanned);
                            skippedLinesDueToLaggingIndexing++;
                        } else {
                            processEntry(entryPath, ffsLine.substring(pipeIndex + 1));
                        }
                        linesScanned++;
                        if (linesScanned % 100_000 == 0) {
                            LOG.info("[{}] Last path scanned: {}. Statistics: {}", linesScanned, entryPath, formatStatistics());
                        }
                    }
                } catch (InterruptedException e) {
                    queue.clear();
                    LOG.info("Scan task interrupted, exiting");
                } finally {
                    LOG.info("Scanned {} entries, enqueuing SENTINEL. Statistics: {}", linesScanned, formatStatistics());
                    Thread.currentThread().setName(oldName);
                    finished = true;
                    queue.put(SENTINEL);
                }
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException(e);
            }
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
                        LOG.info("Sentinel received, exiting");
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
                        totalBytesDownloaded.add(blobSize);
                        long elapsedNanos = System.nanoTime() - startNanos;
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
                LOG.info("Download task interrupted, exiting");
            } finally {
                Thread.currentThread().setName(oldName);
            }
        }
    }

    public void start() {
        ArrayBlockingQueue<Blob> queue = new ArrayBlockingQueue<>(nDownloadThreads * 4);

        downloadTasks = new ArrayList<>();
        downloadFutures = new ArrayList<>();
        for (int i = 0; i < nDownloadThreads; i++) {
            DownloadTask downloadTask = new DownloadTask(queue);
            downloadTasks.add(downloadTask);
            downloadFutures.add(executor.submit(downloadTask));
        }
        scanTask = new ScanTask(queue);
        scanFuture = executor.submit(scanTask);
    }

    public String formatStatistics() {
        return String.format(
                "Downloaded %d blobs, %d bytes in %d ms. cacheHit: %d, linesScanned: %d, notIncludedInIndex: %d, " +
                        "doesNotMatchPattern: %d, inlinedBlobsSkipped: %d, skippedForOtherReasons: %d, skippedLinesDueToLaggingIndexing: %d",
                totalBlobsDownloaded.sum(), totalBytesDownloaded.sum(), totalTimeDownloadingNanos.sum() / 1000000,
                scanTask.blobCacheHit, scanTask.linesScanned, scanTask.notIncludedInIndex, scanTask.doesNotMatchPattern,
                scanTask.inlinedBlobsSkipped, scanTask.skippedForOtherReasons, skippedLinesDueToLaggingIndexing);
    }

    private boolean isBehindIndexer(long scannerPosition) {
        // Always try to be ahead of the indexer
        return scannerPosition <= indexerLastKnownPosition;
    }

    public void stop() {
        LOG.info("Stopping BlobDownloader");
        LOG.info("loadBlobs.size(): {}", downloadedBlobs.size());
        scanFuture.cancel(true);
        LOG.info("Waiting for download tasks to finish");
        for (Future<?> downloadFuture : downloadFutures) {
            downloadFuture.cancel(true);
        }
        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while waiting for download tasks to finish");
            Thread.currentThread().interrupt();
        }
        LOG.info("All download tasks finished");
    }
}
