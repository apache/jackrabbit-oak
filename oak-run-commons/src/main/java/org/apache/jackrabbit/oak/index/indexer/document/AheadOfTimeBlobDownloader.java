package org.apache.jackrabbit.oak.index.indexer.document;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.guava.common.collect.AbstractIterator;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.FlatFileStoreUtils;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.NodeStateEntryReader;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
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
    private static final Blob POISON_PILL = new BlobStoreBlob(null, null);
    private final static AtomicInteger threadNameCounter = new AtomicInteger(0);

    private final static long MINIMUM_STAY_AHEAD = 1024;
    private final int nDownloadThreads = parseEnv("PREFETCH_BLOBS_DOWNLOAD_THREADS", 8);
    private final ExecutorService executor = Executors.newFixedThreadPool(nDownloadThreads + 1);
    private final int PREFETCH_BLOBS_SIZE_MB = parseEnv("PREFETCH_BLOBS_WINDOW_SIZE_MB", 16);

    private final LinkedHashMap<String, String> loadedBlobs = new LinkedHashMap<>(8192, 0.75f, true) {
        final int MAX_ENTRIES = 6 * 1000;

        protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > MAX_ENTRIES;
        }
    };

    // Statistics
    private final LongAdder totalBytesDownloaded = new LongAdder();
    private final LongAdder totalTimeDownloadingNanos = new LongAdder();
    private final LongAdder totalBlobsDownloaded = new LongAdder();
    private final CompositeIndexer indexer;
    private long skippedLinesDueToLaggingIndexing = 0;

    private final Iterator<NodeStateEntry> flatFileStore;

    private Future<?> scanFuture;
    private ArrayList<DownloadTask> downloadTasks;
    private ArrayList<Future<?>> downloadFutures;

    private final AheadOfTimeBlobDownloaderThrottler throttler;
    private ScanTask scanTask;

    private volatile long indexerLastKnownPosition;
    private volatile boolean finished = false;

    private static int parseEnv(String env, int defaultValue) {
        String envVal = System.getenv(env);
        int val = defaultValue;
        if (envVal != null) {
            try {
                val = Integer.parseInt(envVal);
            } catch (NumberFormatException e) {
                LOG.warn("Invalid value for environment variable {}: {}", env, envVal);
            }
        }
        return val;
    }

    public AheadOfTimeBlobDownloader(File ffsPath, Compression algorithm, GarbageCollectableBlobStore blobStore, CompositeIndexer indexer) {
        this.indexer = indexer;
        LineIterator itr = new LineIterator(FlatFileStoreUtils.createReader(ffsPath, algorithm));
        NodeStateEntryReader reader = new NodeStateEntryReader(blobStore);
        this.flatFileStore = new AbstractIterator<>() {
            @Override
            protected NodeStateEntry computeNext() {
                if (itr.hasNext()) {
                    return reader.read(itr.nextLine());
                }
                //End of iterator then close it
                LineIterator.closeQuietly(itr);
                return endOfData();
            }
        };
        this.throttler = new AheadOfTimeBlobDownloaderThrottler(PREFETCH_BLOBS_SIZE_MB * FileUtils.ONE_MB);
        LOG.info("Created AheadOfTimeBlobDownloader. downloadThreads: {}, prefetchMB: {}, minimumStayAhead: {}",
                nDownloadThreads, PREFETCH_BLOBS_SIZE_MB, MINIMUM_STAY_AHEAD);
    }

    public void updateIndexed(long positionIndexed) {
        this.indexerLastKnownPosition = positionIndexed;
        if (!finished) {
            throttler.advanceIndexer(positionIndexed);
        }
    }

    private class ScanTask implements Runnable {
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
            try {
                // Scan for renditions
                String oldName = Thread.currentThread().getName();
                Thread.currentThread().setName("scanner");
                try {
                    while (flatFileStore.hasNext()) {
                        NodeStateEntry entry = flatFileStore.next();
                        if (isBehindIndexer(linesScanned)) {
//                            LOG.warn("Skipping blob at position {} because it was already indexed", linesScanned);
                            skippedLinesDueToLaggingIndexing++;
                        } else {
                            processEntry(entry);
                        }
                        linesScanned++;
                        if (linesScanned % 100_000 == 0) {
                            LOG.info("[{}] Last path scanned: {}. Statistics: {}", linesScanned, entry.getPath(), formatStatistics());
                        }
                    }
                } catch (InterruptedException e) {
                    queue.clear();
                    LOG.info("Scan task interrupted, exiting");
                } finally {
                    LOG.info("Scanned {} entries, enqueuing POISON_PILL. Statistics: {}", linesScanned, formatStatistics());
                    Thread.currentThread().setName(oldName);
                    finished = true;
                    queue.put(POISON_PILL);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        private void processEntry(NodeStateEntry entry) throws InterruptedException {
            String entryPath = entry.getPath();
            if (!entryPath.endsWith("jcr:content/renditions/cqdam.text.txt/jcr:content")) {
                doesNotMatchPattern++;
                return;
            }
            if (!indexer.shouldInclude(entryPath)) {
                notIncludedInIndex++;
                LOG.info("Skipping node: {}. Not included in index", entryPath);
                return;
            }
            PropertyState ps = entry.getNodeState().getProperty("jcr:data");
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
                boolean present = loadedBlobs.containsValue(blob.getContentIdentity());
                if (present) {
                    blobCacheHit++;
                    LOG.debug("[{}] Blob already downloaded or enqueued for download: {}", linesScanned, blob.getContentIdentity());
                    continue;
                }
                throttler.reserveSpaceForBlob(linesScanned, blob.length());
                loadedBlobs.put(blob.getContentIdentity(), blob.getContentIdentity());
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
                Stopwatch watch = Stopwatch.createUnstarted();
                while (true) {
                    Blob blob = queue.take();
                    if (blob == POISON_PILL) {
                        LOG.info("Poison pill received, exiting");
                        queue.put(POISON_PILL);
                        break;
                    }

                    watch.start();
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
                        long elapsedNanos = watch.stop().elapsed(TimeUnit.NANOSECONDS);
                        totalTimeDownloadingNanos.add(elapsedNanos);
                        totalBlobsDownloaded.increment();
                        if (totalBlobsDownloaded.sum() % 1000 == 0) {
                            LOG.info("Retrieved blob: {}, size: {}, in {} ms. Global statistics: {}", blob.getContentIdentity(), blob.length(), elapsedNanos / 1000000, formatStatistics());
                        }
                        watch.reset();
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

    private boolean isBehindIndexer(long position) {
        // Always try to be ahead of the indexer
        return position <= indexerLastKnownPosition + MINIMUM_STAY_AHEAD;
    }

    public void stop() {
        LOG.info("Stopping BlobDownloader");
        LOG.info("loadBlobs.size(): {}", loadedBlobs.size());
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
