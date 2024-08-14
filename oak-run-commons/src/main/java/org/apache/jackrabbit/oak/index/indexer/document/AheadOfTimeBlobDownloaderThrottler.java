package org.apache.jackrabbit.oak.index.indexer.document;


import org.apache.jackrabbit.oak.commons.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class AheadOfTimeBlobDownloaderThrottler {
    private final static Logger LOG = LoggerFactory.getLogger(AheadOfTimeBlobDownloaderThrottler.class);
    public final long downloadBudgetBytes;
    private final int numberOfBuckets = 16;
    private final long bucketSizeBytes;

    static class PrefetchWindow {
        final long start;
        final long end;
        final long sizeInBytes;

        public PrefetchWindow(long start, long end, long sizeInBytes) {
            this.start = start;
            this.end = end;
            this.sizeInBytes = sizeInBytes;
        }

        @Override
        public String toString() {
            return "PrefetchWindow{" +
                    "start=" + start +
                    ", end=" + end +
                    ", sizeInBytes=" + sizeInBytes +
                    '}';
        }
    }

    static class DownloadedBlock {
        final long firstIndex;
        final long lastIndex;
        private final long dataInBlock;

        public DownloadedBlock(long start, long end, long dataInBlock) {
            this.firstIndex = start;
            this.lastIndex = end;
            this.dataInBlock = dataInBlock;
        }

        @Override
        public String toString() {
            return "DownloadedBlock{" +
                    "firstIndex=" + firstIndex +
                    ", lastIndex=" + lastIndex +
                    ", dataInBlock=" + dataInBlock +
                    '}';
        }
    }

    private final ArrayDeque<DownloadedBlock> aotDownloadedBlocks = new ArrayDeque<>();

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    private long dataPendingStartPosition = 0;
    private long lastPosition = 0;
    private long dataInDownloadedBlocks = 0;
    private long dataPending = 0;

    public AheadOfTimeBlobDownloaderThrottler(long downloadBudgetBytes) {
        this.downloadBudgetBytes = downloadBudgetBytes;
        this.bucketSizeBytes = downloadBudgetBytes / numberOfBuckets;
    }

    long reserveSpaceForBlob(long blobPosition, long length) throws InterruptedException {
        if (blobPosition <= dataPendingStartPosition) {
            LOG.warn("Position should be greater than start. position: {}, start: {}", blobPosition, dataPendingStartPosition);
        }
        this.lastPosition = blobPosition;
        LOG.debug("Reserving space for blob. Position: {}, length: {}", blobPosition, length);
        try {
            lock.lock();
            dataPending += length;
            if (dataPending >= this.bucketSizeBytes) {
                var block = new DownloadedBlock(dataPendingStartPosition, blobPosition, dataPending);
                aotDownloadedBlocks.addLast(block);
                dataPendingStartPosition = blobPosition;
                dataPending = 0;
                dataInDownloadedBlocks += block.dataInBlock;
                LOG.info("Creating block: {}. aotDownloaded: {}", block, prefetchedWindowSizeInBytes());
            }
            // Check if we have space for more data
            long downloadedButNotConsumed = prefetchedWindowSizeInBytes();
            if (downloadedButNotConsumed < downloadBudgetBytes) {
                return downloadBudgetBytes - downloadedButNotConsumed;
            }
            while (downloadedButNotConsumed >= downloadBudgetBytes) {
                LOG.info("Waiting until indexer catches up. Downloader position: {}, AOT data downloader: {}",
                        dataPendingStartPosition, IOUtils.humanReadableByteCount(downloadedButNotConsumed)
                );
                condition.await();
                downloadedButNotConsumed = prefetchedWindowSizeInBytes();
            }
            return downloadBudgetBytes - downloadedButNotConsumed;
        } finally {
            lock.unlock();
        }
    }

    void advanceIndexer(long indexerPosition) {
        try {
            LOG.info("advanceIndexer. position: {}. {}", indexerPosition, prefetchWindow());
            boolean advanced = false;
            lock.lock();
            while (true) {
                DownloadedBlock head = aotDownloadedBlocks.peekFirst();
                if (head != null && head.lastIndex < indexerPosition) {
                    advanced = true;
                    LOG.info("Discarding AOT block: {}", head);
                    dataInDownloadedBlocks -= head.dataInBlock;
                    aotDownloadedBlocks.pollFirst();
                } else {
                    break;
                }
            }
            if (advanced) {
                if (prefetchedWindowSizeInBytes() < 0) {
                    throw new IllegalStateException("AOT downloaded is negative. aotDownloaded: " + prefetchedWindowSizeInBytes());
                }
                LOG.info("Signaling");
                condition.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    private PrefetchWindow prefetchWindow() {
        try {
            lock.lock();
            var start = aotDownloadedBlocks.isEmpty() ? dataPendingStartPosition : aotDownloadedBlocks.peekFirst().firstIndex;
            var end = lastPosition;
            var sizeInBytes = dataInDownloadedBlocks + dataPending;
            return new PrefetchWindow(start, end, sizeInBytes);
        } finally {
            lock.unlock();
        }
    }

    private long prefetchedWindowSizeInBytes() {
        try {
            lock.lock();
            return dataInDownloadedBlocks + dataPending;
        } finally {
            lock.unlock();
        }
    }
}