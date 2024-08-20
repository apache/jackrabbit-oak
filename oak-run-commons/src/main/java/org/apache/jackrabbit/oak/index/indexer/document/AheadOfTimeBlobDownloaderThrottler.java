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


import org.apache.jackrabbit.oak.commons.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Tracks a prefetch window for the AOT downloader. This class keeps a window from {startPosition, endPosition} together
 * with an estimation of the data that was downloaded inside this window. It exposes two operations:
 *
 * <ul>
 * <li> {@link #reserveSpaceForBlob(long, long)} - Reserves space to download blob at the given position and size,
 *      blocking until enough space is available.
 * <li> {@link #advanceIndexer(long)} - Advances the window until the given position, removing from the window any
 *      positions lower or equal than the new index.
 * </ul>
 * <p>
 * Because keeping an exact count of the size of the window would require too much memory (would require remembering
 * the position and size of all blobs inside the window), this class instead stores the sum of sizes for the bobs in
 * several ranges of positions. That is, keeps a non-overlapping list of segments <code>[startPosition, endPosition, sumSize]</code>,
 * A segment is discarded only when the indexer advances past is end position. In practice, this will delay slightly the
 * moment when the window size is reduced. If the window is full and there is a caller trying to reserve space, it may
 * take a little longer for it to be unblocked.
 */
public class AheadOfTimeBlobDownloaderThrottler {
    private final static Logger LOG = LoggerFactory.getLogger(AheadOfTimeBlobDownloaderThrottler.class);
    private final static int NUMBER_OF_BUCKETS = 16;

    // A window of prefetched data
    private static class PrefetchWindow {
        final long first;
        final long last;
        final long sizeInBytes;

        public PrefetchWindow(long first, long last, long sizeInBytes) {
            this.first = first;
            this.last = last;
            this.sizeInBytes = sizeInBytes;
        }

        @Override
        public String toString() {
            return "PrefetchWindow{" +
                    "first=" + first +
                    ", last=" + last +
                    ", sizeInBytes=" + sizeInBytes +
                    '}';
        }
    }

    // A segment of data downloaded
    private static class DownloadedSegment {
        final long first;
        final long last;
        private final long dataInBlock;

        public DownloadedSegment(long first, long last, long dataInBlock) {
            this.first = first;
            this.last = last;
            this.dataInBlock = dataInBlock;
        }

        @Override
        public String toString() {
            return "DownloadedSegment{" +
                    "first=" + first +
                    ", last=" + last +
                    ", dataInBlock=" + dataInBlock +
                    '}';
        }
    }

    private final long downloadWindowSize;
    private final long targetSegmentSizeBytes;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    // The window is stored as a list of full segments plus the data that was not yet added to a segment. That is, we
    // first accumulate data until reaching targetSegmentSizeBytes, before creating a segment.
    private final ArrayDeque<DownloadedSegment> downloadedSegments = new ArrayDeque<>();
    // How much data is in the downloaded segments. This is just an optimization to avoid traversing the list of segments
    // every time we need the total window size.
    private long dataInListOfSegments = 0;
    // The next variables track the data that was not yet added to a segment.
    private long dataPendingStartPosition = 0;
    private long dataPendingBytes = 0;
    // Top of the prefetch window
    private long windowLastPosition = 0;

    /**
     * @param downloadWindowSize How many bytes can be downloaded ahead of the indexer.
     */
    public AheadOfTimeBlobDownloaderThrottler(long downloadWindowSize) {
        this.downloadWindowSize = downloadWindowSize;
        this.targetSegmentSizeBytes = downloadWindowSize / NUMBER_OF_BUCKETS;
    }

    /**
     * Reserves space for a blob to be downloaded. This method blocks until there is enough space in the prefetch window.
     *
     * @param blobPosition The position of the blob to be downloaded.
     * @param length       The length of the blob to be downloaded.
     * @return
     * @throws InterruptedException
     */
    public long reserveSpaceForBlob(long blobPosition, long length) throws InterruptedException {
        if (blobPosition <= windowLastPosition) {
            throw new IllegalArgumentException("blobPosition " + blobPosition + " is not higher than the current last position of the window " + windowLastPosition);
        }
        this.windowLastPosition = blobPosition;
        LOG.debug("Reserving space for blob. Position: {}, length: {}", blobPosition, length);
        try {
            lock.lock();
            dataPendingBytes += length;
            if (dataPendingBytes >= this.targetSegmentSizeBytes) {
                var newSegment = new DownloadedSegment(dataPendingStartPosition, blobPosition, dataPendingBytes);
                downloadedSegments.addLast(newSegment);
                dataPendingStartPosition = blobPosition + 1;
                dataPendingBytes = 0;
                dataInListOfSegments += newSegment.dataInBlock;
                LOG.debug("Created segment: {}. Prefetch window: {}", newSegment, prefetchedWindowSizeInBytes());
            }
            // Check if we have space for more data
            long downloadedButNotConsumed = prefetchedWindowSizeInBytes();
            while (downloadedButNotConsumed >= downloadWindowSize) {
                LOG.info("Waiting until indexer catches up. Downloader position: {}, AOT data downloader: {}",
                        dataPendingStartPosition, IOUtils.humanReadableByteCount(downloadedButNotConsumed)
                );
                condition.await();
                downloadedButNotConsumed = prefetchedWindowSizeInBytes();
            }
            return downloadWindowSize - downloadedButNotConsumed;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Advances the indexer to the given position. This method will remove from the prefetch window any segments that
     * are before the given position.
     *
     * @param indexerPosition The new position of the indexer.
     */
    public void advanceIndexer(long indexerPosition) {
        try {
//            LOG.info("advanceIndexer. position: {}. {}", indexerPosition, prefetchWindow());
            boolean windowSizeReduced = false;
            lock.lock();
            while (true) {
                DownloadedSegment head = downloadedSegments.peekFirst();
                if (head != null && head.last < indexerPosition) {
                    windowSizeReduced = true;
                    LOG.info("Discarding AOT segment: {}, {}", head, prefetchWindow());
                    downloadedSegments.pollFirst();
                    dataInListOfSegments -= head.dataInBlock;
                } else {
                    break;
                }
            }
            if (windowSizeReduced) {
                if (prefetchedWindowSizeInBytes() < 0) {
                    throw new IllegalStateException("AOT downloaded is negative. aotDownloaded: " + prefetchedWindowSizeInBytes());
                }
                condition.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    private PrefetchWindow prefetchWindow() {
        try {
            lock.lock();
            var start = downloadedSegments.isEmpty() ? dataPendingStartPosition : downloadedSegments.peekFirst().first;
            var end = windowLastPosition;
            var sizeInBytes = dataInListOfSegments + dataPendingBytes;
            return new PrefetchWindow(start, end, sizeInBytes);
        } finally {
            lock.unlock();
        }
    }

    private long prefetchedWindowSizeInBytes() {
        try {
            lock.lock();
            return dataInListOfSegments + dataPendingBytes;
        } finally {
            lock.unlock();
        }
    }
}