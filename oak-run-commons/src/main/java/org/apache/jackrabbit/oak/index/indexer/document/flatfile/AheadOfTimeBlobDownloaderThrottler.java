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
 */
public class AheadOfTimeBlobDownloaderThrottler {
    private final static Logger LOG = LoggerFactory.getLogger(AheadOfTimeBlobDownloaderThrottler.class);

    private static class DownloadedBlob {
        // Position (line number) in the FFS
        final long position;
        final long size;

        public DownloadedBlob(long position, long size) {
            this.position = position;
            this.size = size;
        }

        @Override
        public String toString() {
            return "DownloadedBlob{" +
                    "position=" + position +
                    ", size=" + size +
                    '}';
        }
    }

    // Maximum number of blobs that can be downloaded ahead of time
    private final int maxWindowSizeNumberOfBlobs;
    // Maximum number of bytes that can be downloaded ahead of the indexer
    private final long maxWindowSizeBytes;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    // List of blobs downloaded ahead of the indexer
    private final ArrayDeque<DownloadedBlob> aotDownloadedBlobs;
    // How much data is currently downloaded ahead of time. This is just an optimization to avoid traversing the list of
    // downloaded blobs every time we need the total window size.
    private long currentWindowSizeBytes = 0;
    // Top of the prefetch window
    private long windowLastPosition = -1;
    // Position of the indexer
    private long indexerPosition = -1;

    // Statistics, the maximum size ever reached by the window
    private int highestWindowSizeNumberOfBlobs = 0;
    private long highestWindowSizeBytes = 0;

    /**
     * @param maxWindowSizeBytes How many bytes can be downloaded ahead of the indexer.
     * @param maxWindowSizeNumberOfBlobs      How many blobs can be downloaded ahead of the indexer.
     */
    public AheadOfTimeBlobDownloaderThrottler(int maxWindowSizeNumberOfBlobs, long maxWindowSizeBytes) {
        if (maxWindowSizeNumberOfBlobs <= 0) {
            throw new IllegalArgumentException("windowSizeNumberOfBlobs must be positive");
        }
        if (maxWindowSizeBytes <= 0) {
            throw new IllegalArgumentException("maximumSizeBytes must be positive");
        }
        this.maxWindowSizeNumberOfBlobs = maxWindowSizeNumberOfBlobs;
        this.maxWindowSizeBytes = maxWindowSizeBytes;
        this.aotDownloadedBlobs = new ArrayDeque<>(maxWindowSizeNumberOfBlobs);
    }

    /**
     * Reserves space for a blob to be downloaded. This method blocks until there is enough space in the prefetch window.
     * If the position of the reservation is lower or equal to the indexer position, the reservation is ignored.
     *
     * @param position The position of the blob to be downloaded.
     * @param length   The length of the blob to be downloaded.
     * @return true if space was reserved, false if the indexer is already ahead of the position of the blob.
     */
    public boolean reserveSpaceForBlob(long position, long length) throws InterruptedException {
        if (length > maxWindowSizeBytes) {
            LOG.warn("Blob length {} is higher than the maximum size of the window {}. Proceeding with a reservation for the maximumSize of the throttler.", length, maxWindowSizeBytes);
            length = maxWindowSizeBytes;
        }
        lock.lock();
        try {
            if (position <= windowLastPosition) {
                throw new IllegalArgumentException("blobPosition " + position + " is not higher than the current last position of the window " + windowLastPosition);
            }
            if (position <= indexerPosition) {
                LOG.warn("Blob position {} is lower than the indexer position {}. Ignoring space reservation request", position, indexerPosition);
                // Do not reserve space for this blob, it is already indexed
                return false;
            }

            while (currentWindowSizeBytes + length > maxWindowSizeBytes || aotDownloadedBlobs.size() >= maxWindowSizeNumberOfBlobs) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Waiting until indexer catches up. Downloader position: {}, AOT data downloader: {}, number of aot downloaded blobs: {}",
                            firstPosition(), IOUtils.humanReadableByteCount(currentWindowSizeBytes), aotDownloadedBlobs.size()
                    );
                }
                condition.await();
            }
            windowLastPosition = position;
            aotDownloadedBlobs.addLast(new DownloadedBlob(position, length));
            currentWindowSizeBytes += length;
            // Update maximum values
            if (aotDownloadedBlobs.size() > highestWindowSizeNumberOfBlobs) {
                highestWindowSizeNumberOfBlobs = aotDownloadedBlobs.size();
            }
            if (currentWindowSizeBytes > highestWindowSizeBytes) {
                highestWindowSizeBytes = currentWindowSizeBytes;
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    public long getAvailableWindowSize() {
        lock.lock();
        try {
            return maxWindowSizeNumberOfBlobs - aotDownloadedBlobs.size();
        } finally {
            lock.unlock();
        }
    }

    public long getAvailableWindowBytes() {
        lock.lock();
        try {
            return maxWindowSizeBytes - currentWindowSizeBytes;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Advances the indexer to the given position.
     *
     * @param indexerPosition The new position of the indexer.
     */
    public void advanceIndexer(long indexerPosition) {
        lock.lock();
        try {
            int oldWindowSize = aotDownloadedBlobs.size();
            long oldWindowBytes = currentWindowSizeBytes;
            while (true) {
                DownloadedBlob head = aotDownloadedBlobs.peekFirst();
                if (head != null && head.position <= indexerPosition) {
                    aotDownloadedBlobs.pollFirst();
                    currentWindowSizeBytes -= head.size;
                } else {
                    break;
                }
            }
            if (oldWindowSize != aotDownloadedBlobs.size()) {
                LOG.debug("Window size reduced. Indexer position: {}. windowSize: {} -> {}, windowSizeBytes: {} -> {}",
                        indexerPosition, oldWindowSize, aotDownloadedBlobs.size(),
                        IOUtils.humanReadableByteCountBin(oldWindowBytes), IOUtils.humanReadableByteCountBin(currentWindowSizeBytes));
                if (currentWindowSizeBytes < 0) {
                    throw new IllegalStateException("AOT downloaded bytes is negative. aotDownloaded: " + currentWindowSizeBytes);
                }
                condition.signalAll();
            }
            this.indexerPosition = indexerPosition;
        } finally {
            lock.unlock();
        }
    }

    private long firstPosition() {
        return aotDownloadedBlobs.isEmpty() ? -1 : aotDownloadedBlobs.getFirst().position;
    }

    public String formatStats() {
        lock.lock();
        try {
            return String.format("AOT Downloader throttler: {aotDownloadedBlobsSize: %s, aotDownloadedBlobsSizeBytes: %s, maxWindowSizeNumberOfBlobs: %s, maxWindowSizeBytes: %s}",
                    aotDownloadedBlobs.size(), IOUtils.humanReadableByteCount(currentWindowSizeBytes), highestWindowSizeNumberOfBlobs, IOUtils.humanReadableByteCount(highestWindowSizeBytes));
        } finally {
            lock.unlock();
        }
    }
}