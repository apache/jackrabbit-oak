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
 */
public class AheadOfTimeBlobDownloaderThrottler {
    private final static Logger LOG = LoggerFactory.getLogger(AheadOfTimeBlobDownloaderThrottler.class);

    // A segment of data downloaded
    private static class DownloadedBlob {
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

    private final int maximumSize;
    private final long maximumSizeBytes;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    // The window is stored as a list of full segments plus the data that was not yet added to a segment. That is, we
    // first accumulate data until reaching targetSegmentSizeBytes, before creating a segment.
    private final ArrayDeque<DownloadedBlob> aotDownloadedBlobs;
    // How much data is in the downloaded segments. This is just an optimization to avoid traversing the list of segments
    // every time we need the total window size.
    private long aotDownloadedBytes = 0;
    // Top of the prefetch window
    private long windowLastPosition = -1;
    // Position of the indexer
    private long indexerPosition = -1;

    private int maxAotDownloadedBlobsSize = 0;
    private long maxAotDownloadedBlobsSizeBytes = 0;

    /**
     * @param maximumSizeBytes How many bytes can be downloaded ahead of the indexer.
     * @param maximumSize      How many blobs can be downloaded ahead of the indexer.
     */
    public AheadOfTimeBlobDownloaderThrottler(int maximumSize, long maximumSizeBytes) {
        if (maximumSize <= 0) {
            throw new IllegalArgumentException("maximumSize must be positive");
        }
        if (maximumSizeBytes <= 0) {
            throw new IllegalArgumentException("maximumSizeBytes must be positive");
        }
        this.maximumSize = maximumSize;
        this.maximumSizeBytes = maximumSizeBytes;
        this.aotDownloadedBlobs = new ArrayDeque<>(maximumSize);
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
        if (length > maximumSizeBytes) {
            LOG.warn("Blob length {} is higher than the maximum size of the window {}. Proceeding with a reservation for the maximumSize of the throttler.", length, maximumSizeBytes);
            length = maximumSizeBytes;
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

            while (aotDownloadedBytes + length > maximumSizeBytes || aotDownloadedBlobs.size() >= maximumSize) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Waiting until indexer catches up. Downloader position: {}, AOT data downloader: {}, number of aot downloaded blobs: {}",
                            firstPosition(), IOUtils.humanReadableByteCount(aotDownloadedBytes), aotDownloadedBlobs.size()
                    );
                }
                condition.await();
            }
            windowLastPosition = position;
            aotDownloadedBlobs.addLast(new DownloadedBlob(position, length));
            aotDownloadedBytes += length;
            // Update maximum values
            if (aotDownloadedBlobs.size() > maxAotDownloadedBlobsSize) {
                maxAotDownloadedBlobsSize = aotDownloadedBlobs.size();
            }
            if (aotDownloadedBytes > maxAotDownloadedBlobsSizeBytes) {
                maxAotDownloadedBlobsSizeBytes = aotDownloadedBytes;
            }
            return true;
        } finally {
            lock.unlock();
        }
    }

    public long getAvailableWindowSize() {
        lock.lock();
        try {
            return maximumSize - aotDownloadedBlobs.size();
        } finally {
            lock.unlock();
        }
    }

    public long getAvailableWindowBytes() {
        lock.lock();
        try {
            return maximumSizeBytes - aotDownloadedBytes;
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
        lock.lock();
        try {
            int oldWindowSize = aotDownloadedBlobs.size();
            long oldWindowBytes = aotDownloadedBytes;
            while (true) {
                DownloadedBlob head = aotDownloadedBlobs.peekFirst();
                if (head != null && head.position <= indexerPosition) {
                    aotDownloadedBlobs.pollFirst();
                    aotDownloadedBytes -= head.size;
                } else {
                    break;
                }
            }
            if (oldWindowSize != aotDownloadedBlobs.size()) {
                LOG.debug("Window size reduced. Indexer position: {}. windowSize: {} -> {}, windowSizeBytes: {} -> {}",
                        indexerPosition, oldWindowSize, aotDownloadedBlobs.size(),
                        IOUtils.humanReadableByteCountBin(oldWindowBytes), IOUtils.humanReadableByteCountBin(aotDownloadedBytes));
                if (aotDownloadedBytes < 0) {
                    throw new IllegalStateException("AOT downloaded bytes is negative. aotDownloaded: " + aotDownloadedBytes);
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
            return String.format("AOT Downloader throttler: {aotDownloadedBlobsSize: %s, aotDownloadedBlobsSizeBytes: %s, maxAotDownloadedBlobsSize: %s, maxAotDownloadedBlobsSizeBytes: %s}",
                    aotDownloadedBlobs.size(), IOUtils.humanReadableByteCount(aotDownloadedBytes), maxAotDownloadedBlobsSize, IOUtils.humanReadableByteCount(maxAotDownloadedBlobsSizeBytes));
        } finally {
            lock.unlock();
        }
    }
}