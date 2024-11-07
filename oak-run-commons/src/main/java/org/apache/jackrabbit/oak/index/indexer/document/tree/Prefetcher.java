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
package org.apache.jackrabbit.oak.index.indexer.document.tree;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.utils.Hash;
import org.apache.jackrabbit.oak.index.indexer.document.flatfile.analysis.utils.HyperLogLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The prefetcher, in a separate threads, reads ahead of indexing, such that the
 * nodestore cache and datastore cache is filled.
 *
 * There are 3 threads (in addition to the threads that read from the datastore):
 * - TRACK_INDEXING - tries to be at the same position as the indexing
 * - DATASTORE_PREFETCH - reads ahead about 1 GB, such that the datastore cache is filled
 * - NODESTORE_CACHE_FILLER - reads ahead about 32'000 entries, such that the node store cache is filled
 */
public class Prefetcher {

    private static final Logger LOG = LoggerFactory.getLogger(Prefetcher.class);

    private static final int PRETCH_THREADS = 16;

    private final TreeStore prefetchStore;
    private final TreeStore indexStore;
    private final ExecutorService executorService;
    private final AtomicLong downloadMax = new AtomicLong();
    private final AtomicLong iterateCount = new AtomicLong();
    private final Semaphore semaphore = new Semaphore(PRETCH_THREADS);
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();

    private String blobSuffix;

    private volatile long blobReadAheadSize = 4 * 1024 * 1024 * 1024L;
    private volatile long nodeReadAheadCount = 64 * 1024;
    private volatile long maxBlobSize;

    public Prefetcher(TreeStore prefetchStore, TreeStore indexStore) {
        this.prefetchStore = prefetchStore;
        this.indexStore = indexStore;
        this.executorService = Executors.newFixedThreadPool(3 + PRETCH_THREADS, new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setName("BlobPrefetcher-" + threadNumber.getAndIncrement());
                return t;
            }
        });
    }

    public void setBlobSuffix(String blobSuffix) {
        this.blobSuffix = blobSuffix;
    }

    public void setBlobReadAheadSize(long blobReadAheadSize) {
        this.blobReadAheadSize = blobReadAheadSize;
    }

    public void setNodeReadAheadCount(long nodeReadAheadCount) {
        this.nodeReadAheadCount = nodeReadAheadCount;
    }

    public boolean shutdown() {
        closed.set(true);
        executorService.shutdown();
        try {
            return executorService.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.info("Prefetcher termination was interrupted: {}", e.toString());
            return false;
        }
    }

    public void start() {
        if (started.getAndSet(true)) {
            // already running
            return;
        }
        LOG.info("Prefetch suffix '{}', prefetch {}, index {}",
                blobSuffix, prefetchStore, indexStore);
        executorService.submit(
                iterator(PrefetchType.TRACK_INDEXING));
        executorService.submit(
                iterator(PrefetchType.NODESTORE_CACHE_FILLER));
        if (!blobSuffix.isEmpty()) {
            executorService.submit(
                    iterator(PrefetchType.BLOB_PREFETCH));
        }
    }

    public void sleep(String status) throws InterruptedException {
        Thread.sleep(10);
    }

    Runnable iterator(PrefetchType prefetchType) {
        return () -> {
            Iterator<String> it = prefetchStore.iteratorOverPaths();
            HyperLogLog estimatedUniqueBlobCount = new HyperLogLog(1024, 0);
            AtomicLong prefetched = new AtomicLong();
            long count = 0;
            try {
                long totalBlobCount = 0;
                long inlinedBlobCount = 0;
                long totalBlobSize = 0;
                while (it.hasNext()) {
                    if (closed.get()) {
                        break;
                    }
                    String path = it.next();
                    if (++count % 1_000_000 == 0) {
                        int available = semaphore.availablePermits();
                        LOG.info("Iterated {} type {} inlinedCount {} totalCount {} " +
                                "totalSize {} maxSize {} max {} availableThreads {} " +
                                "indexing {} prefetch {} path {}",
                                count, prefetchType, inlinedBlobCount, totalBlobCount,
                                totalBlobSize, maxBlobSize, downloadMax.get(), available,
                                indexStore.toString(), prefetchStore.toString(), path);
                    }
                    if (prefetchType == PrefetchType.TRACK_INDEXING) {
                        iterateCount.set(count);
                        while (true) {
                            String indexingPath = indexStore.getHighestReadKey();
                            if (indexingPath.compareTo(path) >= 0) {
                                break;
                            }
                            sleep("wait for indexing to progress");
                        }
                    }
                    if (prefetchType == PrefetchType.NODESTORE_CACHE_FILLER) {
                        while (count - nodeReadAheadCount > iterateCount.get()) {
                            sleep("wait in node cache fillter");
                        }
                        // this will fill the page cache of the index store
                        String value = indexStore.getSession().get(path);
                        // this will not cause a cache miss
                        TreeStoreNodeState entry = indexStore.buildNodeState(path, value);
                        indexStore.prefillCache(path, entry);
                        continue;
                    }
                    if (!path.endsWith(blobSuffix)) {
                        continue;
                    }
                    NodeStateEntry nse = prefetchStore.getNodeStateEntry(path);
                    PropertyState p = nse.getNodeState().getProperty("jcr:data");
                    if (p == null || p.isArray() || p.getType() != Type.BINARY) {
                        continue;
                    }
                    Blob blob = p.getValue(Type.BINARY);
                    if (blob.isInlined()) {
                        inlinedBlobCount++;
                        continue;
                    }
                    estimatedUniqueBlobCount.add(longHash(blob));
                    totalBlobCount++;
                    totalBlobSize += blob.length();
                    maxBlobSize = Math.max(maxBlobSize, blob.length());
                    if (prefetchType == PrefetchType.TRACK_INDEXING) {
                        downloadMax.set(totalBlobSize);
                        continue;
                    }
                    if (prefetchType != PrefetchType.BLOB_PREFETCH) {
                        throw new IllegalStateException("Incorrect type: " + prefetchType);
                    }
                    String indexingPos = indexStore.getHighestReadKey();
                    if (indexingPos.compareTo(path) >= 0) {
                        // we are behind indexing, so skip downloading,
                        // in order to catch up
                        // (this can happen eg. when indexing in parallel)
                        continue;
                    }
                    while (totalBlobSize - blobReadAheadSize > downloadMax.get()) {
                        sleep("wait in downloader");
                    }
                    semaphore.acquire();
                    executorService.submit(() -> {
                        try {
                            LOG.debug("Prefetching {} took {} ms", path);
                            InputStream in = blob.getNewStream();
                            // read one byte only, in order to prefetch
                            in.read();
                            in.close();
                        } catch (IOException e) {
                            LOG.warn("Prefetching failed", path, e);
                        }
                        semaphore.release();
                        prefetched.incrementAndGet();
                    });
                }
            } catch (Exception e) {
                LOG.warn("Prefetch error", e);
            } finally {
                LOG.info("Completed after {} nodes, {} prefetched, {} unique",
                        count, prefetched.get(),
                        estimatedUniqueBlobCount.estimate());
            }
        };
    }

    private static long longHash(Blob blob) {
        // the String.hashCode is only 32 bit
        // because of that, we mix it with the length
        // and then we use a secondary hash
        // otherwise the estimation is way off
        int h = blob.getContentIdentity().hashCode();
        return Hash.hash64(h | (blob.length() << 32));
    }

    static enum PrefetchType {
        TRACK_INDEXING,
        BLOB_PREFETCH,
        NODESTORE_CACHE_FILLER
    }

}
