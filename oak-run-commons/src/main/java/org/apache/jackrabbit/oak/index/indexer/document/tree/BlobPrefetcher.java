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

public class BlobPrefetcher {

    private static final Logger LOG = LoggerFactory.getLogger(BlobPrefetcher.class);

    private static final String BLOB_PREFETCH_PROPERTY_NAME = "oak.index.BlobPrefetchSuffix";
    private static final String BLOB_PREFETCH_SUFFIX_DEFAULT = "";
    private static final int PRETCH_THREADS = 16;
    private static final long DATASTORE_CACHE_SIZE = 16 * 1024 * 1024 * 1024L;

    public static void prefetch(TreeStore prefetchStore, TreeStore indexStore) {
        String prefix = System.getProperty(
                BLOB_PREFETCH_PROPERTY_NAME,
                BLOB_PREFETCH_SUFFIX_DEFAULT);
        LOG.info("Prefetch prefix '{}', prefetch {}, index {}",
                prefix, prefetchStore, indexStore);
        if (prefix.isEmpty()) {
            return;
        }
        Semaphore semaphore = new Semaphore(PRETCH_THREADS);
        ExecutorService executorService = Executors.newFixedThreadPool(2 + PRETCH_THREADS, new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("BlobPrefetcher-" + threadNumber.getAndIncrement());
                return t;
            }
        });
        AtomicLong downloadMax = new AtomicLong();
        executorService.submit(
                iterator(prefetchStore, indexStore,
                        null, null, false, downloadMax, prefix));
        executorService.submit(
                iterator(prefetchStore, indexStore,
                    executorService, semaphore, true, downloadMax, prefix));
    }

    static Runnable iterator(TreeStore prefetchStore,
            TreeStore indexStore,
            ExecutorService executorService,
            Semaphore semaphore, boolean download,
            AtomicLong downloadMax,
            String prefix) {
        return () -> {
            Iterator<String> it = prefetchStore.pathIterator();
            HyperLogLog estimateUniqueCount = new HyperLogLog(1024, 0);
            AtomicLong prefetched = new AtomicLong();
            int count = 0;
            try {
                long totalCount = 0;
                long inlinedCount = 0;
                long totalSize = 0;
                long maxSize = 0;
                while (it.hasNext()) {
                    String path = it.next();
                    if (++count % 1_000_000 == 0) {
                        int available = semaphore == null ? -1 : semaphore.availablePermits();
                        LOG.info("Iterated {} path {} inlinedCount {} totalCount {} " +
                                "totalSize {} maxSize {} indexing {} download {} max {} available {}",
                                count, path, inlinedCount, totalCount,
                                totalSize, maxSize, indexStore.toString(),
                                download, downloadMax.get(),
                                available
                                );
                    }
                    if (!download) {
                        while (true) {
                            String indexingPath = indexStore.getHighestReadKey();
                            if (indexingPath.compareTo(path) >= 0) {
                                break;
                            }
                            Thread.sleep(1000);
                        }
                    }
                    if (!path.endsWith(prefix)) {
                        continue;
                    }
                    NodeStateEntry nse = prefetchStore.getNodeStateEntry(path);
                    PropertyState p = nse.getNodeState().getProperty("jcr:data");
                    if (p == null || p.isArray() || p.getType() != Type.BINARY) {
                        continue;
                    }
                    Blob blob = p.getValue(Type.BINARY);
                    if (blob.isInlined()) {
                        inlinedCount++;
                        continue;
                    }
                    estimateUniqueCount.add(longHash(blob));
                    totalCount++;
                    totalSize += blob.length();
                    if (download) {
                        while (totalSize - DATASTORE_CACHE_SIZE > downloadMax.get()) {
                            Thread.sleep(1000);
                        }
                    } else {
                        downloadMax.set(totalSize);
                    }
                    maxSize = Math.max(maxSize, blob.length());
                    if (!download) {
                        continue;
                    }
                    String indexingPos = indexStore.getHighestReadKey();
                    if (indexingPos.compareTo(path) >= 0) {
                        // we are behind indexing!
                        // do not download, in order to catch up
                        LOG.debug("Indexing is ahead; ignoring {}", path);
                        continue;
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
                        estimateUniqueCount.estimate());
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

}
