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
import org.apache.jackrabbit.oak.index.indexer.document.tree.TreeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BlobPrefetcher {

    private static final Logger LOG = LoggerFactory.getLogger(BlobPrefetcher.class);

    private static final String BLOB_PREFETCH_PROPERTY_NAME = "oak.index.BlobPrefetchSuffix";
    private static final String BLOB_PREFETCH_SUFFIX_DEFAULT = "";
    private static final int PRETCH_THREADS = 16;

    public static void prefetch(TreeStore treeStore) {
        String prefix = System.getProperty(
                BLOB_PREFETCH_PROPERTY_NAME,
                BLOB_PREFETCH_SUFFIX_DEFAULT);
        LOG.info("Prefetch prefix '{}'", prefix);
        if (prefix.isEmpty()) {
            return;
        }
        Iterator<String> it = treeStore.pathIterator();
        Semaphore semaphore = new Semaphore(PRETCH_THREADS);
        ExecutorService executorService = Executors.newFixedThreadPool(1 + PRETCH_THREADS, new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("BlobPrefetcher-" + threadNumber.getAndIncrement());
                return t;
            }
        });
        AtomicLong prefetched = new AtomicLong();
        executorService.submit(() -> {
            int count = 0;
            try {
                while (it.hasNext()) {
                    String e = it.next();
                    if (++count % 200_000 == 0) {
                        LOG.info("Count {} path {}", count, e);
                    }
                    if (e.endsWith(BLOB_PREFETCH_SUFFIX_DEFAULT)) {
                        NodeStateEntry nse = treeStore.getNodeStateEntry(e);
                        PropertyState p = nse.getNodeState().getProperty("jcr:data");
                        if (p == null || p.isArray() || p.getType() != Type.BINARY) {
                            continue;
                        }
                        Blob blob = p.getValue(Type.BINARY);
                        semaphore.acquire();
                        executorService.submit(() -> {
                            try {
                                InputStream in = blob.getNewStream();
                                // read one byte only, in order to prefetch
                                in.read();
                                in.close();
                            } catch (IOException e1) {
                                LOG.warn("Prefetching failed", e);
                            }
                            semaphore.release();
                            prefetched.incrementAndGet();
                        });
                    }
                }
            } catch (Exception e) {
                LOG.warn("Prefetch error", e);
            } finally {
                LOG.info("Completed after {} nodes, {} prefetched", count, prefetched.get());
            }
        });
    }

}
