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
package org.apache.jackrabbit.oak.index.indexer.document.tree.store;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.oak.index.indexer.document.tree.store.utils.Cache;

public class DiskCacheStore implements Store {

    private final Properties config;

    // the frontend (local disk)
    private Store frontend;

    // the backend (azure or s3)
    private Store backend;

    // set of entries that are not yet uploaded
    private ConcurrentHashMap<String, String> uploading = new ConcurrentHashMap<>();

    // executor service that uploads files
    private ExecutorService executor;

    // cache for entries that were downloaded
    // (doesn't contain entries that are not yet uploaded)
    private Cache<String, String> cache;

    private int readAhead;

    public String toString() {
        return "diskCache(" + frontend + ", " + backend + ")";
    }

    public DiskCacheStore(Properties config) {
        this.config = config;
        frontend = StoreBuilder.build(StoreBuilder.subProperties(config, "front."));

        // cache size, in number of files on disk
        int cacheSize = Integer.parseInt(config.getProperty("cacheSize", "1000000000"));

        // read ahead this number of children
        this.readAhead = Integer.parseInt(config.getProperty("readAhead", "1000000"));

        // using this number of threads at most
        int threads = Integer.parseInt(config.getProperty("threads", "20"));

        cache = new Cache<String, String>(cacheSize) {
            private static final long serialVersionUID = 1L;

            @Override
            public boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                boolean result = super.removeEldestEntry(eldest);
                if (result) {
                    frontend.remove(Collections.singleton(eldest.getKey()));
                }
                return result;
            }
        };
        executor = new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                r -> {
                    Thread thread = new Thread(r, "DiskCacheStore");
                    thread.setDaemon(true);
                    return thread;
                });
        backend = StoreBuilder.build(StoreBuilder.subProperties(config, "back."));
    }

    @Override
    public PageFile getIfExists(String key) {
        PageFile result = frontend.getIfExists(key);
        if (result != null) {
            return result;
        }
        if (backend.supportsByteOperations() && frontend.supportsByteOperations()) {
            byte[] data = backend.getBytes(key);
            frontend.putBytes(key, data);
            result = frontend.get(key);
        } else {
            result = backend.get(key);
            frontend.put(key, result);
        }
        synchronized (cache) {
            cache.put(key, key);
        }
        if (result.isInnerNode()) {
            for (int i = 0; i < readAhead && i < result.getValueCount(); i++) {
                String childKey = result.getChildValue(i);
                // System.out.println("    prefetching " + childKey + " because we read " + key);
                executor.submit(new Runnable() {

                    @Override
                    public void run() {
                        // this will put the entry in the cache
                        // and also read the children if needed
                        getIfExists(childKey);
                        // System.out.println("    prefetch done for " + childKey + " because we read " + key);
                    }

                });
            }
        }
        return result;
    }

    @Override
    public void put(String key, PageFile value) {
        if (uploading.containsKey(key)) {
            System.out.println("WARNING: upload is in progress: " + key +
                    " and a new upload is scheduled. waiting for the upload to finish, to avoid concurrency issues.");
            while (uploading.containsKey(key)) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
        uploading.put(key, key);
        frontend.put(key, value);
        executor.submit(new Runnable() {

            @Override
            public void run() {
                try {
                    if (frontend.supportsByteOperations() && backend.supportsByteOperations()) {
                        byte[] data = frontend.getBytes(key);
                        backend.putBytes(key, data);
                    } else {
                        backend.put(key, frontend.get(key));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                uploading.remove(key);
            }

        });
    }

    @Override
    public String newFileName() {
        return frontend.newFileName();
    }

    @Override
    public Set<String> keySet() {
        return backend.keySet();
    }

    @Override
    public void remove(Set<String> set) {
        synchronized (cache) {
            set.forEach(k -> cache.remove(k));
        }
        frontend.remove(set);
        backend.remove(set);
    }

    @Override
    public void removeAll() {
        frontend.removeAll();
        backend.removeAll();
    }

    @Override
    public long getWriteCount() {
        return frontend.getWriteCount();
    }

    @Override
    public long getReadCount() {
        return frontend.getReadCount();
    }

    @Override
    public void setWriteCompression(Compression compression) {
        frontend.setWriteCompression(compression);
        backend.setWriteCompression(compression);
    }

    @Override
    public void close() {
        try {
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        frontend.close();
        backend.close();
    }

    @Override
    public Properties getConfig() {
        return config;
    }

}
