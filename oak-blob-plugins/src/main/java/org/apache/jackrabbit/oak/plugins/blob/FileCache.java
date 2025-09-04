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
package org.apache.jackrabbit.oak.plugins.blob;

import static org.apache.commons.io.FilenameUtils.normalizeNoEndSeparator;
import static org.apache.jackrabbit.oak.commons.FileIOUtils.copyInputStreamToFile;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.guava.common.cache.Cache;
import org.apache.jackrabbit.guava.common.cache.CacheLoader;
import org.apache.jackrabbit.guava.common.cache.RemovalCause;
import org.apache.jackrabbit.guava.common.cache.Weigher;
import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.commons.io.FileTreeTraverser;
import org.apache.jackrabbit.oak.commons.time.Stopwatch;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.jackrabbit.guava.common.cache.AbstractCache;

/**
 */
public class FileCache extends AbstractCache<String, File> implements Closeable {
    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(FileCache.class);

    private static final int SEGMENT_COUNT = Integer.getInteger("oak.blob.fileCache.segmentCount", 1);

    // the maximum number of entries (default: 0.5 million)
    private static final int MAX_ENTRY_COUNT = Integer.getInteger("oak.blob.fileCache.maxEntryCount", 500_000);

    protected static final String DOWNLOAD_DIR = "download";

    private static final long ONE_SECOND_IN_MILLIS = 1000;

    private static final AtomicLong lastLogMessage = new AtomicLong();

    /**
     * Parent of the cache root directory
     */
    private File parent;

    /**
     * The cacheRoot directory of the cache.
     */
    private File cacheRoot;

    private CacheLIRS<String, String> cache;

    private FileCacheStats cacheStats;

    private ExecutorService executor;

    private CacheLoader<String, String> cacheLoader;

    private Weigher<String, String> weigher;
    private Weigher<String, String> memWeigher;

    // the maximum number of entries (files)
    private long maxEntryCount = MAX_ENTRY_COUNT;

    // the maximum number of blocks, as configured via maxSize
    private final long maxBlocks;

    // the current block limit. by default, this is maxBlocks,
    // unless if there are too many entries, in which cache
    // the limit is adjusted
    private long currentBlockLimit;
    private long highWaterMark;
    private long loggedWaterMark;

    private FileCache(long maxSize /* bytes */, File root,
        final CacheLoader<String, InputStream> loader, @Nullable final ExecutorService executor) {

        this.parent = root;
        this.cacheRoot = new File(root, DOWNLOAD_DIR);

        // convert to number of 4 KB blocks
        maxBlocks = Math.round(maxSize / (1024L * 4));
        currentBlockLimit = maxBlocks;

        /**
         * Convert the size calculation to KB to support max file size of 2 TB
         */
        weigher = (key, value) -> {
            long value2 = getFile(key).length();
            // convert to number of 4 KB blocks, plus an overhead of 1 blocks per file
            return Math.round(value2 / (4 * 1024)) + 0;
        };

        //Rough estimate of the in-memory key, value pair
        memWeigher = (key, value) -> (StringUtils.estimateMemoryUsage(key) + 128);

        cacheLoader = new CacheLoader<>() {
            @Override
            public String load(String key) throws Exception {
                // Fetch from local cache directory and if not found load from backend
                File cachedFile = getFile(key);
                if (cachedFile.exists()) {
                    return key;
                } else {
                    long startNanos = System.nanoTime();
                    try (InputStream is = loader.load(key))  {
                        copyInputStreamToFile(is, cachedFile);
                    } catch (Exception e) {
                        LOG.warn("Error reading object for id [{}] from backend", key, e);
                        throw e;
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Loaded file: {} in {}", key, (System.nanoTime() - startNanos) / 1_000_000);
                    }
                    return key;
                }
            }
        };

        cache = new CacheLIRS.Builder<String, String>()
            .maximumWeight(maxBlocks)
            .recordStats()
            .weigher(weigher)
            .segmentCount(SEGMENT_COUNT)
            .evictionCallback((key, value, cause) -> {
                try {
                    if (value != null && getFile(key).exists()
                        && cause != RemovalCause.REPLACED) {
                        long last = lastLogMessage.get();
                        DataStoreCacheUtils.recursiveDelete(getFile(key), cacheRoot);
                        long now = System.currentTimeMillis();
                        if (now - last >= ONE_SECOND_IN_MILLIS) {
                            if (lastLogMessage.compareAndSet(last, now)) {
                                String reason = cause.toString();
                                if ("SIZE".equals(reason) && currentBlockLimit != maxBlocks) {
                                    reason = "ENTRY_COUNT > " + maxEntryCount;
                                }
                                LOG.info("File [{}] evicted with reason [{}]", getFile(key), reason);
                            }
                        }
                    }
                } catch (IOException e) {
                    LOG.info("Cached file deletion failed after eviction", e);
                }
            })
            .build();

        this.cacheStats =
            new FileCacheStats(cache, weigher, memWeigher, maxSize);

        //  TODO: Check persisting the in-memory map and initializing Vs building from fs
        // Build in-memory cache asynchronously from the file system entries
        if (executor == null) {
            this.executor = Executors.newSingleThreadExecutor();
        } else {
            this.executor = executor;
        }
        this.executor.submit(new CacheBuildJob());
    }

    /**
     * Set the maximum number of files.
     */
    public void setMaxEntryCount(long maxEntryCount) {
        this.maxEntryCount = maxEntryCount;
    }

    /**
     * Get the current entry count (number of files).
     */
    public long getEntryCount() {
        return cache.size();
    }

    private FileCache() {
        maxBlocks = 0;
    }

    private File getFile(String key) {
        return DataStoreCacheUtils.getFile(key, cacheRoot);
    }

    public static FileCache build(long maxSize /* bytes */, File root,
        final CacheLoader<String, InputStream> loader, @Nullable final ExecutorService executor) {
        if (maxSize > 0) {
            return new FileCache(maxSize, root, loader, executor);
        }
        return new FileCache() {

            private final Cache<?, ?> cache = new CacheLIRS<>(0);

            @Override public void put(String key, File file) {
            }

            @Override public boolean containsKey(String key) {
                return false;
            }

            @Nullable @Override public File getIfPresent(String key) {
                return null;
            }

            @Override public File get(String key) {
                return null;
            }

            @Override public void invalidate(Object key) {
            }

            @Override public DataStoreCacheStatsMBean getStats() {
                return new FileCacheStats(cache, (key, value) -> 1, (key, value) -> 1, 0);
            }

            @Override public void close() {
            }
        };
    }

    /**
     * Puts the given key and file into the cache.
     * The file is moved to the cache. So, the original file
     * won't be available after this operation. It can be retrieved
     * using {@link #getIfPresent(String)}.
     *
     * @param key of the file
     * @param file to put into cache
     */
    @Override
    public void put(String key, File file) {
        adjustSize();
        put(key, file, true);
    }

    private void put(String key, File file, boolean copy) {
        try {
            File cached = DataStoreCacheUtils.getFile(key, cacheRoot);
            if (!cached.exists()) {
                if (copy) {
                    FileUtils.copyFile(file, cached);
                } else {
                    FileUtils.moveFile(file, cached);
                }
            }
            cache.put(key, key);
        } catch (IOException e) {
            LOG.error("Exception adding id [{}] with file [{}] to cache, root cause: {}", key, file, e.getMessage());
            LOG.debug("Root cause", e);
        }
    }

    public boolean containsKey(String key) {
        return cache.containsKey(key);
    }

    /**
     * Retrieves the file handle from the cache if present and null otherwise.
     *
     * @param key of the file to retrieve
     * @return File handle if available
     */
    @Nullable
    public File getIfPresent(String key) {
        try {
            String value = cache.getIfPresent(key);
            return value == null ? null : getFile(key);
        } catch (Exception e) {
            LOG.error("Error in retrieving [{}] from cache", key, e);
        }
        return null;
    }

    @Nullable
    @Override
    public File getIfPresent(Object key) {
        return getIfPresent((String) key);
    }

    public File get(String key) throws IOException {
        adjustSize();
        try {
            // get from cache and download if not available
            cache.get(key, () -> cacheLoader.load(key));
            return getFile(key);
        } catch (ExecutionException e) {
            LOG.error("Error loading [{}] from cache", key);
            throw new IOException(e);
        }
    }

    private void adjustSize() {
        long currentSize = cache.size();
        if (currentSize > highWaterMark) {
            highWaterMark = currentSize;
            // low for each additional 50'000 entries
            while (highWaterMark > loggedWaterMark + 50_000) {
                loggedWaterMark += 50_000;
                LOG.info("New high water mark: {} entries", loggedWaterMark);
            }
        }
        if (currentSize < maxEntryCount * 0.9 && currentBlockLimit  == maxBlocks) {
            // normal case:
            // less than 90% of the max number of entries,
            // and the limit is unchanged
            return;
        }
        if (currentSize < maxEntryCount) {
            if (currentSize >= maxEntryCount * 0.9) {
                // more than 90% full: keep current limit
                return;
            }
            // possibly increase the limit, to allow for more files
            if (currentBlockLimit < maxBlocks) {
                // not yet at the maximum:
                // grow the maximum size, 10 blocks at the time,
                // starting at the current size
                currentBlockLimit = Math.max(
                        currentBlockLimit + 10,
                        cache.getUsedMemory() + 10);
                // never grow larger than the configured size
                currentBlockLimit = Math.min(currentBlockLimit, maxBlocks);
                LOG.debug("Grow the cache size to {}", currentBlockLimit);
                cache.setMaxMemory(currentBlockLimit);
            }
            return;
        }
        // shrink the cache, 2 percent at the time, starting at the current size
        currentBlockLimit = Math.min(
                (int) (currentBlockLimit * 0.98 - 1),
                (int) (cache.getUsedMemory() * 0.98 - 1));
        // never grow larger than the configured size
        currentBlockLimit = Math.min(currentBlockLimit, maxBlocks);
        LOG.info("Shrinking the file cache size to {} because there are {} files (limit: {})",
                currentBlockLimit, cache.size(), maxEntryCount);
        cache.setMaxMemory(currentBlockLimit);
    }

    @Override
    public void invalidate(Object key) {
        cache.invalidate(key);
    }

    public DataStoreCacheStatsMBean getStats() {
        return cacheStats;
    }

    @Override
    public void close() {
        LOG.info("Cache stats on close [{}]", cacheStats.cacheInfoAsString());
        new ExecutorCloser(executor).close();
    }

    /**
     * Called to initialize the in-memory cache from the fs folder
     */
    private class CacheBuildJob implements Callable<Integer> {
        @Override
        public Integer call() {
            Stopwatch watch = Stopwatch.createStarted();
            int count = build();
            LOG.info("Cache built with [{}] files from file system in [{}] seconds",
                count, watch.elapsed(TimeUnit.SECONDS));
            return count;
        }
    }

    /**
     * Retrieves all the files present in the fs cache folder and builds the in-memory cache.
     */
    private int build() {
        // Move older generation cache downloaded files to the new folder
        DataStoreCacheUpgradeUtils.moveDownloadCache(parent);

        // Iterate over all files in the cache folder
        long count = FileTreeTraverser.depthFirstPostOrder(cacheRoot)
                .filter(file -> file.isFile() &&
                        !normalizeNoEndSeparator(file.getParent()).equals(cacheRoot.getAbsolutePath())
                )
                .flatMap(toBeSyncedFile -> {
                    try {
                        put(toBeSyncedFile.getName(), toBeSyncedFile, false);
                        LOG.trace("Added file [{}} to in-memory cache", toBeSyncedFile);
                        return Stream.of(toBeSyncedFile);
                    } catch (Exception e) {
                        LOG.error("Error in putting cached file in map[{}]", toBeSyncedFile);
                        return Stream.empty();
                    }
                })
                .count();
        LOG.trace("[{}] files put in im-memory cache", count);
        return (int) count;
    }
}

class FileCacheStats extends CacheStats implements DataStoreCacheStatsMBean {
    private static final long BLOCK_SIZE = 4 * 1024;
    private final Weigher<Object, Object> memWeigher;
    private final Weigher<Object, Object> weigher;
    private final Cache<Object, Object> cache;

    /**
     * Construct the cache stats object.
     *  @param cache     the cache
     * @param weigher   the weigher used to estimate the current weight
     * @param maxWeight the maximum weight
     */
    public FileCacheStats(Cache<?, ?> cache, Weigher<?, ?> weigher, Weigher<?, ?> memWeigher,
        long maxWeight) {
        super(cache, "DataStore-DownloadCache", weigher, maxWeight);
        this.memWeigher = (Weigher<Object, Object>) memWeigher;
        this.weigher = (Weigher<Object, Object>) weigher;
        this.cache = (Cache<Object, Object>) cache;
    }

    @Override
    public long estimateCurrentMemoryWeight() {
        if (memWeigher == null) {
            return -1;
        }
        long size = 0;
        for (Map.Entry<?, ?> e : cache.asMap().entrySet()) {
            Object k = e.getKey();
            Object v = e.getValue();
            size += memWeigher.weigh(k, v);
        }
        return size;
    }

    @Override
    public long estimateCurrentWeight() {
        if (weigher == null) {
            return -1;
        }
        long size = 0;
        for (Map.Entry<?, ?> e : cache.asMap().entrySet()) {
            Object k = e.getKey();
            Object v = e.getValue();
            size += weigher.weigh(k, v) * BLOCK_SIZE;
        }
        return size;
    }

}
