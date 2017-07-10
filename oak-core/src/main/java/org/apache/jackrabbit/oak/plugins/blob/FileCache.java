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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.cache.AbstractCache;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.Weigher;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.cache.CacheLIRS;
import org.apache.jackrabbit.oak.cache.CacheLIRS.EvictionCallback;
import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.io.FileUtils.copyInputStreamToFile;
import static org.apache.commons.io.FilenameUtils.normalizeNoEndSeparator;

/**
 */
public class FileCache extends AbstractCache<String, File> implements Closeable {
    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(FileCache.class);

    protected static final String DOWNLOAD_DIR = "download";

    /**
     * Parent of the cache root directory
     */
    private File parent;

    /**
     * The cacheRoot directory of the cache.
     */
    private File cacheRoot;

    private CacheLIRS<String, File> cache;

    private FileCacheStats cacheStats;

    private ExecutorService executor;

    /**
     * Convert the size calculation to KB to support max file size of 2 TB
     */
    private static final Weigher<String, File> weigher = new Weigher<String, File>() {
        @Override public int weigh(String key, File value) {
            return Math.round(value.length() / (4 * 1024)); // convert to KB
        }};

    //Rough estimate of the in-memory key, value pair
    private static final Weigher<String, File> memWeigher = new Weigher<String, File>() {
        @Override public int weigh(String key, File value) {
            return (StringUtils.estimateMemoryUsage(key) +
                StringUtils.estimateMemoryUsage(value.getAbsolutePath()) + 48);
        }};

    private FileCache(long maxSize /* bytes */, File root,
        final CacheLoader<String, InputStream> loader, @Nullable final ExecutorService executor) {

        this.parent = root;
        this.cacheRoot = new File(root, DOWNLOAD_DIR);

        /* convert to 4 KB block */
        long size = Math.round(maxSize / (1024L * 4));

        cache = new CacheLIRS.Builder<String, File>()
            .maximumWeight(size)
            .recordStats()
            .weigher(weigher)
            .evictionCallback(new EvictionCallback<String, File>() {
                @Override
                public void evicted(@Nonnull String key, @Nullable File cachedFile,
                    @Nonnull RemovalCause cause) {
                    try {
                        if (cachedFile != null && cachedFile.exists()
                            && cause != RemovalCause.REPLACED) {
                            DataStoreCacheUtils.recursiveDelete(cachedFile, cacheRoot);
                            LOG.info("File [{}] evicted with reason [{}]", cachedFile, cause
                                .toString());
                        }
                    } catch (IOException e) {
                        LOG.info("Cached file deletion failed after eviction", e);
                    }
                }})
            .build(new CacheLoader<String, File>() {
                @Override
                public File load(String key) throws Exception {
                    // Fetch from local cache directory and if not found load from backend
                    File cachedFile = DataStoreCacheUtils.getFile(key, cacheRoot);
                    if (cachedFile.exists()) {
                        return cachedFile;
                    } else {
                        InputStream is = null;
                        boolean threw = true;
                        try {
                            is = loader.load(key);
                            copyInputStreamToFile(is, cachedFile);
                            threw = false;
                        } finally {
                            Closeables.close(is, threw);
                        }
                        return cachedFile;
                    }
                }
            });
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

    private FileCache() {
    }

    public static FileCache build(long maxSize /* bytes */, File root,
        final CacheLoader<String, InputStream> loader, @Nullable final ExecutorService executor) {
        if (maxSize > 0) {
            return new FileCache(maxSize, root, loader, executor);
        }
        return new FileCache() {
            @Override public void put(String key, File file) {
            }

            @Override public boolean containsKey(String key) {
                return false;
            }

            @Nullable @Override public File getIfPresent(String key) {
                return null;
            }

            @Override public File get(String key) throws IOException {
                return null;
            }

            @Override public void invalidate(Object key) {
            }

            @Override public DataStoreCacheStatsMBean getStats() {
                return new FileCacheStats(this, weigher, memWeigher, 0);
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
            cache.put(key, cached);
        } catch (IOException e) {
            LOG.error("Exception adding id [{}] with file [{}] to cache", key, file);
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
            return cache.getIfPresent(key);
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
        try {
            // get from cache and download if not available
            return cache.get(key);
        } catch (ExecutionException e) {
            LOG.error("Error loading [{}] from cache", key);
            throw new IOException(e);
        }
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
    private class CacheBuildJob implements Callable {
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
        int count = 0;

        // Move older generation cache downloaded files to the new folder
        DataStoreCacheUpgradeUtils.moveDownloadCache(parent);

        // Iterate over all files in the cache folder
        Iterator<File> iter = Files.fileTreeTraverser().postOrderTraversal(cacheRoot)
            .filter(new Predicate<File>() {
                @Override public boolean apply(File input) {
                    return input.isFile() && !normalizeNoEndSeparator(input.getParent())
                        .equals(cacheRoot.getAbsolutePath());
                }
            }).iterator();
        while (iter.hasNext()) {
            File toBeSyncedFile = iter.next();
            try {
                put(toBeSyncedFile.getName(), toBeSyncedFile, false);
                count++;
                LOG.trace("Added file [{}} to in-memory cache", toBeSyncedFile);
            } catch (Exception e) {
                LOG.error("Error in putting cached file in map[{}]", toBeSyncedFile);
            }
        }
        LOG.trace("[{}] files put in im-memory cache", count);
        return count;
    }
}

class FileCacheStats extends CacheStats implements DataStoreCacheStatsMBean {
    private static final long KB = 4 * 1024;
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
            size += weigher.weigh(k, v) * KB;
        }
        return size;
    }
}
