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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.Nullable;

import com.google.common.cache.AbstractCache;
import com.google.common.cache.CacheLoader;

import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;

/**
 */
public class CompositeDataStoreCache extends AbstractCache<String, File> implements Closeable {
    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(CompositeDataStoreCache.class);

    /**
     * Cache for downloaded blobs
     */
    private final FileCache downloadCache;

    /**
     * Cache for staging async uploads
     */
    private final UploadStagingCache stagingCache;

    /**
     * The directory where the files are created.
     */
    private final File directory;

    public CompositeDataStoreCache(String path, File home, long size, int uploadSplitPercentage,
        int uploadThreads, CacheLoader<String, InputStream> loader, final StagingUploader uploader,
        StatisticsProvider statsProvider, ListeningExecutorService listeningExecutor,
        ScheduledExecutorService scheduledExecutor /* purge scheduled executor */,
        ExecutorService executor /* File cache executor */,
        int purgeInterval /* async purge interval secs */,
        int stagingRetryInterval /* async retry interval secs */) {

        checkArgument(uploadSplitPercentage >= 0 && uploadSplitPercentage < 100,
            "Upload percentage should be between 0 and 100");

        this.directory = new File(path);

        long uploadSize = (size * uploadSplitPercentage) / 100;

        this.stagingCache = UploadStagingCache
            .build(directory, home, uploadThreads, uploadSize, uploader, null, statsProvider,
                listeningExecutor, scheduledExecutor, purgeInterval, stagingRetryInterval);
        this.downloadCache = FileCache.build((size - uploadSize), directory, loader, executor);
        stagingCache.setDownloadCache(downloadCache);
    }

    @Nullable
    public File getIfPresent(String key) {
        // Check if the file scheduled for async upload
        File staged = stagingCache.getIfPresent(key);
        if (staged != null && staged.exists()) {
            return staged;
        }
        return downloadCache.getIfPresent(key);
    }

    @Nullable
    @Override
    public File getIfPresent(Object key) {
        return getIfPresent((String) key);
    }

    public File get(String key) throws IOException {
        try {
            // Check if the file scheduled for async upload
            File staged = stagingCache.getIfPresent(key);
            if (staged != null && staged.exists()) {
                return staged;
            }
            // get from cache and download if not available
            return downloadCache.get(key);
        } catch (IOException e) {
            LOG.error("Error loading [{}] from cache", key);
            throw e;
        }
    }

    @Override
    public void invalidate(Object key) {
        stagingCache.invalidate((String) key);
        downloadCache.invalidate(key);
    }

    public boolean stage(String key, File file) {
        return stagingCache.put(key, file).isPresent();
    }


    public DataStoreCacheStatsMBean getStagingCacheStats() {
        return stagingCache.getStats();
    }

    public DataStoreCacheStatsMBean getCacheStats() {
        return downloadCache.getStats();
    }

    @Override
    public void close() {
        downloadCache.close();
        stagingCache.close();
    }

    UploadStagingCache getStagingCache() {
        return stagingCache;
    }

    FileCache getDownloadCache() {
        return downloadCache;
    }
}
