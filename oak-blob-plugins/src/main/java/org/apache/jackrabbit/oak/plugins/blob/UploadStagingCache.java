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
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.jackrabbit.guava.common.cache.Weigher;
import org.apache.jackrabbit.guava.common.io.Files;
import org.apache.jackrabbit.guava.common.util.concurrent.FutureCallback;
import org.apache.jackrabbit.guava.common.util.concurrent.Futures;
import org.apache.jackrabbit.guava.common.util.concurrent.ListenableFuture;
import org.apache.jackrabbit.guava.common.util.concurrent.ListeningExecutorService;
import org.apache.jackrabbit.guava.common.util.concurrent.MoreExecutors;
import org.apache.jackrabbit.guava.common.util.concurrent.SettableFuture;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.util.NamedThreadFactory;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.commons.jmx.AnnotatedStandardMBean;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.String.format;
import static org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount;
import static org.apache.jackrabbit.oak.plugins.blob.DataStoreCacheUpgradeUtils
    .movePendingUploadsToStaging;

/**
 * Cache for staging async uploads. This serves as a temporary cache for serving local
 * requests till the time the upload has not been synced with the backend.
 * <p>
 * The appropriate backend for this cache are wrapped in {@link StagingUploader}
 * implementations.
 * <p>
 */
public class UploadStagingCache implements Closeable {
    /**
     * Logger instance.
     */
    private static final Logger LOG = LoggerFactory.getLogger(UploadStagingCache.class);

    protected static final String UPLOAD_STAGING_DIR = "upload";

    //Rough estimate of the in-memory key, value pair
    private final Weigher<String, File> memWeigher = new Weigher<>() {
        @Override public int weigh(String key, File value) {
            return (StringUtils.estimateMemoryUsage(key) +
                StringUtils.estimateMemoryUsage(value.getAbsolutePath()) + 48);
        }};

    /**
     * Max size of the upload staging cache in bytes
     */
    private long size;

    /**
     * Current cache size in bytes
     */
    private AtomicLong currentSize;

    /**
     * Executor for async uploads
     */
    private ListeningExecutorService executor;

    /**
     * Scheduled executor for build and remove
     */
    private ScheduledExecutorService scheduledExecutor;

    /**
     * In memory map for staged files
     */
    private ConcurrentMap<String, File> map;

    /**
     * In memory map for files to be deleted after uploads
     */
    private ConcurrentMap<String, File> attic;

    /**
     * Local directory where uploads are staged
     */
    private File uploadCacheSpace;

    /**
     * Wrapper to where the blobs are uploaded/written
     */
    private StagingUploader uploader;

    /**
     * Cache stats
     */
    private StagingCacheStats cacheStats;

    /**
     * Handle for download cache if any
     */
    @Nullable
    private FileCache downloadCache;

    /**
     * Scheduled executor for stats in case required
     */
    private ScheduledExecutorService statsExecutor;

    /**
     * Queue containing items to retry.
     */
    private LinkedBlockingQueue<String> retryQueue;

    private UploadStagingCache(File dir, File home, int uploadThreads, long size /* bytes */,
        StagingUploader uploader, @Nullable FileCache cache, StatisticsProvider statisticsProvider,
        @Nullable ListeningExecutorService executor,
        @Nullable ScheduledExecutorService scheduledExecutor,
        int purgeInterval /* secs */, int retryInterval /* secs */) {

        this.currentSize = new AtomicLong();
        this.size = size;
        this.executor = executor;
        if (executor == null) {
            this.executor = MoreExecutors.listeningDecorator(Executors
                .newFixedThreadPool(uploadThreads, new NamedThreadFactory("oak-ds-async-upload-thread")));
        }

        this.scheduledExecutor = scheduledExecutor;
        if (scheduledExecutor == null) {
            this.scheduledExecutor = Executors
                .newScheduledThreadPool(2, new NamedThreadFactory("oak-ds-cache-scheduled-thread"));
        }

        this.map = new ConcurrentHashMap<>();
        this.attic = new ConcurrentHashMap<>();
        this.retryQueue = new LinkedBlockingQueue<>();
        this.uploadCacheSpace = new File(dir, "upload");
        this.uploader = uploader;
        if (statisticsProvider == null) {
            statsExecutor = Executors.newSingleThreadScheduledExecutor();
            statisticsProvider = new DefaultStatisticsProvider(statsExecutor);
        }
        this.cacheStats = new StagingCacheStats(this, statisticsProvider, size);
        this.downloadCache = cache;

        build(home, dir);

        this.scheduledExecutor
            .scheduleAtFixedRate(new RemoveJob(), purgeInterval, purgeInterval, TimeUnit.SECONDS);
        this.scheduledExecutor
            .scheduleAtFixedRate(new RetryJob(), retryInterval, retryInterval, TimeUnit.SECONDS);
    }

    private UploadStagingCache() {
    }

    public static UploadStagingCache build(File dir, File home, int uploadThreads, long size
        /* bytes */, StagingUploader uploader, @Nullable FileCache cache,
        StatisticsProvider statisticsProvider, @Nullable ListeningExecutorService executor,
        @Nullable ScheduledExecutorService scheduledExecutor, int purgeInterval /* secs */,
        int retryInterval /* secs */) {
        if (size > 0) {
            return new UploadStagingCache(dir, home, uploadThreads, size, uploader, cache,
                statisticsProvider, executor, scheduledExecutor, purgeInterval, retryInterval);
        }
        return new UploadStagingCache() {
            @Override public Optional<SettableFuture<Integer>> put(String id, File input) {
                return Optional.empty();
            }

            @Override protected void invalidate(String key) {
            }

            @Override protected Iterator<String> getAllIdentifiers() {
                return Collections.emptyIterator();
            }

            @Nullable @Override public File getIfPresent(String key) {
                return null;
            }

            @Override public DataStoreCacheStatsMBean getStats() {
                return new StagingCacheStats(this, StatisticsProvider.NOOP, 0);
            }

            @Override public void close() {
            }
        };
    }

    /**
     * Retrieves all the files staged in the staging area and schedules them for uploads.
     * @param home the home of the repo
     * @param rootPath the parent of the cache
     */
    private void build(File home, File rootPath) {
        LOG.info("Scheduling pending uploads");
        // Move any older cache pending uploads
        movePendingUploadsToStaging(home, rootPath, true);

        List<File> files;
        try {
            uploadCacheSpace.mkdirs();
            try (Stream<Path> stream = java.nio.file.Files.find(uploadCacheSpace.toPath(), Integer.MAX_VALUE, (path, basicFileAttributes) -> basicFileAttributes.isRegularFile())) {
                files = stream.map(Path::toFile).collect(Collectors.toList());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        int count = 0;
        for (File toBeSyncedFile : files) {
            Optional<SettableFuture<Integer>> scheduled =
                putOptionalDisregardingSize(toBeSyncedFile.getName(), toBeSyncedFile, true);
            if (scheduled.isPresent()) {
                count++;
            } else {
                LOG.info("File [{}] not setup for upload", toBeSyncedFile.getName());
            }
        }

        LOG.info("Scheduled [{}] pending uploads", count);
    }

    /**
     * Puts the file into the staging cache if possible.
     * Returns an optional SettableFuture if staged for upload otherwise empty.
     *
     * @param id the id of the file to be staged
     * @param input the file to be staged
     * @return An Optional SettableFuture containing
     *              1 if upload was successful,
     *              0 if an existing id is already pending for upload
     */
    public Optional<SettableFuture<Integer>> put(String id, File input) {
        return putOptionalDisregardingSize(id, input, false);
    }

    /**
     * Puts the file into the staging cache if ignoreSize else if possible
     * Returns an optional SettableFuture if staged for upload otherwise empty.
     *
     * @param id
     * @param input
     * @param ignoreSize
     * @return
     */
    private Optional<SettableFuture<Integer>> putOptionalDisregardingSize(String id, File input,
        boolean ignoreSize) {
        cacheStats.markRequest();

        long length = input.length();
        File uploadFile = DataStoreCacheUtils.getFile(id, uploadCacheSpace);

        // if ignoreSize update internal size else size permits
        // and not upload complete or already scheduled for upload
        if (((ignoreSize && currentSize.addAndGet(length) >= 0)
                || currentSize.addAndGet(length) <= size)
            && !attic.containsKey(id)
            && existsOrNotExistsMoveFile(input, uploadFile, currentSize, length)
            && map.putIfAbsent(id, uploadFile) == null ) {

            try {
                // update stats
                cacheStats.markHit();
                cacheStats.incrementCount();
                cacheStats.incrementSize(length);
                cacheStats.incrementMemSize(memWeigher.weigh(id, uploadFile));

                return Optional.of(stage(id, uploadFile));
            } catch (Exception e) {
                LOG.info("Error moving file to staging", e);
                //reset the current state and return empty flag as not added to cache
                currentSize.addAndGet(-length);
                map.remove(id, uploadFile);
            }
        } else {
            currentSize.addAndGet(-length);

            // if file is still pending upload, count it as present
            if (map.containsKey(id) || attic.containsKey(id)) {
                SettableFuture<Integer> result = SettableFuture.create();
                result.set(0);
                return Optional.of(result);
            }
        }
        return Optional.empty();
    }

    private synchronized boolean existsOrNotExistsMoveFile(File source, File destination, AtomicLong currentSize,
        long length) {
        if (!destination.exists()) {
            try {
                uploader.adopt(source, destination);
                LOG.trace("Moved file to staging");
            } catch (IOException e) {
                LOG.info("Error moving file to staging", e);
                currentSize.addAndGet(-length);
                return false;
            }
            LOG.trace("File [{}] moved to staging cache [{}]", source, destination);
            return true;
        }
        return true;
    }

    /**
     * Stages the file for async upload.
     * * Puts the file into the stage caching file system directory
     * * Schedules a job for upload to write using the given {@link StagingUploader}
     * * Updates the internal map and size variable
     * * Adds a callback listener to remove the file once finished
     * @param id of the file to be staged
     * @param upload the file to be staged
     * @return a SettableFuture instance
     */
    private SettableFuture<Integer> stage(final String id, final File upload) {
        final SettableFuture<Integer> result = SettableFuture.create();

        try {
            // create an async job
            ListenableFuture<Integer> future = executor.submit(() -> {
                try (TimerStats.Context uploadContext = cacheStats.startUpLoaderTimer()) {

                    uploader.write(id, upload);
                    LOG.debug("File added to backend [{}]", upload);

                    return 1;
                } catch (Exception e) {
                    LOG.error("Error adding file to backend", e);
                    throw e;
                }
            });

            // Add a callback to the returned Future object for handling success and error
            Futures.addCallback(future, new FutureCallback<>() {
                @Override public void onSuccess(@Nullable Integer r) {
                    LOG.info("Successfully added [{}], [{}]", id, upload);

                    try {
                        // move to attic to be deleted and remove from in-memory map
                        attic.put(id, upload);

                        // Add the uploaded file to the download cache if available
                        if (downloadCache != null) {
                            // Touch the file to update timestamp and record length
                            Files.touch(upload);
                            downloadCache.put(id, upload);

                            LOG.debug("[{}] added to cache", id);
                        }

                        map.remove(id);
                    } catch (IOException e) {
                        LOG.warn("Error in cleaning up [{}] from staging", upload);
                    }
                    result.set(r);
                }

                @Override public void onFailure(Throwable t) {
                    LOG.error("Error adding [{}] with file [{}] to backend", id, upload, t);
                    result.setException(t);
                    retryQueue.add(id);
                }
            }, new SameThreadExecutorService());
            LOG.debug("File [{}] scheduled for upload [{}]", upload, result);
        } catch (Exception e) {
            LOG.error("Error staging file for upload [{}]", upload, e);
        }
        return result;
    }


    /**
     * Invalidate called externally.
     * @param key to invalidate
     */
    protected void invalidate(String key) {
        // Check if not already scheduled for deletion
        if (!attic.containsKey(key) && map.containsKey(key)) {
            try {
                LOG.debug("Invalidating [{}]", key);
                File toBeDeleted = map.get(key);
                deleteInternal(key, toBeDeleted);
                map.remove(key, toBeDeleted);
            } catch (IOException e) {
                LOG.warn("Could not delete file from staging", e);
            }
        }
    }

    /**
     * Returns all identifiers presently staged.
     *
     * @return iterator of all identifiers presently staged.
     */
    protected Iterator<String> getAllIdentifiers() {
        return map.keySet().iterator();
    }

    /**
     * Removes all cached from attic
     */
    private void remove() {
        LOG.info("Starting purge of uploaded files, current size [{}]", humanReadableByteCount(currentSize.get()));

        Iterator<String> iterator = attic.keySet().iterator();
        int count = 0;
        while (iterator.hasNext()) {
            String key = iterator.next();
            try {
                // Check if not already scheduled for upload
                if (!map.containsKey(key)) {
                    LOG.trace("upload map contains id [{}]", key);

                    File toBeDeleted = attic.get(key);
                    deleteInternal(key, toBeDeleted);
                    iterator.remove();

                    LOG.debug("Cache [{}] file deleted for id [{}]", toBeDeleted, key);
                    count++;
                }
            } catch (IOException e) {
                LOG.error("Error in removing entry for id [{}]", key);
            }
        }

        LOG.info("Finished removal of [{}] files, current size [{}]", count, humanReadableByteCount(currentSize.get()));
    }

    /**
     * Adjust stats and delete file.
     *
     * @param key to delete
     * @param toBeDeleted file to delete
     * @throws IOException
     */
    private void deleteInternal(String key, File toBeDeleted) throws IOException {
        LOG.debug("Trying to delete file [{}]", toBeDeleted);
        long length = toBeDeleted.length();

        DataStoreCacheUtils.recursiveDelete(toBeDeleted, uploadCacheSpace);
        LOG.debug("deleted file [{}]", toBeDeleted);

        currentSize.addAndGet(-length);
        // Update stats for removal
        cacheStats.decrementSize(length);
        cacheStats.decrementMemSize(memWeigher.weigh(key, toBeDeleted));
        cacheStats.decrementCount();
    }

    /**
     * Returns the File if present or null otherwise.
     * Any usage of the returned file should assert for its existence as the file
     * could be purged from the file system once uploaded using the internal scheduled remove
     * mechanism.
     *
     * @param key of the file to check
     * @return a File object if found
     */
    @Nullable
    public File getIfPresent(String key) {
        cacheStats.markLoad();
        if (map.containsKey(key)) {
            cacheStats.markLoadSuccess();
            return map.get(key);
        }
        return null;
    }

    /**
     * Cache related stats
     *
     * @return an instance of the {@link DataStoreCacheStatsMBean}.
     */
    public DataStoreCacheStatsMBean getStats() {
        return cacheStats;
    }

    @Override
    public void close() {
        LOG.info("Uploads in progress on close [{}]", map.size());
        LOG.info("Uploads completed but not cleared from cache [{}]", attic.size());
        LOG.info("Staging cache stats on close [{}]", cacheStats.cacheInfoAsString());
        new ExecutorCloser(executor).close();
        new ExecutorCloser(scheduledExecutor).close();
        new ExecutorCloser(statsExecutor).close();
    }

    protected void setDownloadCache(@Nullable FileCache downloadCache) {
        this.downloadCache = downloadCache;
    }

    /**
     * Class which calls remove on all
     */
    class RemoveJob implements Runnable {
        @Override
        public void run() {
            remove();
        }
    }


    /**
     * Job to retry failed uploads.
     */
    class RetryJob implements Runnable {
        @Override
        public void run() {
            LOG.debug("Retry job started");
            int count = 0;
            List<String> entries = new ArrayList<>();
            retryQueue.drainTo(entries);
            for (String key : entries) {
                File file = map.get(key);
                LOG.info("Retrying upload of id [{}] with file [{}] ", key, file);
                stage(key, file);
                count++;
                LOG.info("Scheduled retry for upload of id [{}] with file [{}]", key, file);
            }
            LOG.debug("Retry job finished with staging [{}] jobs", count);
        }
    }
}

/**
 * Upload Staging Cache Statistics.
 */
class StagingCacheStats extends AnnotatedStandardMBean implements DataStoreCacheStatsMBean {

    private static final String HITS = "HITS";
    private static final String REQUESTS = "REQUESTS";
    private static final String UPLOAD_TIMER = "UPLOAD_TIMER";
    private static final String LOAD_SUCCESS = "CACHE_LOAD_SUCCESS";
    private static final String LOAD = "CACHE_LOAD";
    private static final String CURRENT_SIZE = "CURRENT_SIZE";
    private static final String CURRENT_MEM_SIZE = "CURRENT_MEM_SIZE";
    private static final String COUNT = "COUNT";

    private final String cacheName;

    /** Max size in bytes configured for the cache **/
    private final long maxWeight;

    /** Tracking the number of uploads that could be staged **/
    private final MeterStats hitMeter;

    /** Tracking the number of requests to upload & stage **/
    private final MeterStats requestMeter;

    /** Tracking the number of get requests serviced by the cache **/
    private final MeterStats loadSuccessMeter;

    /** Tracking the number of get requests received by the cache **/
    private final MeterStats loadMeter;

    /** Tracking the upload time **/
    private final TimerStats uploadTimer;

    /** Tracking the current size in MB **/
    private final CounterStats currentSizeMeter;

    /** Tracking the in-memory size of cache **/
    private final CounterStats currentMemSizeMeter;

    /** Tracking the cache element count **/
    private final CounterStats countMeter;

    /** Handle to the cache **/
    private final UploadStagingCache cache;

    StagingCacheStats(UploadStagingCache cache, StatisticsProvider provider, long maxWeight) {
        super(DataStoreCacheStatsMBean.class);
        this.cache = cache;

        StatisticsProvider statisticsProvider = provider;

        // Configure cache name
        cacheName = "DataStore-StagingCache";

        this.maxWeight = maxWeight;

        // Fetch stats and time series
        String statName;

        statName = getStatName(HITS, cacheName);
        hitMeter = statisticsProvider.getMeter(statName, StatsOptions.METRICS_ONLY);

        statName = getStatName(REQUESTS, cacheName);
        requestMeter = statisticsProvider.getMeter(statName, StatsOptions.METRICS_ONLY);

        statName = getStatName(UPLOAD_TIMER, cacheName);
        uploadTimer = statisticsProvider.getTimer(statName, StatsOptions.METRICS_ONLY);

        statName = getStatName(LOAD_SUCCESS, cacheName);
        loadSuccessMeter = statisticsProvider.getMeter(statName, StatsOptions.METRICS_ONLY);

        statName = getStatName(LOAD, cacheName);
        loadMeter = statisticsProvider.getMeter(statName, StatsOptions.METRICS_ONLY);

        statName = getStatName(CURRENT_SIZE, cacheName);
        currentSizeMeter = statisticsProvider.getCounterStats(statName, StatsOptions.METRICS_ONLY);

        statName = getStatName(CURRENT_MEM_SIZE, cacheName);
        currentMemSizeMeter = statisticsProvider.getCounterStats(statName, StatsOptions.METRICS_ONLY);

        statName = getStatName(COUNT, cacheName);
        countMeter = statisticsProvider.getCounterStats(statName, StatsOptions.METRICS_ONLY);
    }

    //~--------------------------------------< stats update methods

    void markHit() {
        hitMeter.mark(1);
    }

    void markRequest() {
        requestMeter.mark(1);
    }

    void markLoadSuccess() {
        loadSuccessMeter.mark(1);
    }

    void markLoad() {
        loadMeter.mark(1);
    }

    TimerStats.Context startUpLoaderTimer() {
        return this.uploadTimer.time();
    }

    void incrementCount() {
        countMeter.inc(1);
    }

    void incrementSize(long size) {
        currentSizeMeter.inc(size);
    }

    void incrementMemSize(long size) {
        currentMemSizeMeter.inc(size);
    }

    void decrementCount() {
        countMeter.dec(1);
    }

    void decrementSize(long size) {
        currentSizeMeter.dec(size);
    }


    void decrementMemSize(int size) {
        currentMemSizeMeter.dec(size);
    }

    @Override
    public String getName() {
        return cacheName;
    }

    @Override
    public long getRequestCount() {
        return requestMeter.getCount();
    }

    @Override
    public long getHitCount() {
        return hitMeter.getCount();
    }

    @Override
    public double getHitRate() {
        long hitCount = hitMeter.getCount();
        long requestCount = requestMeter.getCount();
        return (requestCount == 0L ? 0L : (double)hitCount/requestCount);
    }

    @Override
    public long getMissCount() {
        return requestMeter.getCount() - hitMeter.getCount();
    }

    @Override
    public double getMissRate() {
        long missCount = getMissCount();
        long requestCount = requestMeter.getCount();
        return (requestCount == 0L ? 0L : (double) missCount/requestCount);
    }

    @Override
    public long getLoadCount() {
        return loadMeter.getCount();
    }

    @Override
    public long getLoadSuccessCount() {
        return loadSuccessMeter.getCount();
    }

    @Override
    public long getLoadExceptionCount() {
        return (getLoadCount() - getLoadSuccessCount());
    }

    @Override
    public double getLoadExceptionRate() {
        long loadExceptionCount = getLoadExceptionCount();
        long loadCount = loadMeter.getCount();
        return (loadCount == 0L ? 0L : (double) loadExceptionCount/loadCount);
    }

    @Override
    public long getElementCount() {
        return countMeter.getCount();
    }

    @Override
    public long getMaxTotalWeight() {
        return maxWeight;
    }

    @Override
    public long estimateCurrentWeight() {
        return currentSizeMeter.getCount();
    }

    @Override
    public long estimateCurrentMemoryWeight() {
        return currentMemSizeMeter.getCount();
    }

    @Override
    public String cacheInfoAsString() {
        return new StringJoiner(", ", "StagingCacheStats + [", "]")
            .add("requestCount=" + getRequestCount())
            .add("hitCount=" + getHitCount())
            .add("hitRate=" + format("%1.2f", getHitRate()))
            .add("missCount=" + getMissCount())
            .add("missRate=" + format("%1.2f", getMissRate()))
            .add("loadCount=" + getLoadCount())
            .add("loadSuccessCount=" + getLoadSuccessCount())
            .add("elementCount=" + getElementCount())
            .add("currentMemSize=" + estimateCurrentMemoryWeight())
            .add("totalWeight=" + humanReadableByteCount(estimateCurrentWeight()))
            .add("maxWeight=" + humanReadableByteCount(getMaxTotalWeight()))
            .toString();
    }

    //~--------------------------------------< CacheStatsMBean - stats that are not (yet) available
    @Override
    public long getTotalLoadTime() {
        return 0;
    }

    @Override
    public double getAverageLoadPenalty() {
        return 0;
    }

    @Override
    public long getEvictionCount() {
        return 0;
    }

    @Override
    public void resetStats() {
    }

    //~--------------------------------------< private helpers

    private static String getStatName(String meter, String cacheName) {
        return cacheName + "." + meter;
    }
}

/**
 * Wrapper for backend used for uploading
 */
interface StagingUploader {
    void write(String id, File f) throws DataStoreException;

    void adopt(File f, File moved) throws IOException;
}
