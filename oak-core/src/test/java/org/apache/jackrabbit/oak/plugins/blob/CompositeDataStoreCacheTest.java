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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.io.Closer;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.util.concurrent.MoreExecutors.sameThreadExecutor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link CompositeDataStoreCache}.
 */
public class CompositeDataStoreCacheTest extends AbstractDataStoreCacheTest {
    private static final Logger LOG = LoggerFactory.getLogger(UploadStagingCacheTest.class);
    private static final String ID_PREFIX = "12345";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    private CompositeDataStoreCache cache;
    private final Closer closer = Closer.create();
    private File root;
    private TestStagingUploader uploader;
    private TestCacheLoader loader;

    private CountDownLatch taskLatch;
    private CountDownLatch callbackLatch;
    private CountDownLatch afterExecuteLatch;
    private TestExecutor executor;
    private StatisticsProvider statsProvider;
    private ScheduledExecutorService scheduledExecutor;
    private ExecutorService fileCacheExecutor;

    @Before
    public void setup() throws Exception {
        LOG.info("Starting setup");

        root = folder.newFolder();
        loader = new TestCacheLoader<String, InputStream>(folder.newFolder());
        uploader = new TestStagingUploader(folder.newFolder());

        // create executor
        taskLatch = new CountDownLatch(1);
        callbackLatch = new CountDownLatch(1);
        afterExecuteLatch = new CountDownLatch(1);
        executor = new TestExecutor(1, taskLatch, callbackLatch, afterExecuteLatch);

        // stats
        ScheduledExecutorService statsExecutor = Executors.newSingleThreadScheduledExecutor();
        closer.register(new ExecutorCloser(statsExecutor, 500, TimeUnit.MILLISECONDS));
        statsProvider = new DefaultStatisticsProvider(statsExecutor);

        scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        closer.register(new ExecutorCloser(scheduledExecutor, 500, TimeUnit.MILLISECONDS));

        fileCacheExecutor = sameThreadExecutor();

        //cache instance
        cache =
            new CompositeDataStoreCache(root.getAbsolutePath(), null, 80 * 1024 /* bytes */, 10, 1/*threads*/,
                loader, uploader, statsProvider, executor, scheduledExecutor, fileCacheExecutor,3000, 6000);
        closer.register(cache);

        LOG.info("Finished setup");
    }

    @After
    public void tear() throws IOException {
        closer.close();
    }

    @Test
    public void zeroCache() throws IOException {
        LOG.info("Starting zeroCache");

        cache = new CompositeDataStoreCache(root.getAbsolutePath(), null, 0 /* bytes
        */, 10, 1/*threads*/, loader,
            uploader, statsProvider, executor, scheduledExecutor, fileCacheExecutor,
            3000, 6000);
        closer.register(cache);

        File f = copyToFile(randomStream(0, 4 * 1024), folder.newFile());
        boolean accepted = cache.stage(ID_PREFIX + 0, f);
        assertFalse(accepted);
        assertNull(cache.getIfPresent(ID_PREFIX + 0));
        assertNull(cache.get(ID_PREFIX + 0));

        assertEquals(0, cache.getStagingCache().getStats().getMaxTotalWeight());
        assertEquals(0, cache.getStagingCacheStats().getMaxTotalWeight());
        assertEquals(0,cache.getDownloadCache().getStats().getMaxTotalWeight());
        assertEquals(0,cache.getCacheStats().getMaxTotalWeight());
        cache.invalidate(ID_PREFIX + 0);
        cache.close();

        LOG.info("Finished zeroCache");
    }

    /**
     * {@link CompositeDataStoreCache#getIfPresent(String)} when no cache.
     */
    @Test
    public void getIfPresentNoCache() {
        LOG.info("Starting getIfPresentNoCache");

        File file = cache.getIfPresent(ID_PREFIX + 0);
        assertNull(file);
        assertCacheStats(cache.getStagingCacheStats(), 0, 0, 0, 0);

        LOG.info("Finished getIfPresentNoCache");
    }

    /**
     * {@link CompositeDataStoreCache#get(String)} when no cache.
     * @throws IOException
     */
    @Test
    public void getNoCache() throws IOException {
        LOG.info("Starting getNoCache");

        expectedEx.expect(IOException.class);
        cache.get(ID_PREFIX + 0);

        LOG.info("Finished getNoCache");
    }

    /**
     * {@link CompositeDataStoreCache#getIfPresent(Object)} when no cache.
     */
    @Test
    public void getIfPresentObjectNoCache() {
        LOG.info("Starting getIfPresentObjectNoCache");

        File file = cache.getIfPresent((Object) (ID_PREFIX + 0));
        assertNull(file);
        assertCacheStats(cache.getStagingCacheStats(), 0, 0, 0, 0);
        assertCacheStats(cache.getDownloadCache().getStats(), 0, 0, 0, 1);

        LOG.info("Finished getIfPresentObjectNoCache");
    }

    /**
     * Add to staging
     */
    @Test
    public void add() throws Exception {
        LOG.info("Starting add");

        File f = copyToFile(randomStream(0, 4 * 1024), folder.newFile());
        boolean accepted = cache.stage(ID_PREFIX + 0, f);
        assertTrue(accepted);

        //start
        taskLatch.countDown();
        callbackLatch.countDown();

        waitFinish();

        File file = cache.getIfPresent(ID_PREFIX + 0);
        assertNotNull(f);
        assertFile(file, 0, folder);
        assertCacheStats(cache.getStagingCacheStats(), 0, 0, 1, 1);

        LOG.info("Finished add");
    }

    /**
     * Add to staging when cache full.
     */
    @Test
    public void addCacheFull() throws IOException {
        LOG.info("Starting addCacheFull");

        cache = new CompositeDataStoreCache(root.getAbsolutePath(), null, 40 * 1024 /*
        bytes */, 10 /* staging % */,
            1/*threads*/, loader, uploader, statsProvider, executor, scheduledExecutor, fileCacheExecutor,
            3000,6000);
        closer.register(cache);

        File f = copyToFile(randomStream(0, 4 * 1024), folder.newFile());
        boolean accepted = cache.stage(ID_PREFIX + 0, f);
        assertTrue(accepted);

        File f2 = copyToFile(randomStream(1, 4 * 1024), folder.newFile());
        accepted = cache.stage(ID_PREFIX + 1, f2);
        assertFalse(accepted);

        //start the original upload
        taskLatch.countDown();
        callbackLatch.countDown();

        waitFinish();

        File file = cache.getIfPresent(ID_PREFIX + 0);
        assertNotNull(f);
        assertFile(file, 0, folder);
        assertCacheStats(cache.getStagingCacheStats(), 0, 0, 1, 2);

        LOG.info("Finished addCacheFull");
    }

    /**
     * Invalidate from staging.
     */
    @Test
    public void invalidateStaging() throws IOException {
        LOG.info("Starting invalidateStaging");

        // create executor
        taskLatch = new CountDownLatch(2);
        callbackLatch = new CountDownLatch(2);
        afterExecuteLatch = new CountDownLatch(2);
        executor = new TestExecutor(1, taskLatch, callbackLatch, afterExecuteLatch);
        cache = new CompositeDataStoreCache(root.getAbsolutePath(), null, 80 * 1024 /*
        bytes */, 10 /* staging % */,
            1/*threads*/, loader, uploader, statsProvider, executor, scheduledExecutor, fileCacheExecutor,
            3000,6000);
        closer.register(cache);


        File f = copyToFile(randomStream(0, 4 * 1024), folder.newFile());
        boolean accepted = cache.stage(ID_PREFIX + 0, f);
        assertTrue(accepted);

        File f2 = copyToFile(randomStream(1, 4 * 1024), folder.newFile());
        accepted = cache.stage(ID_PREFIX + 1, f2);
        assertTrue(accepted);

        cache.invalidate(ID_PREFIX + 0);

        //start the original uploads
        taskLatch.countDown();
        taskLatch.countDown();
        callbackLatch.countDown();
        callbackLatch.countDown();

        waitFinish();

        File file = cache.getIfPresent(ID_PREFIX + 0);
        assertNull(file);
        file = cache.getIfPresent(ID_PREFIX + 1);
        assertFile(file, 1, folder);
        assertCacheStats(cache.getStagingCacheStats(), 0, 0, 2, 2);

        LOG.info("Finished invalidateStaging");
    }

    /**
     * Test {@link CompositeDataStoreCache#getIfPresent(String)} when file staged
     * and then put in download cache when uploaded.
     * @throws IOException
     */
    @Test
    public void getIfPresentStaged() throws IOException {
        LOG.info("Starting getIfPresentStaged");

        get(false);

        LOG.info("Finished getIfPresentStaged");
    }

    /**
     * Test {@link CompositeDataStoreCache#get(String)} when file staged and then put in
     * download cache when uploaded.
     * @throws IOException
     */
    @Test
    public void getStaged() throws IOException {
        LOG.info("Starting getStaged");

        get(true);

        LOG.info("Finished getStaged");
    }

    private void get(boolean get) throws IOException {
        File f = copyToFile(randomStream(0, 4 * 1024), folder.newFile());
        boolean accepted = cache.stage(ID_PREFIX + 0, f);
        assertTrue(accepted);

        // hit the staging cache as not uploaded
        File file;
        if (get) {
            file = cache.get(ID_PREFIX + 0);
        } else {
            file = cache.getIfPresent(ID_PREFIX + 0);
        }
        assertNotNull(file);
        assertFile(file, 0, folder);
        assertCacheStats(cache.getStagingCacheStats(), 1, 4 * 1024, 1, 1);

        //start the original upload
        taskLatch.countDown();
        callbackLatch.countDown();

        waitFinish();

        // Now should hit the download cache
        if (get) {
            file = cache.get(ID_PREFIX + 0);
        } else {
            file = cache.getIfPresent(ID_PREFIX + 0);
        }
        LOG.info("File loaded from cache [{}]", file);

        assertNotNull(file);
        assertFile(file, 0, folder);
        assertCacheStats(cache.getStagingCacheStats(), 0, 0, 1, 1);
        assertCacheStats(cache.getCacheStats(), 1, 4 * 1024, 1, 1);
    }

    /**
     * Load and get from the download cache.
     * @throws Exception
     */
    @Test
    public void getLoad() throws Exception {
        LOG.info("Starting getLoad");

        File f = copyToFile(randomStream(0, 4 * 1024), folder.newFile());
        loader.write(ID_PREFIX + 0, f);

        // Not present yet
        File cached = cache.getIfPresent(ID_PREFIX + 0);
        assertNull(cached);

        // present after loading
        cached = cache.get(ID_PREFIX + 0);
        assertNotNull(cached);
        assertTrue(Files.equal(f, cached));

        assertCacheStats(cache.getStagingCacheStats(), 0, 0, 0, 0);
        assertEquals(2, cache.getStagingCacheStats().getLoadCount());
        assertEquals(0, cache.getStagingCacheStats().getLoadSuccessCount());

        assertCacheStats(cache.getCacheStats(), 1, 4 * 1024, 0, 3);
        assertEquals(1, cache.getCacheStats().getLoadCount());
        assertEquals(1, cache.getCacheStats().getLoadSuccessCount());

        LOG.info("Finished getLoad");
    }

    /**
     * Invalidate cache entry.
     * @throws Exception
     */
    @Test
    public void invalidate() throws Exception {
        LOG.info("Starting invalidate");

        File f = copyToFile(randomStream(0, 4 * 1024), folder.newFile());
        loader.write(ID_PREFIX + 0, f);

        // present after loading
        File cached = cache.get(ID_PREFIX + 0);
        assertNotNull(cached);
        assertTrue(Files.equal(f, cached));

        cache.invalidate(ID_PREFIX + 0);

        // Not present now
        cached = cache.getIfPresent(ID_PREFIX + 0);
        assertNull(cached);

        assertCacheStats(cache.getStagingCacheStats(), 0, 0, 0, 0);
        assertEquals(2, cache.getStagingCacheStats().getLoadCount());
        assertEquals(0, cache.getStagingCacheStats().getLoadSuccessCount());

        assertCacheStats(cache.getCacheStats(), 0, 0, 0, 3);
        assertEquals(1, cache.getCacheStats().getLoadCount());
        assertEquals(1, cache.getCacheStats().getLoadSuccessCount());

        /** Check eviction count */
        assertEquals(0, cache.getCacheStats().getEvictionCount());

        LOG.info("Finished invalidate");
    }

    /**
     * Concurrently retrieves 2 different files from cache.
     * @throws Exception
     */
    @Test
    public void concurrentGetCached() throws Exception {
        LOG.info("Starting concurrentGetCached");

        // Add 2 files to backend
        // Concurrently get both
        ListeningExecutorService executorService =
            MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(2));
        closer.register(new ExecutorCloser(executorService, 5, TimeUnit.MILLISECONDS));

        File f = copyToFile(randomStream(0, 4 * 1024), folder.newFile());
        loader.write(ID_PREFIX + 0, f);
        assertTrue(f.exists());

        File f2 = copyToFile(randomStream(1, 4 * 1024), folder.newFile());
        loader.write(ID_PREFIX + 1, f2);
        assertTrue(f2.exists());

        CountDownLatch thread1Start = new CountDownLatch(1);
        SettableFuture<File> future1 =
            retrieveThread(executorService, ID_PREFIX + 0, cache, thread1Start);

        CountDownLatch thread2Start = new CountDownLatch(1);
        SettableFuture<File> future2 =
            retrieveThread(executorService, ID_PREFIX + 1, cache, thread2Start);

        thread1Start.countDown();
        thread2Start.countDown();

        File cached = future1.get();
        File cached2 = future2.get();
        LOG.info("Async tasks finished");

        assertTrue(Files.equal(f, cached));
        assertTrue(Files.equal(f2, cached2));

        assertCacheStats(cache.getStagingCacheStats(), 0, 0, 0, 0);
        assertEquals(2, cache.getStagingCacheStats().getLoadCount());
        assertEquals(0, cache.getStagingCacheStats().getLoadSuccessCount());

        assertCacheStats(cache.getCacheStats(), 2, 8 * 1024, 0, 4);
        assertEquals(2, cache.getCacheStats().getLoadCount());
        assertEquals(2, cache.getCacheStats().getLoadSuccessCount());

        LOG.info("Finished concurrentGetCached");
    }

    /**
     * Concurrently retrieves 2 different files from cache.
     * One is staged and other in the download cache.
     * @throws Exception
     */
    @Test
    public void concurrentGetFromStagedAndCached() throws Exception {
        LOG.info("Starting concurrentGetFromStagedAndCached");

        // Add 1 to backend
        // Add 2 to upload area
        // Stop upload execution
        // Concurrently get 1 & 2
        // continue upload execution
        ListeningExecutorService executorService =
            MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(2));
        closer.register(new ExecutorCloser(executorService, 5, TimeUnit.MILLISECONDS));

        // Add file to backend
        File f2 = copyToFile(randomStream(1, 4 * 1024), folder.newFile());
        loader.write(ID_PREFIX + 1, f2);
        assertTrue(f2.exists());

        // stage for upload
        File f = copyToFile(randomStream(0, 4 * 1024), folder.newFile());
        boolean accepted = cache.stage(ID_PREFIX + 0, f);
        assertTrue(accepted);

        // Would hit the staging cache
        CountDownLatch thread1Start = new CountDownLatch(1);
        SettableFuture<File> future1 =
            retrieveThread(executorService, ID_PREFIX + 0, cache, thread1Start);

        // Would hit the download cache and load
        CountDownLatch thread2Start = new CountDownLatch(1);
        SettableFuture<File> future2 =
            retrieveThread(executorService, ID_PREFIX + 1, cache, thread2Start);

        thread1Start.countDown();
        thread2Start.countDown();

        File cached = future1.get();
        File cached2 = future2.get();
        LOG.info("Async tasks finished");

        assertFile(cached, 0, folder);
        assertTrue(Files.equal(f2, cached2));

        //start the original upload
        taskLatch.countDown();
        callbackLatch.countDown();

        waitFinish();

        assertCacheStats(cache.getStagingCacheStats(), 0, 0, 1, 1);
        assertEquals(2, cache.getStagingCacheStats().getLoadCount());
        assertEquals(1, cache.getStagingCacheStats().getLoadSuccessCount());

        assertCacheStats(cache.getCacheStats(), 2, 8 * 1024, 0, 2);
        assertEquals(1, cache.getCacheStats().getLoadCount());
        assertEquals(1, cache.getCacheStats().getLoadSuccessCount());

        LOG.info("Finished concurrentGetFromStagedAndCached");
    }

    /**
     * Concurrently stage and get a file and then upload.
     * Use the file retrieve to read contents.
     * @throws Exception
     */
    @Test
    public void concurrentAddGet() throws Exception {
        LOG.info("Starting concurrentAddGet");

        // Add to the upload area
        // stop upload execution
        // Same as above but concurrently
        // Get
        // Continue upload execution
        ListeningExecutorService executorService =
            MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(2));
        closer.register(new ExecutorCloser(executorService, 5, TimeUnit.MILLISECONDS));

        // stage for upload
        File f = copyToFile(randomStream(0, 4 * 1024), folder.newFile());
        boolean accepted = cache.stage(ID_PREFIX + 0, f);
        assertTrue(accepted);

        // Would hit the staging cache
        CountDownLatch thread1Start = new CountDownLatch(1);
        SettableFuture<File> future1 =
            retrieveThread(executorService, ID_PREFIX + 0, cache, thread1Start);
        // Get a handle to the file and open stream
        File fileOnUpload = cache.getIfPresent(ID_PREFIX + 0);
        assertNotNull(fileOnUpload);
        final InputStream fStream = Files.asByteSource(fileOnUpload).openStream();

        thread1Start.countDown();

        //start the original upload
        taskLatch.countDown();
        callbackLatch.countDown();

        future1.get();
        waitFinish();
        LOG.info("Async tasks finished");

        File gold = copyToFile(randomStream(0, 4 * 1024), folder.newFile());
        File fromUploadStream = copyToFile(fStream, folder.newFile());
        assertTrue(Files.equal(gold, fromUploadStream));

        assertEquals(2, cache.getStagingCacheStats().getLoadCount());
        assertEquals(0, cache.getCacheStats().getLoadCount());
        assertEquals(0, cache.getCacheStats().getLoadSuccessCount());

        LOG.info("Finished concurrentAddGet");
    }

    /**--------------------------- Helper Methods -----------------------------------------------**/

    private static SettableFuture<File> retrieveThread(ListeningExecutorService executor,
        final String id, final CompositeDataStoreCache cache, final CountDownLatch start) {
        final SettableFuture<File> future = SettableFuture.create();
        executor.submit(new Runnable() {
            @Override public void run() {
                try {
                    LOG.info("Waiting for start retrieve");
                    start.await();
                    LOG.info("Starting retrieve [{}]", id);
                    File cached = cache.get(id);
                    LOG.info("Finished retrieve");
                    future.set(cached);
                } catch (Exception e) {
                    LOG.info("Exception in get", e);
                    future.setException(e);
                }
            }
        });
        return future;
    }

    private void waitFinish() {
        try {
            // wait for upload finish
            afterExecuteLatch.await();
            // Force execute removal from staging cache
            ScheduledFuture<?> scheduledFuture = scheduledExecutor
                .schedule(cache.getStagingCache().new RemoveJob(), 0, TimeUnit.MILLISECONDS);
            scheduledFuture.get();
            LOG.info("After jobs completed");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void assertCacheStats(DataStoreCacheStatsMBean cache, long elems, long weight,
        long hits, long count) {
        assertEquals("elements don't match", elems, cache.getElementCount());
        assertEquals("weight doesn't match", weight, cache.estimateCurrentWeight());
        assertEquals("hits count don't match", hits, cache.getHitCount());
        assertEquals("requests count don't match", count, cache.getRequestCount());
    }

    private void assertFile(File f, int seed, TemporaryFolder folder) throws IOException {
        assertTrue(f.exists());
        File temp = copyToFile(randomStream(seed, 4 * 1024), folder.newFile());
        assertTrue("Uploaded file content differs", FileUtils.contentEquals(temp, f));
    }
}
