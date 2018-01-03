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

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import ch.qos.logback.classic.Level;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.commons.FileIOUtils;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.commons.junit.LogCustomizer;
import org.apache.jackrabbit.oak.stats.DefaultStatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link UploadStagingCache}.
 */
public class UploadStagingCacheTest extends AbstractDataStoreCacheTest {
    private static final Logger LOG = LoggerFactory.getLogger(UploadStagingCacheTest.class);
    private static final String ID_PREFIX = "12345";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private final Closer closer = Closer.create();
    private TestStagingUploader uploader;
    private File root;
    private CountDownLatch taskLatch;
    private CountDownLatch callbackLatch;
    private CountDownLatch afterExecuteLatch;
    private TestExecutor executor;
    private UploadStagingCache stagingCache;
    private StatisticsProvider statsProvider;
    private ScheduledExecutorService removeExecutor;

    @Before
    public void setup() throws IOException {
        root = folder.newFolder();
        init(0);
    }

    private void init(int i) throws IOException {
        init(i, new TestStagingUploader(folder.newFolder()), null);
    }

    private void init(int i, TestStagingUploader testUploader, File homeDir) {
        // uploader
        uploader = testUploader;

        // create executor
        taskLatch = new CountDownLatch(1);
        callbackLatch = new CountDownLatch(1);
        afterExecuteLatch = new CountDownLatch(i);
        executor = new TestExecutor(1, taskLatch, callbackLatch, afterExecuteLatch);

        // stats
        ScheduledExecutorService statsExecutor = Executors.newSingleThreadScheduledExecutor();
        closer.register(new ExecutorCloser(statsExecutor, 500, TimeUnit.MILLISECONDS));
        statsProvider = new DefaultStatisticsProvider(statsExecutor);

        removeExecutor = Executors.newSingleThreadScheduledExecutor();
        closer.register(new ExecutorCloser(removeExecutor, 500, TimeUnit.MILLISECONDS));

        //cache instance
        stagingCache =
            UploadStagingCache.build(root, homeDir, 1/*threads*/, 8 * 1024 /* bytes */,
                uploader, null/*cache*/, statsProvider, executor, null, 3000, 6000);
        closer.register(stagingCache);
    }

    @After
    public void tear() throws IOException {
        closer.close();
    }

    @Test
    public void testZeroCache() throws IOException {
        stagingCache =
            UploadStagingCache.build(root, null, 1/*threads*/, 0 /* bytes */,
                uploader, null/*cache*/, statsProvider, executor, null, 3000, 6000);
        closer.register(stagingCache);

        File f = copyToFile(randomStream(0, 4 * 1024), folder.newFile());
        Optional<SettableFuture<Integer>> future = stagingCache.put(ID_PREFIX + 0, f);
        assertFalse(future.isPresent());

        assertNull(stagingCache.getIfPresent(ID_PREFIX + 0));
        assertEquals(0, Iterators.size(stagingCache.getAllIdentifiers()));
        assertEquals(0, stagingCache.getStats().getMaxTotalWeight());
    }

    @Test
    public void testDefaultStatsProvider() throws Exception {
        stagingCache =
            UploadStagingCache.build(root, null, 1/*threads*/, 8 * 1024 /* bytes */,
                uploader, null/*cache*/, null, executor, null, 3000, 6000);

        // add load
        File f = copyToFile(randomStream(0, 4 * 1024), folder.newFile());
        Optional<SettableFuture<Integer>> future = stagingCache.put(ID_PREFIX + 0, f);
        assertTrue(future.isPresent());

        assertNotNull(stagingCache.getIfPresent(ID_PREFIX + 0));

        assertCacheStats(stagingCache, 1, 4 * 1024, 1, 1);
    }

    /**
     *  Stage file successful upload.
     * @throws Exception
     */
    @Test
    public void testAdd() throws Exception {
        // add load
        List<ListenableFuture<Integer>> futures = put(folder);

        //start
        taskLatch.countDown();
        callbackLatch.countDown();

        assertFuture(futures, 0);
        assertCacheStats(stagingCache, 0, 0, 1, 1);
    }

    /**
     * Stage file unsuccessful upload.
     * @throws Exception
     */
    @Test
    public void testAddUploadException() throws Exception {
        final AtomicInteger count = new AtomicInteger(0);
        TestStagingUploader secondTimeUploader = new TestStagingUploader(folder.newFolder()) {
            @Override
            public void write(String id, File f) throws DataStoreException {
                if (count.get() == 0) {
                    throw new DataStoreException("Error in writing blob");
                }
                super.write(id, f);
            }
        };

        // initialize staging cache using the mocked uploader
        init(2, secondTimeUploader, null);

        // Add load
        List<ListenableFuture<Integer>> futures = put(folder);

        //start
        taskLatch.countDown();
        callbackLatch.countDown();
        waitFinish(futures);

        // assert file retrieved from staging cache
        File ret = stagingCache.getIfPresent(ID_PREFIX + 0);
        assertTrue(Files.equal(copyToFile(randomStream(0, 4 * 1024), folder.newFile()), ret));

        assertEquals(1, stagingCache.getStats().getLoadCount());
        assertEquals(1, stagingCache.getStats().getLoadSuccessCount());
        assertCacheStats(stagingCache, 1, 4 * 1024, 1, 1);

        // Retry upload and wait for finish
        count.incrementAndGet();
        ScheduledFuture<?> scheduledFuture =
            removeExecutor.schedule(stagingCache.new RetryJob(), 0, TimeUnit.MILLISECONDS);
        scheduledFuture.get();
        afterExecuteLatch.await();

        // Now uploaded
        ret = stagingCache.getIfPresent(ID_PREFIX + 0);
        assertNull(ret);
        assertTrue(Files.equal(copyToFile(randomStream(0, 4 * 1024), folder.newFile()),
            secondTimeUploader.read(ID_PREFIX + 0)));
    }

    /**
     * Retrieve without adding.
     * @throws Exception
     */
    @Test
    public void testGetNoAdd() throws Exception {
        File ret = stagingCache.getIfPresent(ID_PREFIX + 0);

        // assert no file
        assertNull(ret);
        assertEquals(1, stagingCache.getStats().getLoadCount());
        assertCacheStats(stagingCache, 0, 0, 0, 0);
    }

    /**
     * GetAllIdentifiers without adding.
     * @throws Exception
     */
    @Test
    public void testGetAllIdentifiersNoAdd() throws Exception {
        Iterator<String> ids = stagingCache.getAllIdentifiers();
        assertFalse(ids.hasNext());
    }

    /**
     * Invalidate without adding.
     * @throws Exception
     */
    @Test
    public void testInvalidateNoAdd() throws Exception {
        stagingCache.invalidate(ID_PREFIX + 0);
        assertCacheStats(stagingCache, 0, 0, 0, 0);
    }

    /**
     * Error in putting file to stage.
     * @throws Exception
     */
    @Test
    public void testPutMoveFileError() throws Exception {
        File empty = new File(folder.getRoot(), String.valueOf(System.currentTimeMillis()));
        assertFalse(empty.exists());
        Optional<SettableFuture<Integer>> future = stagingCache.put(ID_PREFIX + 0, empty);
        // assert no file
        assertFalse(future.isPresent());
        assertEquals(1, stagingCache.getStats().getMissCount());
        assertCacheStats(stagingCache, 0, 0, 0, 1);
    }

    /**
     * Put and retrieve different files concurrently.
     * @throws Exception
     */
    @Test
    public void testGetAddDifferent() throws Exception {
        //add load
        List<ListenableFuture<Integer>> futures = put(folder);

        // Create an async retrieve task
        final SettableFuture<File> retFuture = SettableFuture.create();
        Thread t = new Thread(new Runnable() {
            @Override public void run() {
                retFuture.set(stagingCache.getIfPresent(ID_PREFIX + 1));
            }
        });

        //start
        taskLatch.countDown();
        callbackLatch.countDown();
        t.start();

        //assert no file retrieve
        assertNull(retFuture.get());
        assertEquals(1, stagingCache.getStats().getLoadCount());

        assertFuture(futures, 0);
        assertCacheStats(stagingCache, 0, 0, 1, 1);
    }

    /**
     * Stage file when cache full.
     * @throws Exception
     */
    @Test
    public void testCacheFullAdd() throws Exception {
        // initialize cache to have restricted size
        stagingCache =
            UploadStagingCache.build(root, null, 1/*threads*/, 4 * 1024 /* bytes */,
                uploader, null/*cache*/, statsProvider, executor, null, 3000, 6000);
        closer.register(stagingCache);

        // add load
        List<ListenableFuture<Integer>> futures = put(folder);

        // Add another load
        File f2 = copyToFile(randomStream(1, 4 * 1024), folder.newFile());
        Optional<SettableFuture<Integer>> future2 = stagingCache.put(ID_PREFIX + 1, f2);
        assertFalse(future2.isPresent());

        //start
        taskLatch.countDown();
        callbackLatch.countDown();
        assertFuture(futures, 0);

        // Try 2nd upload again
        Optional<SettableFuture<Integer>> future = stagingCache.put(ID_PREFIX + 1, f2);
        futures = Lists.newArrayList();
        if (future.isPresent()) {
            futures.add(future.get());
        }
        assertFuture(futures, 1);

        assertCacheStats(stagingCache, 0, 0, 2, 3);
    }

    /**
     * GetAllIdentifiers after staging before upload.
     * @throws Exception
     */
    @Test
    public void testGetAllIdentifiers() throws Exception {
        // add load
        List<ListenableFuture<Integer>> futures = put(folder);

        // Check getAllIdentifiers
        Iterator<String> idsIter = stagingCache.getAllIdentifiers();
        assertEquals(ID_PREFIX + 0, Iterators.getOnlyElement(idsIter));

        //start
        taskLatch.countDown();
        callbackLatch.countDown();
        assertFuture(futures, 0);

        assertCacheStats(stagingCache, 0, 0, 1, 1);

        // Should not return anything
        idsIter = stagingCache.getAllIdentifiers();
        assertEquals(0, Iterators.size(idsIter));
    }

    /**
     * Invalidate after staging before upload.
     * @throws Exception
     */
    @Test
    public void testInvalidate() throws Exception {
        // add load
        List<ListenableFuture<Integer>> futures = put(folder);

        // Check invalidate
        stagingCache.invalidate(ID_PREFIX + 0);
        File file = stagingCache.getIfPresent(ID_PREFIX + 0);
        assertNull(file);

        //start
        taskLatch.countDown();
        callbackLatch.countDown();
        waitFinish(futures);

        assertCacheStats(stagingCache, 0, 0, 1, 1);

        // Should not return anything
        file = stagingCache.getIfPresent(ID_PREFIX + 0);
        assertNull(file);
    }

    /**
     * Stage same file concurrently.
     * @throws Exception
     */
    @Test
    public void testConcurrentSameAdd() throws Exception {
        // Add load
        List<ListenableFuture<Integer>> futures = put(folder);

        File f = copyToFile(randomStream(0, 4 * 1024), folder.newFile());
        Optional<SettableFuture<Integer>> future2 = stagingCache.put(ID_PREFIX + 0, f);
        assertTrue(future2.isPresent());
        assertEquals(future2.get().get().intValue(), 0);

        //start
        taskLatch.countDown();
        callbackLatch.countDown();
        assertFuture(futures, 0);

        assertCacheStats(stagingCache, 0, 0, 1, 2);
    }

    /**
     * Stage different files concurrently
     * @throws Exception
     */
    @Test
    public void testConcurrentDifferentAdd() throws Exception {
        // Add load
        List<ListenableFuture<Integer>> futures = put(folder);

        // Add diff load
        File f2 = copyToFile(randomStream(1, 4 * 1024), folder.newFile());
        Optional<SettableFuture<Integer>> future2 = stagingCache.put(ID_PREFIX + 1, f2);
        if (future2.isPresent()) {
            futures.add(future2.get());
        }

        //start
        taskLatch.countDown();
        callbackLatch.countDown();
        assertFuture(futures, 0, 1);

        assertCacheStats(stagingCache, 0, 0, 2, 2);
    }

    /**
     * Concurrently retrieve after stage but before upload.
     * @throws Exception
     */
    @Test
    public void testConcurrentGetDelete() throws Exception {
        ListeningExecutorService executorService =
            MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(2));
        closer.register(new ExecutorCloser(executorService));

        // Add load
        List<ListenableFuture<Integer>> futures = put(folder);

        // Get a handle to the file and open stream
        File file = stagingCache.getIfPresent(ID_PREFIX + 0);
        final InputStream fStream = Files.asByteSource(file).openStream();

        // task to copy the steam to a file simulating read from the stream
        File temp = folder.newFile();
        CountDownLatch copyThreadLatch = new CountDownLatch(1);
        SettableFuture<File> future1 =
            copyStreamThread(executorService, fStream, temp, copyThreadLatch);

        //start
        taskLatch.countDown();
        callbackLatch.countDown();

        waitFinish(futures);

        // trying copying now
        copyThreadLatch.countDown();
        future1.get();

        assertTrue(Files.equal(temp, uploader.read(ID_PREFIX + 0)));
    }

    /**
     * Concurrently stage and trigger delete after upload for same file.
     * @throws Exception
     */
    @Test
    public void testConcurrentPutDeleteSame() throws Exception {
        testConcurrentPutDelete(0);
    }

    /**
     * Concurrently stage and trigger delete after upload for different file.
     * @throws Exception
     */
    @Test
    public void testConcurrentPutDeleteDifferent() throws Exception {
        testConcurrentPutDelete(1);
    }

    private void testConcurrentPutDelete(int diff) throws Exception {
        ListeningExecutorService executorService =
            MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(2));
        closer.register(new ExecutorCloser(executorService));
        //start immediately
        taskLatch.countDown();

        // Add immediately
        List<ListenableFuture<Integer>> futures = put(folder);

        // New task to put another file
        File f2 = copyToFile(randomStream(diff, 4 * 1024), folder.newFile());
        CountDownLatch putThreadLatch = new CountDownLatch(1);
        CountDownLatch triggerLatch = new CountDownLatch(1);
        SettableFuture<Optional<SettableFuture<Integer>>> future1 =
            putThread(executorService, diff, f2, stagingCache, putThreadLatch, triggerLatch);
        putThreadLatch.countDown();

        // wait for put thread to go ahead
        callbackLatch.countDown();
        ScheduledFuture<?> scheduledFuture =
            removeExecutor.schedule(stagingCache.new RemoveJob(), 0, TimeUnit.MILLISECONDS);
        triggerLatch.await();
        if (future1.get().isPresent()) {
            futures.add(future1.get().get());
        }

        ListenableFuture<List<Integer>> listListenableFuture = Futures.successfulAsList(futures);
        try {
            listListenableFuture.get();
            scheduledFuture.get();
        } catch (Exception e) {
            e.printStackTrace();
        }

        assertTrue(Files.equal(copyToFile(randomStream(0, 4 * 1024), folder.newFile()),
            uploader.read(ID_PREFIX + 0)));
        assertTrue(Files.equal(copyToFile(randomStream(diff, 4 * 1024), folder.newFile()),
            uploader.read(ID_PREFIX + diff)));
    }

    /**
     * Test build on start.
     * @throws Exception
     */
    @Test
    public void testBuild() throws Exception {
        // Add load
        List<ListenableFuture<Integer>> futures = put(folder);
        // Close before uploading finished
        closer.close();

        // Start again
        init(1);
        taskLatch.countDown();
        callbackLatch.countDown();
        afterExecuteLatch.await();

        waitFinish(futures);

        assertNull(stagingCache.getIfPresent(ID_PREFIX + 0));
        assertTrue(Files.equal(copyToFile(randomStream(0, 4 * 1024), folder.newFile()),
            uploader.read(ID_PREFIX + 0)));
        assertCacheStats(stagingCache, 0, 0, 1, 1);
    }

    /**
     * Test build on start with more files available in terms of total size in the upload cache.
     * @throws Exception
     */
    @Test
    public void testBuildMoreThanCacheSize() throws Exception {
        closer.close();

        // create load greater than the cache size upgrades or cache size changes
        File f1 = copyToFile(randomStream(1, 4 * 1024),
            DataStoreCacheUtils.getFile(ID_PREFIX + "1", new File(root, "upload")));
        File f2 = copyToFile(randomStream(2, 4 * 1024),
            DataStoreCacheUtils.getFile(ID_PREFIX + "2", new File(root, "upload")));
        File f3 = copyToFile(randomStream(3, 4 * 1024),
            DataStoreCacheUtils.getFile(ID_PREFIX + "3", new File(root, "upload")));
        // Directly add files to staging dir simulating an upgrade scenario

        // Start staging cache
        init(3);

        List<ListenableFuture<Integer>> futures = put(folder);
        // Not staged as already full
        assertTrue(futures.isEmpty());

        taskLatch.countDown();
        callbackLatch.countDown();
        afterExecuteLatch.await();

        waitFinish(futures);

        assertNull(stagingCache.getIfPresent(ID_PREFIX + 1));
        assertNull(stagingCache.getIfPresent(ID_PREFIX + 2));
        assertNull(stagingCache.getIfPresent(ID_PREFIX + 3));

        // Initial files should have been uploaded
        assertTrue(Files.equal(copyToFile(randomStream(1, 4 * 1024), folder.newFile()),
            uploader.read(ID_PREFIX + 1)));
        assertTrue(Files.equal(copyToFile(randomStream(2, 4 * 1024), folder.newFile()),
            uploader.read(ID_PREFIX + 2)));
        assertTrue(Files.equal(copyToFile(randomStream(3, 4 * 1024), folder.newFile()),
            uploader.read(ID_PREFIX + 3)));
        assertCacheStats(stagingCache, 0, 0, 3, 4);
    }

    /**
     * Test upgrade with build on start.
     * @throws Exception
     */
    @Test
    public void testUpgrade() throws Exception {
        // Add load
        List<ListenableFuture<Integer>> futures = put(folder);
        // Close before uploading finished
        closer.close();

        // Create pre-upgrade load
        File home = folder.newFolder();
        File pendingUploadsFile = new File(home, DataStoreCacheUpgradeUtils.UPLOAD_MAP);
        createUpgradeLoad(home, pendingUploadsFile);

        // Start again
        init(2, new TestStagingUploader(folder.newFolder()), home);

        taskLatch.countDown();
        callbackLatch.countDown();
        afterExecuteLatch.await();

        waitFinish(futures);

        assertNull(stagingCache.getIfPresent(ID_PREFIX + 0));
        assertTrue(Files.equal(copyToFile(randomStream(0, 4 * 1024), folder.newFile()),
            uploader.read(ID_PREFIX + 0)));

        assertUpgrade(pendingUploadsFile);

        assertCacheStats(stagingCache, 0, 0, 2, 2);
    }

    @Test
    public void testUpgradeCompromisedSerializedMap() throws IOException {
        // Close the init setup
        closer.close();

        // Create pre-upgrade load
        File home = folder.newFolder();
        File pendingUploadsFile = new File(home, DataStoreCacheUpgradeUtils.UPLOAD_MAP);
        createGibberishLoad(home, pendingUploadsFile);

        LogCustomizer lc = LogCustomizer.forLogger(DataStoreCacheUpgradeUtils.class.getName())
            .filter(Level.WARN)
            .enable(Level.WARN)
            .create();
        lc.starting();
        // Start
        init(2, new TestStagingUploader(folder.newFolder()), home);
        assertThat(lc.getLogs().toString(), containsString("Error in reading pending uploads map"));
    }

    /** -------------------- Helper methods ----------------------------------------------------**/

    private void createUpgradeLoad(File home, File pendingUploadFile) throws IOException {
        String id = ID_PREFIX + 1;
        copyToFile(randomStream(1, 4 * 1024), getFile(id, root));
        String name = id.substring(0, 2) + "/" + id.substring(2, 4) + "/" + id;
        Map<String, Long> pendingUploads = Maps.newHashMap();
        pendingUploads.put(name, System.currentTimeMillis());
        serializeMap(pendingUploads, pendingUploadFile);
    }


    private void createGibberishLoad(File home, File pendingUploadFile) throws IOException {
        BufferedWriter writer = null;
        try {
            writer = Files.newWriter(pendingUploadFile, Charsets.UTF_8);
            FileIOUtils.writeAsLine(writer, "jerhgiuheirghoeoorqehgsjlwjpfkkwpkf", false);
        } finally {
            Closeables.close(writer, true);
        }
    }

    private void assertUpgrade(File pendingUploadFile) throws IOException {
        assertNull(stagingCache.getIfPresent(ID_PREFIX + 1));
        assertTrue(Files.equal(copyToFile(randomStream(1, 4 * 1024), folder.newFile()),
            uploader.read(ID_PREFIX + 1)));
        assertFalse(pendingUploadFile.exists());
    }

    private static SettableFuture<File> copyStreamThread(ListeningExecutorService executor,
        final InputStream fStream, final File temp, final CountDownLatch start) {
        final SettableFuture<File> future = SettableFuture.create();
        executor.submit(new Runnable() {
            @Override public void run() {
                try {
                    LOG.info("Waiting for start of copying");
                    start.await();
                    LOG.info("Starting copy of [{}]", temp);
                    FileUtils.copyInputStreamToFile(fStream, temp);
                    LOG.info("Finished retrieve");
                    future.set(temp);
                } catch (Exception e) {
                    LOG.info("Exception in get", e);
                }
            }
        });
        return future;
    }

    private static SettableFuture<Optional<SettableFuture<Integer>>> putThread(
        ListeningExecutorService executor, final int seed, final File f, final UploadStagingCache cache,
        final CountDownLatch start, final CountDownLatch trigger) {
        final SettableFuture<Optional<SettableFuture<Integer>>> future = SettableFuture.create();
        executor.submit(new Runnable() {
            @Override public void run() {
                try {
                    LOG.info("Waiting for start to put");
                    start.await();
                    LOG.info("Starting put");
                    trigger.countDown();
                    Optional<SettableFuture<Integer>> opt = cache.put(ID_PREFIX + seed, f);
                    LOG.info("Finished put");
                    future.set(opt);
                } catch (Exception e) {
                    LOG.info("Exception in get", e);
                }
            }
        });
        return future;
    }

    private void waitFinish(List<ListenableFuture<Integer>> futures) {
        ListenableFuture<List<Integer>> listListenableFuture = Futures.successfulAsList(futures);
        try {
            listListenableFuture.get();
            ScheduledFuture<?> scheduledFuture =
                removeExecutor.schedule(stagingCache.new RemoveJob(), 0, TimeUnit.MILLISECONDS);
            scheduledFuture.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<ListenableFuture<Integer>> put(TemporaryFolder folder)
        throws IOException {
        File f = copyToFile(randomStream(0, 4 * 1024), folder.newFile());
        Optional<SettableFuture<Integer>> future = stagingCache.put(ID_PREFIX + 0, f);
        List<ListenableFuture<Integer>> futures = Lists.newArrayList();
        if (future.isPresent()) {
            futures.add(future.get());
        }
        return futures;
    }

    private void assertFuture(List<ListenableFuture<Integer>> futures, int... seeds)
        throws Exception {
        waitFinish(futures);

        for (int i = 0; i < seeds.length; i++) {
            File upload = uploader.read(ID_PREFIX + seeds[i]);
            assertFile(upload, seeds[i], folder);
        }
    }

    private void assertFile(File f, int seed, TemporaryFolder folder) throws IOException {
        assertTrue(f.exists());
        File temp = copyToFile(randomStream(seed, 4 * 1024), folder.newFile());
        assertTrue("Uploaded file content differs", FileUtils.contentEquals(temp, f));
    }

    private static void assertCacheStats(UploadStagingCache cache, long elems, long weight,
        long hits, long count) {
        assertEquals(elems, cache.getStats().getElementCount());
        assertEquals(weight, cache.getStats().estimateCurrentWeight());
        assertEquals(hits, cache.getStats().getHitCount());
        assertEquals(count, cache.getStats().getRequestCount());
    }
}
