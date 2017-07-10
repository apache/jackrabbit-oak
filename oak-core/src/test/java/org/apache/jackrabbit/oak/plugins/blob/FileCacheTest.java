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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.io.Closer;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.jackrabbit.oak.commons.StringUtils;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * - Tests for {@link FileCache}
 */
public class FileCacheTest extends AbstractDataStoreCacheTest {
    private static final String ID_PREFIX = "12345";
    private FileCache cache;
    private File root;
    private TestCacheLoader loader;
    private Closer closer;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Rule
    public TestName testName = new TestName();

    CountDownLatch afterExecuteLatch;
    @Before
    public void setup() throws Exception {
        LOG.info("Started setup");

        root = folder.newFolder();
        closer = Closer.create();
        loader = new TestCacheLoader<String, InputStream>(folder.newFolder());

        CountDownLatch beforeLatch = new CountDownLatch(1);
        CountDownLatch afterLatch = new CountDownLatch(1);
        afterExecuteLatch = new CountDownLatch(1);

        TestExecutor executor = new TestExecutor(1, beforeLatch, afterLatch, afterExecuteLatch);
        beforeLatch.countDown();
        afterLatch.countDown();
        cache = FileCache.build(4 * 1024/* KB */, root, loader, executor);
        Futures.successfulAsList((Iterable<? extends ListenableFuture<?>>) executor.futures).get();

        closer.register(cache);

        LOG.info("Finished setup");
    }

    @After
    public void tear() {
        closeQuietly(closer);
    }

    @Test
    public void zeroCache() throws Exception {
        LOG.info("Started zeroCache");

        cache = FileCache.build(0/* KB */, root, loader, null);
        closer.register(cache);
        File f = createFile(0, loader, cache, folder);
        cache.put(ID_PREFIX + 0, f);
        assertNull(cache.getIfPresent(ID_PREFIX + 0));
        assertNull(cache.get(ID_PREFIX + 0));
        assertEquals(0, cache.getStats().getMaxTotalWeight());
        cache.invalidate(ID_PREFIX + 0);
        assertFalse(cache.containsKey(ID_PREFIX + 0));
        cache.close();

        LOG.info("Finished zeroCache");
    }

    /**
     * Load and get from cache.
     * @throws Exception
     */
    @Test
    public void add() throws Exception {
        LOG.info("Started add");

        File f = createFile(0, loader, cache, folder);
        assertCache(0, cache, f);
        assertCacheStats(cache, 1, 4 * 1024, 1, 1);
        assertEquals("Memory weight different",
            getWeight(ID_PREFIX + 0, cache.getIfPresent(ID_PREFIX + 0)),
            cache.getStats().estimateCurrentMemoryWeight());

        LOG.info("Finished add");
    }

    /**
     * Explicitly put in cache.
     * @throws Exception
     */
    @Test
    public void put() throws Exception {
        LOG.info("Started put");

        //File f = FileIOUtils.copy(randomStream(0, 4 * 1024));
        cache.put(ID_PREFIX + 0, copyToFile(randomStream(0, 4 * 1024), folder.newFile()));
        assertCacheIfPresent(0, cache, copyToFile(randomStream(0, 4 * 1024), folder.newFile()));
        assertCacheStats(cache, 1, 4 * 1024, 0, 0);

        LOG.info("Finished put");
    }

    /**
     * Tests {@link FileCache#getIfPresent(Object)} when no cache.
     */
    @Test
    public void getIfPresentObjectNoCache() {
        LOG.info("Started getIfPresentObjectNoCache");

        File file = cache.getIfPresent((Object) (ID_PREFIX + 0));
        assertNull(file);
        assertCacheStats(cache, 0, 0, 0, 0);
        assertEquals(1, cache.getStats().getMissCount());
        LOG.info("Finished getIfPresentObjectNoCache");
    }

    /**
     * Retrieves same file concurrently.
     * @throws Exception
     */
    @Test
    public void retrieveSameConcurrent() throws Exception {
        LOG.info("Started retrieveSameConcurrent");

        File f = createFile(0, loader, cache, folder);
        ListeningExecutorService executorService =
            MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(2));
        closer.register(new ExecutorCloser(executorService, 5, TimeUnit.MILLISECONDS));

        CountDownLatch thread1Start = new CountDownLatch(1);
        SettableFuture<File> future1 =
            retrieveThread(executorService, ID_PREFIX + 0, cache, thread1Start);

        CountDownLatch thread2Start = new CountDownLatch(1);
        SettableFuture<File> future2 =
            retrieveThread(executorService, ID_PREFIX + 0, cache, thread2Start);

        thread1Start.countDown();
        thread2Start.countDown();

        future1.get();
        future2.get();
        LOG.info("Async tasks finished");

        assertCacheIfPresent(0, cache, f);
        assertCacheStats(cache, 1, 4 * 1024, 1, 1);

        LOG.info("Finished retrieveSameConcurrent");
    }

    /**
     * Retrieves different files concurrently.
     * @throws Exception
     */
    @Test
    public void getDifferentConcurrent() throws Exception {
        LOG.info("Started getDifferentConcurrent");

        File f = createFile(0, loader, cache, folder);
        File f2 = createFile(1, loader, cache, folder);

        ListeningExecutorService executorService =
            MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(2));
        closer.register(new ExecutorCloser(executorService, 5, TimeUnit.MILLISECONDS));

        CountDownLatch thread1Start = new CountDownLatch(1);
        SettableFuture<File> future1 =
            retrieveThread(executorService, ID_PREFIX + 0, cache, thread1Start);

        CountDownLatch thread2Start = new CountDownLatch(1);
        SettableFuture<File> future2 =
            retrieveThread(executorService, ID_PREFIX + 1, cache, thread2Start);

        thread1Start.countDown();
        thread2Start.countDown();

        future1.get();
        future2.get();
        LOG.info("Async tasks finished");

        assertCacheIfPresent(0, cache, f);
        assertCacheIfPresent(1, cache, f2);
        assertCacheStats(cache, 2, 8 * 1024, 2, 2);

        LOG.info("Finished getDifferentConcurrent");
    }

    /**
     * Retrieve and put different files concurrently.
     * @throws Exception
     */
    @Test
    public void retrievePutConcurrent() throws Exception {
        LOG.info("Started retrievePutConcurrent");

        //Create load
        final File f = createFile(0, loader, cache, folder);
        File f2 = copyToFile(randomStream(1, 4 * 1024), folder.newFile());

        ListeningExecutorService executorService =
            MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(2));
        closer.register(new ExecutorCloser(executorService, 5, TimeUnit.MILLISECONDS));

        CountDownLatch thread1Start = new CountDownLatch(1);
        SettableFuture<File> future1 =
            retrieveThread(executorService, ID_PREFIX + 0, cache, thread1Start);

        CountDownLatch thread2Start = new CountDownLatch(1);
        SettableFuture<Boolean> future2 = putThread(executorService, 1, f2, cache, thread2Start);

        thread1Start.countDown();
        thread2Start.countDown();

        future1.get();
        future2.get();
        LOG.info("Async tasks finished");

        assertCacheIfPresent(0, cache, f);
        assertCacheIfPresent(1, cache, copyToFile(randomStream(1, 4 * 1024), folder.newFile()));
        assertCacheStats(cache, 2, 8 * 1024, 1, 1);

        LOG.info("Finished retrievePutConcurrent");
    }

    /**
     * evict explicitly.
     * @throws Exception
     */
    @Test
    public void evictExplicit() throws Exception {
        LOG.info("Started evictExplicit");

        File f = createFile(0, loader, cache, folder);
        assertCache(0, cache, f);

        // trigger explicit invalidate
        cache.invalidate(ID_PREFIX + 0);
        assertFalse(cache.containsKey(ID_PREFIX + 0));
        assertCacheStats(cache, 0, 0, 1, 1);

        LOG.info("Finished evictExplicit");
    }

    /**
     * evict implicitly.
     * @throws Exception
     */
    @Test
    public void evictImplicit() throws Exception {
        LOG.info("Started evictImplicit");

        for (int i = 0; i < 15; i++) {
            File f = createFile(i, loader, cache, folder);
            assertCache(i, cache, f);
        }

        File f = createFile(30, loader, cache, folder);
        assertCache(30, cache, f);
        // One of the entries should have been evicted
        assertTrue(cache.getStats().getElementCount() == 15);
        assertCacheStats(cache, 15, 60 * 1024, 16, 16);

        LOG.info("Finished evictImplicit");
    }

    /**
     * test eviction on replacement.
     * @throws Exception
     */
    @Test
    public void evictReplace() throws Exception {
        LOG.info("Started evictReplace");

        File f = createFile(0, loader, cache, folder);
        assertCache(0, cache, f);

        // Again put in cache to trigger eviction with replacement
        cache.put(ID_PREFIX + 0, f);
        // File should still be present
        assertCache(0, cache, f);

        LOG.info("Finished evictReplace");
    }

    /**
     * Retrieve and invalidate concurrently.
     * @throws Exception
     */
    @Test
    public void getInvalidateConcurrent() throws Exception {
        LOG.info("Started getInvalidateConcurrent");

        //Create load
        for (int i = 0; i < 15; i++) {
            if (i != 4) {
                File f = createFile(i, loader, cache, folder);
                assertCache(i, cache, f);
            }
        }
        LOG.info("Finished creating load");
        ListeningExecutorService executorService =
            MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(2));
        closer.register(new ExecutorCloser(executorService, 5, TimeUnit.MILLISECONDS));

        CountDownLatch thread1Start = new CountDownLatch(1);
        SettableFuture<File> future1 =
            retrieveThread(executorService, ID_PREFIX + 10, cache, thread1Start);
        thread1Start.countDown();

        File f = createFile(4, loader, cache, folder);
        CountDownLatch thread2Start = new CountDownLatch(1);
        SettableFuture<File> future2 =
            retrieveThread(executorService, ID_PREFIX + 4, cache, thread2Start);
        thread2Start.countDown();

        File f10 = future1.get();
        File f4 = future2.get();
        LOG.info("Async tasks finished");

        if (f10.exists()) {
            assertCacheIfPresent(10, cache, f10);
        }
        if (f4.exists()) {
            assertCacheIfPresent(4, cache, f4);
        }
        LOG.info("Finished getInvalidateConcurrent");
    }

    /**
     * Trigger build cache on start.
     * @throws Exception
     */
    @Test
    public void rebuild() throws Exception {
        LOG.info("Started rebuild");
        afterExecuteLatch.await();
        LOG.info("Cache built");

        File f = createFile(0, loader, cache, folder);
        assertCache(0, cache, f);
        cache.close();

        CountDownLatch beforeLatch = new CountDownLatch(1);
        CountDownLatch afterLatch = new CountDownLatch(1);
        afterExecuteLatch = new CountDownLatch(1);

        TestExecutor executor = new TestExecutor(1, beforeLatch, afterLatch, afterExecuteLatch);
        beforeLatch.countDown();
        afterLatch.countDown();
        cache = FileCache.build(4 * 1024/* bytes */, root, loader, executor);
        closer.register(cache);
        afterExecuteLatch.await();
        Futures.successfulAsList((Iterable<? extends ListenableFuture<?>>) executor.futures).get();
        LOG.info("Cache rebuilt");

        assertCacheIfPresent(0, cache, f);
        assertCacheStats(cache, 1, 4 * 1024, 0, 0);

        LOG.info("Finished rebuild");
    }

    /**
     * Trigger upgrade cache on start.
     * @throws Exception
     */
    @Test
    public void upgrade() throws Exception {
        LOG.info("Started upgrade");

        afterExecuteLatch.await();

        File f = createFile(0, loader, cache, folder);
        assertCache(0, cache, f);
        cache.close();

        copyToFile(randomStream(1, 4 * 1024), getFile(ID_PREFIX + 1, root));

        CountDownLatch beforeLatch = new CountDownLatch(1);
        CountDownLatch afterLatch = new CountDownLatch(1);
        afterExecuteLatch = new CountDownLatch(1);

        TestExecutor executor = new TestExecutor(1, beforeLatch, afterLatch, afterExecuteLatch);
        beforeLatch.countDown();
        afterLatch.countDown();
        cache = FileCache.build(4 * 1024/* bytes */, root, loader, executor);
        closer.register(cache);
        afterExecuteLatch.await();
        Futures.successfulAsList((Iterable<? extends ListenableFuture<?>>) executor.futures).get();
        LOG.info("Cache rebuilt");

        assertCacheIfPresent(0, cache, f);
        assertCacheIfPresent(1, cache, copyToFile(randomStream(1, 4 * 1024), folder.newFile()));
        assertFalse(getFile(ID_PREFIX + 1, root).exists());

        assertCacheStats(cache, 2, 8 * 1024, 0, 0);

        LOG.info("Finished upgrade");
    }

    /**------------------------------ Helper methods --------------------------------------------**/

    private static SettableFuture<File> retrieveThread(ListeningExecutorService executor,
        final String id, final FileCache cache, final CountDownLatch start) {
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
                }
            }
        });
        return future;
    }



    private static SettableFuture<Boolean> putThread(ListeningExecutorService executor,
        final int seed, final File f, final FileCache cache, final CountDownLatch start) {
        final SettableFuture<Boolean> future = SettableFuture.create();
        executor.submit(new Runnable() {
            @Override public void run() {
                try {
                    LOG.info("Waiting for start to put");
                    start.await();
                    LOG.info("Starting put");
                    cache.put(ID_PREFIX + seed, f);
                    LOG.info("Finished put");
                    future.set(true);
                } catch (Exception e) {
                    LOG.info("Exception in get", e);
                }
            }
        });
        return future;
    }

    private static int getWeight(String key, File value) {
        return StringUtils.estimateMemoryUsage(key) +
            StringUtils.estimateMemoryUsage(value.getAbsolutePath()) + 48;
    }

    private static void assertCacheIfPresent(int seed, FileCache cache, File f) throws IOException {
        File cached = cache.getIfPresent(ID_PREFIX + seed);
        assertNotNull(cached);
        assertTrue(Files.equal(f, cached));
    }

    private static void assertCache(int seed, FileCache cache, File f) throws IOException {
        File cached = cache.get(ID_PREFIX + seed);
        assertNotNull(cached);
        assertTrue(Files.equal(f, cached));
    }

    private static File createFile(int seed, TestCacheLoader loader, FileCache cache,
        TemporaryFolder folder) throws Exception {
        File f = copyToFile(randomStream(0, 4 * 1024),
            folder.newFile());
        loader.write(ID_PREFIX + seed, f);
        assertNull(cache.getIfPresent(ID_PREFIX + seed));
        return f;
    }

    private static void assertCacheStats(FileCache cache, long elems, long weight, long loads,
        long loadSuccesses) {
        assertEquals(elems, cache.getStats().getElementCount());
        assertEquals(weight, cache.getStats().estimateCurrentWeight());
        assertEquals(loads, cache.getStats().getLoadCount());
        assertEquals(loadSuccesses, cache.getStats().getLoadSuccessCount());
    }
}
