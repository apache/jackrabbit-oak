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
package org.apache.jackrabbit.oak.plugins.blob.datastore;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import org.apache.jackrabbit.guava.common.collect.Maps;
import org.apache.jackrabbit.guava.common.util.concurrent.Futures;
import org.apache.jackrabbit.guava.common.util.concurrent.ListenableFuture;
import org.apache.jackrabbit.guava.common.util.concurrent.ListeningExecutorService;
import org.apache.jackrabbit.guava.common.util.concurrent.MoreExecutors;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.core.data.util.NamedThreadFactory;
import org.apache.jackrabbit.oak.commons.FileIOUtils;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils.randomStream;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FSBackendIT {

    protected static final Logger LOG = LoggerFactory.getLogger(FSBackendIT.class);

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private Properties props;
    private FSBackend backend;
    private String dataStoreDir;
    private DataStore ds;
    private ListeningExecutorService executor;
    private Random rand = new Random(0);

    @Before
    public void setUp() throws Exception {
        dataStoreDir = folder.newFolder().getAbsolutePath();
        props = new Properties();
        props.setProperty("cacheSize", "0");
        props.setProperty("fsBackendPath", dataStoreDir);
        ds = createDataStore();
        backend = (FSBackend) ((CachingFileDataStore) ds).getBackend();
        this.executor = MoreExecutors.listeningDecorator(Executors
            .newFixedThreadPool(25, new NamedThreadFactory("oak-backend-test-write-thread")));
    }

    protected DataStore createDataStore() {
        CachingFileDataStore ds = null;
        try {
            ds = new CachingFileDataStore();
            Map<String, ?> config = DataStoreUtils.getConfig();
            props.putAll(config);
            PropertiesUtil.populate(ds, Maps.fromProperties(props), false);
            ds.setProperties(props);
            ds.init(dataStoreDir);
        } catch (Exception e) {
            LOG.error("Exception creating DataStore", e);
        }
        return ds;
    }

    /**
     * Test for write single threaded to FSBackend.
     */
    @Test
    public void testSingleThreadFSBackend() {
        try {
            long start = System.currentTimeMillis();
            LOG.info("Testcase: " + this.getClass().getName() + "#testSingleThread, testDir=" + dataStoreDir);
            doTest(ds, 1, true);
            LOG.info("Testcase: " + this.getClass().getName() + "#testSingleThread finished, time taken = [" + (
                System.currentTimeMillis() - start) + "]ms");
        } catch (Exception e) {
            LOG.error("error", e);
            fail(e.getMessage());
        }
    }

    /**
     * Tests for multi-threaded write of same file to FSBackend
     */
    @Test
    public void testMultiThreadedSame() {
        try {
            long start = System.currentTimeMillis();
            LOG.info("Testcase: " + this.getClass().getName() + "#testMultiThreadedSame, testDir=" + dataStoreDir);
            doTest(ds, 10, true);
            LOG.info("Testcase: " + this.getClass().getName() + "#testMultiThreadedSame finished, time taken = [" + (
                System.currentTimeMillis() - start) + "]ms");
        } catch (Exception e) {
            LOG.error("error", e);
            fail(e.getMessage());
        }
    }

    /**
     * Tests for multi-threaded write of same file to FSBackend
     */
    @Test
    public void testMultiThreadedSameLarge() {
        try {
            long start = System.currentTimeMillis();
            LOG.info("Testcase: " + this.getClass().getName() + "#testMultiThreadedSameLarge, testDir=" + dataStoreDir);
            doTest(ds, 100, true);
            LOG.info("Testcase: " + this.getClass().getName() + "#testMultiThreadedSameLarge finished, time taken = [" + (
                System.currentTimeMillis() - start) + "]ms");
        } catch (Exception e) {
            LOG.error("error", e);
            fail(e.getMessage());
        }
    }

    /**
     * Tests for multi-threaded write of different file to FSBackend
     */
    @Test
    public void testMultiThreadedDifferent() {
        try {
            long start = System.currentTimeMillis();
            LOG.info("Testcase: " + this.getClass().getName() + "#testMultiThreadedDifferent, testDir=" + dataStoreDir);
            doTest(ds, 10, false);
            LOG.info("Testcase: " + this.getClass().getName() + "#testMultiThreadedDifferent finished, time taken = [" + (
                System.currentTimeMillis() - start) + "]ms");
        } catch (Exception e) {
            LOG.error("error", e);
            fail(e.getMessage());
        }
    }

    /**
     * Tests for multi-threaded write of different file to FSBackend
     */
    @Test
    public void testMultiThreadedDifferentLarge() {
        try {
            long start = System.currentTimeMillis();
            LOG.info("Testcase: " + this.getClass().getName() + "#testMultiThreadedDifferentLarge, testDir=" + dataStoreDir);
            doTest(ds, 100, false);
            LOG.info("Testcase: " + this.getClass().getName() + "#testMultiThreadedDifferentLarge finished, time taken = [" + (
                System.currentTimeMillis() - start) + "]ms");
        } catch (Exception e) {
            LOG.error("error", e);
            fail(e.getMessage());
        }
    }

    @After
    public void tearDown() {
        try {
            new ExecutorCloser(executor).close();
            ds.close();
        } catch (DataStoreException e) {
            LOG.error("error", e);
            fail(e.getMessage());
        }
    }

    /**
     * Method to assert record while writing and deleting record from FSBackend
     */
    void doTest(DataStore ds, int concurrency, boolean same) throws Exception {
        List<ListenableFuture<Integer>> futures = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(concurrency);

        int seed = 0;
        for (int i = 0; i < concurrency; i++) {
            if (!same) {
                seed = rand.nextInt(1000);
            }
            put(folder, futures, seed, latch);
        }

        for (int i = 0; i < concurrency; i++) {
            latch.countDown();
        }

        assertFuture(futures);
    }

    private List<ListenableFuture<Integer>> put(TemporaryFolder folder, List<ListenableFuture<Integer>> futures,
        int seed, CountDownLatch writeLatch)
        throws IOException {

        File f = copyToFile(randomStream(seed, 4 * 1024 * 1024), folder.newFile());

        ListenableFuture<Integer> future = executor.submit(() -> {
            try {
                writeLatch.await();
                backend.write(new DataIdentifier("0000ID" + seed), f);
                LOG.info("Added file to backend");
            } catch (Exception e) {
                LOG.error("Error adding file to backend", e);
            }
            return seed;
        });
        futures.add(future);

        return futures;
    }

    private void waitFinish(List<ListenableFuture<Integer>> futures) {
        ListenableFuture<List<Integer>> listenableFutures = Futures.successfulAsList(futures);
        try {
            listenableFutures.get();
        } catch (Exception e) {
            LOG.error("Error in finishing threads", e);
        }
    }

    private void assertFuture(List<ListenableFuture<Integer>> futures) throws Exception {
        waitFinish(futures);

        for (ListenableFuture future : futures) {
            assertFile((Integer) future.get(), folder);
        }
    }

    private void assertFile(int seed, TemporaryFolder folder) throws IOException, DataStoreException {
        DataRecord backendRecord = backend.getRecord(new DataIdentifier("0000ID" + seed));

        assertEquals(backendRecord.getLength(), 4 * 1024 * 1024);
        File original = copyToFile(randomStream(seed, 4 * 1024 * 1024), folder.newFile());
        assertTrue("Backend file content differs",
            FileUtils.contentEquals(original, copyToFile(backendRecord.getStream(), folder.newFile())));
    }

    static File copyToFile(InputStream stream, File file) throws IOException {
        FileIOUtils.copyInputStreamToFile(stream, file);
        return file;
    }
}
