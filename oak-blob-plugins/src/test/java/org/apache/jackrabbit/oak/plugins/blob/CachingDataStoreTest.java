/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.blob;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterators;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.commons.concurrent.ExecutorCloser;
import org.apache.jackrabbit.oak.spi.blob.AbstractSharedBackend;
import org.apache.jackrabbit.oak.spi.blob.BlobOptions;
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
import static org.apache.commons.codec.binary.Hex.encodeHexString;
import static org.apache.jackrabbit.oak.spi.blob.BlobOptions.UploadType.SYNCHRONOUS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link AbstractSharedCachingDataStore}
 */
public class CachingDataStoreTest extends AbstractDataStoreCacheTest {
    private static final Logger LOG = LoggerFactory.getLogger(CachingDataStoreTest.class);
    private static final String ID_PREFIX = "12345";

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    private final Closer closer = Closer.create();
    private File root;

    private CountDownLatch taskLatch;
    private CountDownLatch callbackLatch;
    private CountDownLatch afterExecuteLatch;
    private ScheduledExecutorService scheduledExecutor;
    private AbstractSharedCachingDataStore dataStore;
    private TestMemoryBackend backend;

    @Before
    public void setup() throws Exception {
        root = folder.newFolder();
        init(1, 64 * 1024 * 1024, 10);
    }

    private void init(int i, int cacheSize, int uploadSplit) throws Exception {
        LOG.info("Starting init");

        // create executor
        taskLatch = new CountDownLatch(1);
        callbackLatch = new CountDownLatch(1);
        afterExecuteLatch = new CountDownLatch(i);
        TestExecutor listeningExecutor = new TestExecutor(1, taskLatch, callbackLatch, afterExecuteLatch);

        // stats
        ScheduledExecutorService statsExecutor = Executors.newSingleThreadScheduledExecutor();
        closer.register(new ExecutorCloser(statsExecutor, 500, TimeUnit.MILLISECONDS));
        StatisticsProvider statsProvider = new DefaultStatisticsProvider(statsExecutor);

        scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        closer.register(new ExecutorCloser(scheduledExecutor, 500, TimeUnit.MILLISECONDS));

        final File datastoreRoot = folder.newFolder();
        final TestMemoryBackend testBackend = new TestMemoryBackend(datastoreRoot);
        this.backend = testBackend;

        dataStore = new AbstractSharedCachingDataStore() {
            @Override protected AbstractSharedBackend createBackend() {
                return testBackend;
            }

            @Override public int getMinRecordLength() {
                return 0;
            }
        };
        dataStore.setStatisticsProvider(statsProvider);
        dataStore.setCacheSize(cacheSize);
        dataStore.setStagingSplitPercentage(uploadSplit);
        dataStore.listeningExecutor = listeningExecutor;
        dataStore.schedulerExecutor = scheduledExecutor;
        dataStore.executor = sameThreadExecutor();
        dataStore.init(root.getAbsolutePath());

        LOG.info("Finished init");
    }

    /**
     * Add, get, delete when zero cache size.
     * @throws Exception
     */
    @Test
    public void zeroCacheAddGetDelete() throws Exception {
        LOG.info("Starting zeroCacheAddGetDelete");

        dataStore.close();
        init(1, 0, 0);
        File f = copyToFile(randomStream(0, 4 * 1024), folder.newFile());
        String id = getIdForInputStream(f);
        FileInputStream fin = new FileInputStream(f);
        closer.register(fin);

        DataRecord rec = dataStore.addRecord(fin);
        assertEquals(id, rec.getIdentifier().toString());
        assertFile(rec.getStream(), f, folder);

        rec = dataStore.getRecordIfStored(new DataIdentifier(id));
        assertEquals(id, rec.getIdentifier().toString());
        assertFile(rec.getStream(), f, folder);

        assertEquals(1, Iterators.size(dataStore.getAllIdentifiers()));

        dataStore.deleteRecord(new DataIdentifier(id));
        rec = dataStore.getRecordIfStored(new DataIdentifier(id));
        assertNull(rec);

        LOG.info("Finished zeroCacheAddGetDelete");
    }

    /**
     * Add, get, delete when staging cache is 0.
     * @throws Exception
     */
    @Test
    public void zeroStagingCacheAddGetDelete() throws Exception {
        LOG.info("Starting zeroStagingCacheAddGetDelete");

        dataStore.close();
        init(1, 64 * 1024 * 1024, 0);
        File f = copyToFile(randomStream(0, 4 * 1024), folder.newFile());
        String id = getIdForInputStream(f);
        FileInputStream fin = new FileInputStream(f);
        closer.register(fin);

        DataRecord rec = dataStore.addRecord(fin);
        assertEquals(id, rec.getIdentifier().toString());
        assertFile(rec.getStream(), f, folder);

        rec = dataStore.getRecordIfStored(new DataIdentifier(id));
        assertEquals(id, rec.getIdentifier().toString());
        assertFile(rec.getStream(), f, folder);

        assertEquals(1, Iterators.size(dataStore.getAllIdentifiers()));

        dataStore.deleteRecord(new DataIdentifier(id));
        rec = dataStore.getRecordIfStored(new DataIdentifier(id));
        assertNull(rec);

        LOG.info("Finished zeroStagingCacheAddGetDelete");
    }

    /**
     * Add, get, delete with synchronous option.
     * @throws Exception
     */
    @Test
    public void syncAddGetDelete() throws Exception {
        LOG.info("Starting syncAddGetDelete");

        File f = copyToFile(randomStream(0, 4 * 1024), folder.newFile());
        String id = getIdForInputStream(f);
        FileInputStream fin = new FileInputStream(f);
        closer.register(fin);

        DataRecord rec = dataStore.addRecord(fin, new BlobOptions().setUpload(SYNCHRONOUS));
        assertEquals(id, rec.getIdentifier().toString());
        assertFile(rec.getStream(), f, folder);

        rec = dataStore.getRecordIfStored(new DataIdentifier(id));
        assertEquals(id, rec.getIdentifier().toString());
        assertFile(rec.getStream(), f, folder);

        assertEquals(1, Iterators.size(dataStore.getAllIdentifiers()));

        dataStore.deleteRecord(new DataIdentifier(id));
        rec = dataStore.getRecordIfStored(new DataIdentifier(id));
        assertNull(rec);

        LOG.info("Finished syncAddGetDelete");
    }

    /**
     * Add, get forcing load in cache.
     * @throws Exception
     */
    @Test
    public void syncAddGetLoadCache() throws Exception {
        LOG.info("Starting syncAddGetForceFromCache");

        File f = copyToFile(randomStream(0, 4 * 1024), folder.newFile());
        String id = getIdForInputStream(f);
        FileInputStream fin = new FileInputStream(f);
        closer.register(fin);

        DataRecord rec = dataStore.addRecord(fin, new BlobOptions().setUpload(SYNCHRONOUS));
        assertEquals(id, rec.getIdentifier().toString());
        assertFile(rec.getStream(), f, folder);

        // Invalidate from the local cache
        dataStore.getCache().invalidate(id);

        // Trigger load from backend
        File cacheDownloaded = dataStore.getCache().get(id);
        assertTrue(Files.equal(f, cacheDownloaded));

        assertEquals(1, Iterators.size(dataStore.getAllIdentifiers()));

        LOG.info("Finished syncAddGetLoadCache");
    }

    /**
     * {@link CompositeDataStoreCache#getIfPresent(String)} when no record.
     */
    @Test
    public void getRecordNotAvailable() throws DataStoreException {
        LOG.info("Starting getRecordNotAvailable");

        DataRecord rec = dataStore.getRecordIfStored(new DataIdentifier(ID_PREFIX + 0));
        assertNull(rec);

        LOG.info("Finished getRecordNotAvailable");
    }

    /**
     * Add in datastore, invalidate from cache and lazy load record stream.
     */
    @Test
    public void lazyLoadStream() throws Exception {
        LOG.info("Starting lazyLoadStream");

        File f = copyToFile(randomStream(0, 4 * 1024), folder.newFile());
        String id = getIdForInputStream(f);
        FileInputStream fin = new FileInputStream(f);
        closer.register(fin);

        DataRecord rec = dataStore.addRecord(fin);
        assertEquals(id, rec.getIdentifier().toString());

        //start & finish
        taskLatch.countDown();
        callbackLatch.countDown();
        waitFinish();

        // Invalidate from the local cache
        dataStore.getCache().invalidate(id);

        // retrieve record from the datastore
        rec = dataStore.getRecordIfStored(new DataIdentifier(id));
        assertNotNull(rec);
        assertEquals(id, rec.getIdentifier().toString());

        // the file should not be in cache
        File cached = dataStore.getCache().getIfPresent(id);
        assertNull(cached);

        // assert stream
        assertFile(rec.getStream(), f, folder);

        // Now should be available in the cache
        cached = dataStore.getCache().getIfPresent(id);
        assertNotNull(cached);
        assertTrue(Files.equal(f, cached));

        dataStore.deleteRecord(new DataIdentifier(id));
        rec = dataStore.getRecordIfStored(new DataIdentifier(id));
        assertNull(rec);

        LOG.info("Finished lazyLoadStream");
    }

    /**
     * {@link CompositeDataStoreCache#get(String)} when no cache.
     * @throws IOException
     */
    @Test
    public void exists() throws IOException {
        LOG.info("Starting exists");

        assertFalse(dataStore.exists(new DataIdentifier(ID_PREFIX + 0)));

        LOG.info("Finished exists");
    }

    /**
     * Add in datastore.
     */
    @Test
    public void addDelete() throws Exception {
        LOG.info("Starting addDelete");

        File f = copyToFile(randomStream(0, 4 * 1024), folder.newFile());
        String id = getIdForInputStream(f);
        FileInputStream fin = new FileInputStream(f);
        closer.register(fin);

        DataRecord rec = dataStore.addRecord(fin);
        assertEquals(id, rec.getIdentifier().toString());

        //start & finish
        taskLatch.countDown();
        callbackLatch.countDown();
        waitFinish();

        rec = dataStore.getRecordIfStored(new DataIdentifier(id));
        assertNotNull(rec);
        assertFile(rec.getStream(), f, folder);

        dataStore.deleteRecord(new DataIdentifier(id));
        rec = dataStore.getRecordIfStored(new DataIdentifier(id));
        assertNull(rec);

        LOG.info("Finished addDelete");
    }

    /**
     * Add in staging and delete.
     * @throws Exception
     */
    @Test
    public void addStagingAndDelete() throws Exception {
        LOG.info("Starting addStagingAndDelete");

        File f = copyToFile(randomStream(0, 4 * 1024), folder.newFile());
        String id = getIdForInputStream(f);
        FileInputStream fin = new FileInputStream(f);
        closer.register(fin);

        DataRecord rec = dataStore.addRecord(fin);
        assertEquals(id, rec.getIdentifier().toString());
        assertFile(rec.getStream(), f, folder);

        rec = dataStore.getRecordIfStored(new DataIdentifier(id));
        assertNotNull(rec);
        assertFile(rec.getStream(), f, folder);

        dataStore.deleteRecord(new DataIdentifier(id));
        rec = dataStore.getRecordIfStored(new DataIdentifier(id));
        assertNull(rec);

        Thread.sleep(1000);
        //start & finish
        taskLatch.countDown();
        callbackLatch.countDown();
        waitFinish();

        rec = dataStore.getRecordIfStored(new DataIdentifier(id));
        assertNull(rec);

        LOG.info("Finished addStagingAndDelete");
    }

    /**
     * Get all Identifiers.
     */
    @Test
    public void getAllIdentifiers() throws Exception {
        LOG.info("Starting getAllIdentifiers");

        File f = copyToFile(randomStream(0, 4 * 1024), folder.newFile());
        String id = getIdForInputStream(f);
        FileInputStream fin = new FileInputStream(f);
        closer.register(fin);

        DataRecord rec = dataStore.addRecord(fin);
        assertEquals(id, rec.getIdentifier().toString());

        assertTrue(Iterators.contains(dataStore.getAllIdentifiers(), new DataIdentifier(id)));

        //start & finish
        taskLatch.countDown();
        callbackLatch.countDown();
        waitFinish();

        assertTrue(Iterators.contains(dataStore.getAllIdentifiers(), new DataIdentifier(id)));

        LOG.info("Finished getAllIdentifiers");
    }

    @Test
    public void reference() throws Exception {
        LOG.info("Starting reference");

        File f = copyToFile(randomStream(0, 4 * 1024), folder.newFile());
        String id = getIdForInputStream(f);
        FileInputStream fin = new FileInputStream(f);
        closer.register(fin);

        // Record still in staging
        DataRecord rec = dataStore.addRecord(fin);
        assertEquals(id, rec.getIdentifier().toString());
        assertFile(rec.getStream(), f, folder);
        assertEquals(backend.getReferenceFromIdentifier(rec.getIdentifier()),
            rec.getReference());

        rec = dataStore.getRecordIfStored(new DataIdentifier(id));
        assertNotNull(rec);
        assertFile(rec.getStream(), f, folder);
        assertEquals(backend.getReferenceFromIdentifier(rec.getIdentifier()),
            rec.getReference());

        //start & finish
        taskLatch.countDown();
        callbackLatch.countDown();
        waitFinish();

        // Now record in download cache
        rec = dataStore.getRecordIfStored(new DataIdentifier(id));
        assertNotNull(rec);
        assertFile(rec.getStream(), f, folder);
        assertEquals(backend.getReferenceFromIdentifier(rec.getIdentifier()),
            rec.getReference());

        LOG.info("Finished reference");
    }

    @Test
    public void referenceNoCache() throws Exception {
        LOG.info("Starting referenceNoCache");

        dataStore.close();
        init(1, 0, 0);

        File f = copyToFile(randomStream(0, 4 * 1024), folder.newFile());
        String id = getIdForInputStream(f);
        FileInputStream fin = new FileInputStream(f);
        closer.register(fin);

        // Record still in staging
        DataRecord rec = dataStore.addRecord(fin);
        assertEquals(id, rec.getIdentifier().toString());
        assertFile(rec.getStream(), f, folder);
        assertEquals(backend.getReferenceFromIdentifier(rec.getIdentifier()),
            rec.getReference());

        rec = dataStore.getRecordIfStored(new DataIdentifier(id));
        assertNotNull(rec);
        assertFile(rec.getStream(), f, folder);
        assertEquals(backend.getReferenceFromIdentifier(rec.getIdentifier()),
            rec.getReference());

        LOG.info("Finished referenceNoCache");
    }

    @After
    public void tear() throws Exception {
        closer.close();
        dataStore.close();
    }

    private static void assertFile(InputStream is, File org, TemporaryFolder folder)
        throws IOException {
        try {
            File ret = folder.newFile();
            FileUtils.copyInputStreamToFile(is, ret);
            assertTrue(Files.equal(org, ret));
        } finally {
            IOUtils.closeQuietly(is);
        }
    }

    private String getIdForInputStream(File f)
        throws Exception {
        FileInputStream in = null;
        OutputStream output = null;
        try {
            in = new FileInputStream(f);
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            output = new DigestOutputStream(new NullOutputStream(), digest);
            IOUtils.copyLarge(in, output);
            return encodeHexString(digest.digest());
        } finally {
            IOUtils.closeQuietly(output);
            IOUtils.closeQuietly(in);
        }
    }

    private void waitFinish() {
        try {
            // wait for upload finish
            afterExecuteLatch.await();
            // Force execute removal from staging cache
            ScheduledFuture<?> scheduledFuture = scheduledExecutor
                .schedule(dataStore.getCache().getStagingCache().new RemoveJob(), 0, TimeUnit.MILLISECONDS);
            scheduledFuture.get();
            LOG.info("After jobs completed");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
