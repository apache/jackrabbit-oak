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
package org.apache.jackrabbit.core.data;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Testcase to test local cache.
 */
public class TestLocalCache extends TestCase {

    private static final String CACHE_DIR = "target/cache";

    private static final String TEMP_DIR = "target/temp";

    private static final String TARGET_DIR = "target";

    protected String cacheDirPath;

    protected String tempDirPath;

    /**
     * Random number generator to populate data
     */
    protected Random randomGen = new Random();

    private static final Logger LOG = LoggerFactory.getLogger(TestLocalCache.class);

    @Override
    protected void setUp() {
        try {
            cacheDirPath = CACHE_DIR + "-"
                + String.valueOf(randomGen.nextInt(9999)) + "-"
                + String.valueOf(randomGen.nextInt(9999));
            File cachedir = new File(cacheDirPath);
            for (int i = 0; i < 4 && cachedir.exists(); i++) {
                FileUtils.deleteQuietly(cachedir);
                Thread.sleep(1000);
            }
            cachedir.mkdirs();

            tempDirPath = TEMP_DIR + "-"
                + String.valueOf(randomGen.nextInt(9999)) + "-"
                + String.valueOf(randomGen.nextInt(9999));
            File tempdir = new File(tempDirPath);
            for (int i = 0; i < 4 && tempdir.exists(); i++) {
                FileUtils.deleteQuietly(tempdir);
                Thread.sleep(1000);
            }
            tempdir.mkdirs();
        } catch (Exception e) {
            LOG.error("error:", e);
            fail();
        }
    }

    @Override
    protected void tearDown() throws Exception {
        File cachedir = new File(cacheDirPath);
        for (int i = 0; i < 4 && cachedir.exists(); i++) {
            FileUtils.deleteQuietly(cachedir);
            Thread.sleep(1000);
        }

        File tempdir = new File(tempDirPath);
        for (int i = 0; i < 4 && tempdir.exists(); i++) {
            FileUtils.deleteQuietly(tempdir);
            Thread.sleep(1000);
        }
    }

    /**
     * Test to validate store retrieve in cache.
     */
    public void testStoreRetrieve() {
        try {
            AsyncUploadCache pendingFiles = new AsyncUploadCache();
            pendingFiles.init(tempDirPath, cacheDirPath, 100);
            pendingFiles.reset();
            LocalCache cache = new LocalCache(cacheDirPath, tempDirPath, 400,
                0.95, 0.70, pendingFiles);
            Random random = new Random(12345);
            byte[] data = new byte[100];
            Map<String, byte[]> byteMap = new HashMap<String, byte[]>();
            random.nextBytes(data);
            byteMap.put("a1", data);

            data = new byte[100];
            random.nextBytes(data);
            byteMap.put("a2", data);

            data = new byte[100];
            random.nextBytes(data);
            byteMap.put("a3", data);

            cache.store("a1", new ByteArrayInputStream(byteMap.get("a1")));
            cache.store("a2", new ByteArrayInputStream(byteMap.get("a2")));
            cache.store("a3", new ByteArrayInputStream(byteMap.get("a3")));
            InputStream result = cache.getIfStored("a1");
            assertEquals(new ByteArrayInputStream(byteMap.get("a1")), result);
            IOUtils.closeQuietly(result);
            result = cache.getIfStored("a2");
            assertEquals(new ByteArrayInputStream(byteMap.get("a2")), result);
            IOUtils.closeQuietly(result);
            result = cache.getIfStored("a3");
            assertEquals(new ByteArrayInputStream(byteMap.get("a3")), result);
            IOUtils.closeQuietly(result);
        } catch (Exception e) {
            LOG.error("error:", e);
            fail();
        }
    }

    /**
     * Test to verify cache's purging if cache current size exceeds
     * cachePurgeTrigFactor * size.
     */
    public void testAutoPurge() {
        try {
            AsyncUploadCache pendingFiles = new AsyncUploadCache();
            pendingFiles.init(tempDirPath, cacheDirPath, 100);
            pendingFiles.reset();
            LocalCache cache = new LocalCache(cacheDirPath, tempDirPath, 400,
                0.95, 0.70, pendingFiles);
            Random random = new Random(12345);
            byte[] data = new byte[100];
            Map<String, byte[]> byteMap = new HashMap<String, byte[]>();
            random.nextBytes(data);
            byteMap.put("a1", data);

            data = new byte[100];
            random.nextBytes(data);
            byteMap.put("a2", data);

            data = new byte[100];
            random.nextBytes(data);
            byteMap.put("a3", data);

            data = new byte[100];
            random.nextBytes(data);
            byteMap.put("a4", data);

            cache.store("a1", new ByteArrayInputStream(byteMap.get("a1")));
            cache.store("a2", new ByteArrayInputStream(byteMap.get("a2")));
            cache.store("a3", new ByteArrayInputStream(byteMap.get("a3")));

            InputStream result = cache.getIfStored("a1");
            assertEquals(new ByteArrayInputStream(byteMap.get("a1")), result);
            IOUtils.closeQuietly(result);
            result = cache.getIfStored("a2");
            assertEquals(new ByteArrayInputStream(byteMap.get("a2")), result);
            IOUtils.closeQuietly(result);
            result = cache.getIfStored("a3");
            assertEquals(new ByteArrayInputStream(byteMap.get("a3")), result);
            IOUtils.closeQuietly(result);

            data = new byte[90];
            random.nextBytes(data);
            byteMap.put("a4", data);
            // storing a4 should purge cache
            cache.store("a4", new ByteArrayInputStream(byteMap.get("a4")));
            do {
                Thread.sleep(1000);
            } while (cache.isInPurgeMode());

            result = cache.getIfStored("a1");
            assertNull("a1 should be null", result);
            IOUtils.closeQuietly(result);

            result = cache.getIfStored("a2");
            assertNull("a2 should be null", result);
            IOUtils.closeQuietly(result);

            result = cache.getIfStored("a3");
            assertEquals(new ByteArrayInputStream(byteMap.get("a3")), result);
            IOUtils.closeQuietly(result);

            result = cache.getIfStored("a4");
            assertEquals(new ByteArrayInputStream(byteMap.get("a4")), result);
            IOUtils.closeQuietly(result);

            data = new byte[100];
            random.nextBytes(data);
            byteMap.put("a5", data);
            cache.store("a5", new ByteArrayInputStream(byteMap.get("a5")));
            result = cache.getIfStored("a3");
            assertEquals(new ByteArrayInputStream(byteMap.get("a3")), result);
            IOUtils.closeQuietly(result);
        } catch (Exception e) {
            LOG.error("error:", e);
            fail();
        }
    }

    /**
     * Test to verify cache's purging if cache current size exceeds
     * cachePurgeTrigFactor * size.
     */
    public void testAutoPurgeWithPendingUpload() {
        try {
            AsyncUploadCache pendingFiles = new AsyncUploadCache();
            pendingFiles.init(tempDirPath, cacheDirPath, 100);
            pendingFiles.reset();
            LocalCache cache = new LocalCache(cacheDirPath, tempDirPath, 400,
                0.95, 0.70, pendingFiles);
            Random random = new Random(12345);
            byte[] data = new byte[125];
            Map<String, byte[]> byteMap = new HashMap<String, byte[]>();
            random.nextBytes(data);
            byteMap.put("a1", data);

            data = new byte[125];
            random.nextBytes(data);
            byteMap.put("a2", data);

            data = new byte[125];
            random.nextBytes(data);
            byteMap.put("a3", data);

            data = new byte[100];
            random.nextBytes(data);
            byteMap.put("a4", data);
            File tempDir = new File(tempDirPath);
            File f = File.createTempFile("test", "tmp", tempDir);
            FileOutputStream fos = new FileOutputStream(f);
            fos.write(byteMap.get("a1"));
            fos.close();
            AsyncUploadCacheResult result = cache.store("a1", f, true);
            assertTrue("should be able to add to pending upload",
                result.canAsyncUpload());

            f = File.createTempFile("test", "tmp", tempDir);
            fos = new FileOutputStream(f);
            fos.write(byteMap.get("a2"));
            fos.close();
            result = cache.store("a2", f, true);
            assertTrue("should be able to add to pending upload",
                result.canAsyncUpload());

            f = File.createTempFile("test", "tmp", tempDir);
            fos = new FileOutputStream(f);
            fos.write(byteMap.get("a3"));
            fos.close();
            result = cache.store("a3", f, true);
            assertTrue("should be able to add to pending upload",
                result.canAsyncUpload());

            InputStream inp = cache.getIfStored("a1");
            assertEquals(new ByteArrayInputStream(byteMap.get("a1")), inp);
            IOUtils.closeQuietly(inp);
            inp = cache.getIfStored("a2");
            assertEquals(new ByteArrayInputStream(byteMap.get("a2")), inp);
            IOUtils.closeQuietly(inp);
            inp = cache.getIfStored("a3");
            assertEquals(new ByteArrayInputStream(byteMap.get("a3")), inp);
            IOUtils.closeQuietly(inp);

            data = new byte[90];
            random.nextBytes(data);
            byteMap.put("a4", data);

            f = File.createTempFile("test", "tmp", tempDir);
            fos = new FileOutputStream(f);
            fos.write(byteMap.get("a4"));
            fos.close();

            result = cache.store("a4", f, true);
            assertFalse("should not be able to add to pending upload",
                result.canAsyncUpload());
            Thread.sleep(1000);

            inp = cache.getIfStored("a1");
            assertEquals(new ByteArrayInputStream(byteMap.get("a1")), inp);
            IOUtils.closeQuietly(inp);
            inp = cache.getIfStored("a2");
            assertEquals(new ByteArrayInputStream(byteMap.get("a2")), inp);
            IOUtils.closeQuietly(inp);
            inp = cache.getIfStored("a3");
            assertEquals(new ByteArrayInputStream(byteMap.get("a3")), inp);
            IOUtils.closeQuietly(inp);
            inp = cache.getIfStored("a4");
            assertNull("a4 should be null", inp);
        } catch (Exception e) {
            LOG.error("error:", e);
            fail();
        }
    }

    /**
     * Test concurrent {@link LocalCache} initialization with storing
     * {@link LocalCache}
     */
    public void testConcurrentInitWithStore() {
        try {
            AsyncUploadCache pendingFiles = new AsyncUploadCache();
            pendingFiles.init(tempDirPath, cacheDirPath, 100);
            pendingFiles.reset();
            LocalCache cache = new LocalCache(cacheDirPath, tempDirPath,
                10000000, 0.95, 0.70, pendingFiles);
            Random random = new Random(12345);
            int fileUploads = 1000;
            Map<String, byte[]> byteMap = new HashMap<String, byte[]>(
                fileUploads);
            byte[] data;
            for (int i = 0; i < fileUploads; i++) {
                data = new byte[100];
                random.nextBytes(data);
                String key = "a" + i;
                byteMap.put(key, data);
                cache.store(key, new ByteArrayInputStream(byteMap.get(key)));
            }
            cache.close();

            ExecutorService executor = Executors.newFixedThreadPool(10,
                new NamedThreadFactory("localcache-store-worker"));
            cache = new LocalCache(cacheDirPath, tempDirPath, 10000000, 0.95,
                0.70, pendingFiles);
            executor.execute(new StoreWorker(cache, byteMap));
            executor.shutdown();
            while (!executor.awaitTermination(15, TimeUnit.SECONDS)) {
            }
        } catch (Exception e) {
            LOG.error("error:", e);
            fail();
        }
    }

    private class StoreWorker implements Runnable {
        Map<String, byte[]> byteMap;

        LocalCache cache;

        Random random;

        private StoreWorker(LocalCache cache, Map<String, byte[]> byteMap) {
            this.byteMap = byteMap;
            this.cache = cache;
            random = new Random(byteMap.size());
        }

        public void run() {
            try {
                for (int i = 0; i < 100; i++) {
                    String key = "a" + random.nextInt(byteMap.size());
                    LOG.debug("key=" + key);
                    cache.store(key, new ByteArrayInputStream(byteMap.get(key)));
                }
            } catch (Exception e) {
                LOG.error("error:", e);
                fail();
            }
        }
    }

    /**
     * Assert two inputstream
     */
    protected void assertEquals(InputStream a, InputStream b)
                    throws IOException {
        while (true) {
            int ai = a.read();
            int bi = b.read();
            assertEquals(ai, bi);
            if (ai < 0) {
                break;
            }
        }
        IOUtils.closeQuietly(a);
        IOUtils.closeQuietly(b);
    }

}
