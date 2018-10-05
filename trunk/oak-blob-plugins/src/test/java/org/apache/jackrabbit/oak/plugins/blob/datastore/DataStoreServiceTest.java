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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.jcr.RepositoryException;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.FileDataStore;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.spi.blob.SharedBackend;
import org.apache.jackrabbit.oak.spi.blob.stats.BlobStoreStatsMBean;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DataStoreServiceTest {
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Rule
    public final OsgiContext context = new OsgiContext();

    @Test
    public void mbeanRegs() throws Exception{
        Map<String, Object> config = ImmutableMap.<String, Object>of(
                "repository.home", folder.getRoot().getAbsolutePath()
        );

        FileDataStoreService fds = new FileDataStoreService();
        fds.setStatisticsProvider(StatisticsProvider.NOOP);
        MockOsgi.activate(fds, context.bundleContext(), config);

        assertNotNull(context.getService(BlobStoreStatsMBean.class));
        assertNotNull(context.getService(CacheStatsMBean.class));
    }

    /**
     *
     * Test {@link CachingFileDataStore} is returned when cacheSize > 0 by default.
     */
    @Test
    public void configCachingFileDataStore() throws Exception {
        String nasPath = folder.getRoot().getAbsolutePath() + "/NASPath";
        String cachePath = folder.getRoot().getAbsolutePath() + "/cachePath";
        DataStore ds = getAssertCachingFileDataStore(nasPath, cachePath);
        CachingFileDataStore cds = (CachingFileDataStore) ds;
        SharedBackend backend = cds.getBackend();
        Properties props = (Properties) getField(backend);
        assertEquals("path not equal", nasPath, props.getProperty(FSBackend.FS_BACKEND_PATH));
    }

    /**
     *
     * Test to verify @FileDataStore is returned if cacheSize is not configured.
     */
    @Test
    public void configFileDataStore() throws Exception {
        String nasPath = folder.getRoot().getAbsolutePath() + "/NASPath";
        String cachePath = folder.getRoot().getAbsolutePath() + "/cachePath";
        Map<String, Object> config = new HashMap<String, Object>();
        config.put("repository.home", folder.getRoot().getAbsolutePath());
        config.put(FileDataStoreService.PATH, nasPath);
        config.put(FileDataStoreService.CACHE_PATH, cachePath);
        FileDataStoreService fdsSvc = new FileDataStoreService();

        DataStore ds = fdsSvc.createDataStore(context.componentContext(), config);
        PropertiesUtil.populate(ds, config, false);
        ds.init(folder.getRoot().getAbsolutePath());
        assertTrue("not instance of FileDataStore", ds instanceof FileDataStore);
        FileDataStore fds = (FileDataStore) ds;
        assertEquals("path not equal", nasPath, fds.getPath());
    }

    /**
     * Tests the regitration of CachingFileDataStore and checks existence of
     * reference.key file on first access of getOrCreateReference.
     * @throws Exception
     */
    @Test
    public void registerAndCheckReferenceKey() throws Exception {
        context.registerService(StatisticsProvider.class, StatisticsProvider.NOOP);

        String nasPath = folder.getRoot().getAbsolutePath() + "/NASPath";
        String cachePath = folder.getRoot().getAbsolutePath() + "/cachePath";
        DataStore ds = getAssertCachingFileDataStore(nasPath, cachePath);
        final CachingFileDataStore dataStore = (CachingFileDataStore) ds;

        byte[] key = dataStore.getBackend().getOrCreateReferenceKey();

        // Check bytes retrieved from reference.key file
        File refFile = new File(nasPath, "reference.key");
        byte[] keyRet = FileUtils.readFileToByteArray(refFile);
        assertArrayEquals(key, keyRet);

        assertArrayEquals(key, dataStore.getBackend().getOrCreateReferenceKey());
    }

    private DataStore getAssertCachingFileDataStore(String nasPath, String cachePath)
        throws RepositoryException {
        long cacheSize = 100L;
        Map<String, Object> config = new HashMap<String, Object>();
        config.put("repository.home", folder.getRoot().getAbsolutePath());
        config.put(FileDataStoreService.CACHE_SIZE, cacheSize);
        config.put(FileDataStoreService.PATH, nasPath);
        config.put(FileDataStoreService.CACHE_PATH, cachePath);
        FileDataStoreService fdsSvc = new FileDataStoreService();

        DataStore ds = fdsSvc.createDataStore(context.componentContext(), config);
        PropertiesUtil.populate(ds, config, false);
        ds.init(folder.getRoot().getAbsolutePath());
        assertTrue("not instance of CachingFDS", ds instanceof CachingFileDataStore);
        return ds;
    }

    private static Object getField(Object obj) throws Exception {
        Field f = obj.getClass().getDeclaredField("properties"); //NoSuchFieldException
        f.setAccessible(true);
        return f.get(obj);
    }
}
