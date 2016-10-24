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
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static com.google.common.collect.Maps.newHashMap;
import static org.apache.jackrabbit.oak.plugins.blob.datastore.AbstractDataStoreService
    .JR2_CACHING_PROP;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests {@link OakCachingFDS} and OSGi registration using {@link FileDataStoreService}.
 */
public class OakCachingFDSTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Rule
    public OsgiContext context = new OsgiContext();

    @Before
    public void setUp() {
    }

    private OakCachingFDS dataStore;

    private String fsBackendPath;

    private String path;

    @Before
    public void setup() throws Exception {
        System.setProperty(JR2_CACHING_PROP, "true");
        fsBackendPath = folder.newFolder().getAbsolutePath();
        path = folder.newFolder().getAbsolutePath();
    }

    @After
    public void tear() throws Exception {
        System.clearProperty(JR2_CACHING_PROP);
    }
    @Test
    public void createAndCheckReferenceKey() throws Exception {
        createCachingFDS();
        assertReferenceKey();
    }

    @Test
    public void registerAndCheckReferenceKey() throws Exception {
        context.registerService(StatisticsProvider.class, StatisticsProvider.NOOP);
        registerCachingFDS();
        assertReferenceKey();
    }

    private void assertReferenceKey() throws Exception {
        byte[] key = dataStore.getOrCreateReferenceKey();

        // Check bytes retrieved from reference.key file
        File refFile = new File(fsBackendPath, "reference.key");
        byte[] keyRet = FileUtils.readFileToByteArray(refFile);
        assertArrayEquals(key, keyRet);

        assertArrayEquals(key, dataStore.getOrCreateReferenceKey());
    }

    private void registerCachingFDS() {
        Map<String, Object> props = newHashMap();
        props.put("cachePath", path);
        props.put("path", fsBackendPath);
        props.put("cacheSize", "10");
        props.put("repository.home", new File(fsBackendPath).getParentFile().getAbsolutePath());

        context.registerInjectActivateService(new FileDataStoreService(), props);
        assertNotNull(context.getService(BlobStore.class));
        BlobStore blobStore = context.getService(BlobStore.class);
        assert blobStore instanceof DataStoreBlobStore;

        DataStore ds = ((DataStoreBlobStore) blobStore).getDataStore();
        assert ds instanceof OakCachingFDS;
        dataStore = (OakCachingFDS) ds;
    }

    private void createCachingFDS() throws Exception {
        Properties props = new Properties();
        props.put("fsBackendPath", fsBackendPath);
        props.put("path", path);
        props.put("cacheSize", "10");

        dataStore = new OakCachingFDS();
        dataStore.setFsBackendPath(fsBackendPath);
        dataStore.setAsyncUploadLimit(0);
        dataStore.setProperties(props);
        dataStore.init(folder.newFolder().getAbsolutePath());
    }
}
