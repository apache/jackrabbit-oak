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
package org.apache.jackrabbit.oak.plugins.document;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.jackrabbit.oak.api.jmx.ConsolidatedDataStoreCacheStatsMBean;
import org.apache.jackrabbit.oak.plugins.blob.AbstractSharedCachingDataStore;
import org.apache.jackrabbit.oak.plugins.blob.ConsolidatedDataStoreCacheStats;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.sling.testing.mock.osgi.MockOsgi;
import org.apache.sling.testing.mock.osgi.ReferenceViolationException;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.osgi.framework.ServiceRegistration;

import static com.google.common.collect.Maps.newHashMap;
import static org.apache.sling.testing.mock.osgi.MockOsgi.deactivate;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

/**
 * Tests the registration of the {@link ConsolidatedDataStoreCacheStatsMBean}.
 */
public class DocumentCachingDataStoreStatsTest {

    @Rule
    public OsgiContext context = new OsgiContext();

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Rule
    public final TemporaryFolder target = new TemporaryFolder(new File("target"));

    private String repoHome;

    @BeforeClass
    public static void checkMongoDbAvailable() {
        Assume.assumeTrue(MongoUtils.isAvailable());
        MongoUtils.dropCollections(MongoUtils.DB);
    }

    @Before
    public void setUp() throws IOException {
        context.registerService(StatisticsProvider.class, StatisticsProvider.NOOP);
        repoHome = target.newFolder().getAbsolutePath();
    }

    @After
    public void dropCollections() {
        MongoUtils.dropCollections(MongoUtils.DB);
    }

    @Test
    public void testUseCachingBlobStore() {
        ServiceRegistration delegateReg =
            context.bundleContext().registerService(AbstractSharedCachingDataStore.class.getName(),
                mock(AbstractSharedCachingDataStore.class), null);
        assertNotNull(context.getService(AbstractSharedCachingDataStore.class));
        registerBlobStore();

        registerDocumentNodeStoreService(true);
        assertServiceActivated();

        ConsolidatedDataStoreCacheStats dataStoreStats =
            context.registerInjectActivateService(new ConsolidatedDataStoreCacheStats());
        assertNotNull(context.getService(ConsolidatedDataStoreCacheStatsMBean.class));

        deactivate(dataStoreStats, context.bundleContext());
        unregisterDocumentNodeStoreService();
        unregisterBlobStore();
        delegateReg.unregister();
    }

    @Test
    public void testNoCachingBlobStore() {
        expectedEx.expect(ReferenceViolationException.class);

        registerBlobStore();

        registerDocumentNodeStoreService(true);
        assertServiceActivated();

        try {
            ConsolidatedDataStoreCacheStats dataStoreStats =
                context.registerInjectActivateService(new ConsolidatedDataStoreCacheStats());
            assertNull(context.getService(ConsolidatedDataStoreCacheStatsMBean.class));
        } finally {
            unregisterDocumentNodeStoreService();
            unregisterBlobStore();
        }
    }

    private DocumentNodeStoreService documentNodeStoreService;

    private void registerDocumentNodeStoreService(boolean customBlobStore) {
        Map<String, Object> properties = newHashMap();

        properties.put("mongouri", MongoUtils.URL);
        properties.put("db", MongoUtils.DB);
        properties.put("repository.home", repoHome);
        properties.put(DocumentNodeStoreService.CUSTOM_BLOB_STORE, customBlobStore);
        MockOsgi.setConfigForPid(context.bundleContext(),
                DocumentNodeStoreService.class.getName(), properties);
        context.registerInjectActivateService(new DocumentNodeStoreService.Preset());
        documentNodeStoreService =
            context.registerInjectActivateService(new DocumentNodeStoreService(), properties);
    }

    private void unregisterDocumentNodeStoreService() {
        deactivate(documentNodeStoreService, context.bundleContext());
    }

    private ServiceRegistration blobStore;

    private void registerBlobStore() {
        blobStore = context.bundleContext()
            .registerService(BlobStore.class.getName(), mock(BlobStore.class), null);
    }

    private void unregisterBlobStore() {
        blobStore.unregister();
    }

    private void assertServiceActivated() {
        assertNotNull(context.getService(NodeStore.class));
    }
}
