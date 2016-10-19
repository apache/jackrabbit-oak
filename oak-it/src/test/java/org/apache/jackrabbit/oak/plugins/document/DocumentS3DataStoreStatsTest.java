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

import org.apache.jackrabbit.oak.blob.cloud.aws.s3.S3DataStoreStats;
import org.apache.jackrabbit.oak.blob.cloud.aws.s3.SharedS3DataStore;
import org.apache.jackrabbit.oak.blob.cloud.s3.stats.S3DataStoreStatsMBean;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.sling.testing.mock.osgi.ReferenceViolationException;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
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
 * Tests the registration of the S3DataStoreStatsMbean.
 */
public class DocumentS3DataStoreStatsTest {

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
    }

    @Before
    public void setUp() throws IOException {
        context.registerService(StatisticsProvider.class, StatisticsProvider.NOOP);
        repoHome = target.newFolder().getAbsolutePath();
    }

    @Test
    public void testUseS3BlobStore() {
        ServiceRegistration delegateReg =
            context.bundleContext().registerService(SharedS3DataStore.class.getName(),
                mock(SharedS3DataStore.class), null);
        assertNotNull(context.getService(SharedS3DataStore.class));
        registerBlobStore();

        registerDocumentNodeStoreService(true);
        assertServiceActivated();

        S3DataStoreStats s3DataStoreStats =
            context.registerInjectActivateService(new S3DataStoreStats(), null);
        assertNotNull(context.getService(S3DataStoreStatsMBean.class));

        deactivate(s3DataStoreStats);
        unregisterDocumentNodeStoreService();
        unregisterBlobStore();
        delegateReg.unregister();
    }

    @Test
    public void testNoS3BlobStore() {
        expectedEx.expect(ReferenceViolationException.class);

        registerBlobStore();

        registerDocumentNodeStoreService(true);
        assertServiceActivated();

        S3DataStoreStats s3DataStoreStats =
            context.registerInjectActivateService(new S3DataStoreStats(), null);
        assertNull(context.getService(S3DataStoreStatsMBean.class));

        unregisterDocumentNodeStoreService();
        unregisterBlobStore();
    }

    private DocumentNodeStoreService documentNodeStoreService;

    private void registerDocumentNodeStoreService(boolean customBlobStore) {
        Map<String, Object> properties = newHashMap();

        properties.put("mongouri", MongoUtils.URL);
        properties.put("db", MongoUtils.DB);
        properties.put("repository.home", repoHome);
        properties.put(DocumentNodeStoreService.CUSTOM_BLOB_STORE, customBlobStore);
        documentNodeStoreService =
            context.registerInjectActivateService(new DocumentNodeStoreService(), properties);
    }

    private void unregisterDocumentNodeStoreService() {
        deactivate(documentNodeStoreService);
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
