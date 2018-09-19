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

package org.apache.jackrabbit.oak.segment;

import static com.google.common.collect.Maps.newHashMap;
import static org.apache.sling.testing.mock.osgi.MockOsgi.deactivate;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.oak.plugins.blob.BlobGCMBean;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreBlobStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.SharedDataStoreUtils;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.GarbageCollectableBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.osgi.framework.ServiceRegistration;

public class SegmentNodeStoreServiceTest {

    @Rule
    public OsgiContext context = new OsgiContext();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Before
    public void setUp(){
        context.registerService(StatisticsProvider.class, StatisticsProvider.NOOP);
    }

    /**
     * A NodeStore service should be registered when a BlobStore service is not
     * available and the "customBlobStore" configuration property is false.
     */
    @Test
    public void testNoCustomBlobStoreWithoutBlobStore() {
        registerSegmentNodeStoreService(false);
        assertServiceActivated();
        assertBlobGCMbeanNotActivated();

        unregisterSegmentNodeStoreService();
    }

    /**
     * A NodeStore service should be registered when a BlobStore service is not
     * available but the "customBlobStore" configuration property is false.
     */
    @Test
    public void testNoCustomBlobStoreWithBlobStore() {
        registerBlobStore();

        registerSegmentNodeStoreService(false);
        assertServiceActivated();
        assertBlobGCMbeanNotActivated();

        unregisterSegmentNodeStoreService();
        unregisterBlobStore();
    }

    /**
     * A NodeStore service should not be registered when the "customBlobStore"
     * configuration property is true but a BlobStore service is not available.
     */
    @Test
    public void testUseCustomBlobStoreWithoutBlobStore() {
        registerSegmentNodeStoreService(true);
        assertServiceNotActivated();
        assertBlobGCMbeanNotActivated();

        unregisterSegmentNodeStoreService();
    }

    /**
     * A NodeStore service should be registered when the "customBlobStore"
     * configuration property is true and a BlobStore service is available.
     */
    @Test
    public void testUseCustomBlobStoreWithBlobStore() {
        registerBlobStore();

        registerSegmentNodeStoreService(true);
        assertServiceActivated();
        assertBlobGCMbeanActivated();

        unregisterSegmentNodeStoreService();
        unregisterBlobStore();
    }

    /**
     * A NodeStore service should be registered when the "customBlobStore"
     * configuration property is true and a BlobStore service becomes
     * dynamically available.
     */
    @Test
    public void testUseCustomBlobStoreWithDynamicBlobStoreActivation() {
        registerSegmentNodeStoreService(true);
        assertServiceNotActivated();

        registerBlobStore();
        assertServiceActivated();
        assertBlobGCMbeanActivated();

        unregisterSegmentNodeStoreService();
        unregisterBlobStore();
    }

    /**
     * A NodeStore service should be unregistered when the "customBlobStore"
     * configuration property is true and a BlobStore service becomes
     * dynamically unavailable.
     */
    @Test
    public void testUseCustomBlobStoreWithDynamicBlobStoreDeactivation() {
        registerBlobStore();

        registerSegmentNodeStoreService(true);
        assertServiceActivated();
        assertBlobGCMbeanActivated();

        unregisterBlobStore();
        assertServiceNotActivated();

        unregisterSegmentNodeStoreService();
    }

    /**
     * A SharedDataStore service should be registered when the "customBlobStore"
     * configuration property is true and a BlobStore service as SharedDataStore available.
     */
    @Test
    public void testUseCustomBlobStoreWithSharedBlobStore() throws IOException {
        DataStoreBlobStore dataStoreBlobStore = registerSharedDataStore(folder.newFolder());

        registerSegmentNodeStoreService(true);
        assertServiceActivated();

        assertBlobGCMbeanActivated();
        assertSharedDataStoreRegistered(dataStoreBlobStore);

        unregisterSegmentNodeStoreService();
        unregisterBlobStore();
    }

    /**
     * A SharedDataStore service should not be registered when the "customBlobStore"
     * configuration property is false and a BlobStore service as SharedDataStore available.
     */
    @Test
    public void testUseNoCustomBlobStoreWithSharedBlobStore() throws IOException {
        DataStoreBlobStore dataStoreBlobStore = registerSharedDataStore(folder.newFolder());

        registerSegmentNodeStoreService(false);
        assertServiceActivated();

        assertBlobGCMbeanNotActivated();
        assertSharedDataStoreNotRegistered(dataStoreBlobStore);

        unregisterSegmentNodeStoreService();
        unregisterBlobStore();
    }

    private SegmentNodeStoreService segmentNodeStoreService;

    protected void registerSegmentNodeStoreService(boolean customBlobStore) {
        Map<String, Object> properties = newHashMap();

        properties.put(SegmentNodeStoreService.CUSTOM_BLOB_STORE, customBlobStore);
        properties.put(SegmentNodeStoreService.REPOSITORY_HOME_DIRECTORY, folder.getRoot().getAbsolutePath());

        segmentNodeStoreService = context.registerInjectActivateService(new SegmentNodeStoreService(), properties);
    }

    protected void unregisterSegmentNodeStoreService() {
        deactivate(segmentNodeStoreService, context.bundleContext());
    }

    private ServiceRegistration blobStore;

    private void registerBlobStore() {
        blobStore = context.bundleContext().registerService(BlobStore.class.getName(), mock(GarbageCollectableBlobStore.class), null);
    }

    private DataStoreBlobStore registerSharedDataStore(File home) {
        OakFileDataStore ds = new OakFileDataStore();
        ds.init(home.getAbsolutePath());
        DataStoreBlobStore dataStoreBlobStore = new DataStoreBlobStore(ds);

        blobStore = context.bundleContext().registerService(BlobStore.class.getName(), dataStoreBlobStore, null);
        return dataStoreBlobStore;
    }

    private void unregisterBlobStore() {
        blobStore.unregister();
    }

    protected void assertServiceActivated() {
        assertNotNull(context.getService(NodeStore.class));
        assertNotNull(context.getService(SegmentStoreProvider.class));
    }

    protected void assertServiceNotActivated() {
        assertNull(context.getService(NodeStore.class));
        assertNull(context.getService(SegmentStoreProvider.class));
    }

    protected void assertSharedDataStoreRegistered(DataStoreBlobStore dataStoreBlobStore) {
        List<DataRecord> allMetadataRecords =
            dataStoreBlobStore.getAllMetadataRecords(SharedDataStoreUtils.SharedStoreRecordType.REPOSITORY.getType());
        assertFalse(allMetadataRecords.isEmpty());
    }

    protected void assertSharedDataStoreNotRegistered(DataStoreBlobStore dataStoreBlobStore) {
        List<DataRecord> allMetadataRecords =
            dataStoreBlobStore.getAllMetadataRecords(SharedDataStoreUtils.SharedStoreRecordType.REPOSITORY.getType());
        assertTrue(allMetadataRecords.isEmpty());
    }


    protected void assertBlobGCMbeanActivated() {
        assertNotNull(context.getService(BlobGCMBean.class));
    }

    protected void assertBlobGCMbeanNotActivated() {
        assertNull(context.getService(BlobGCMBean.class));
    }
}
