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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import org.apache.jackrabbit.oak.plugins.blob.AbstractSharedCachingDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.ConfigurableDataRecordAccessProvider;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUpload;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadException;
import org.apache.jackrabbit.oak.spi.blob.data.DataIdentifier;
import org.apache.jackrabbit.oak.spi.blob.data.DataRecord;
import org.apache.jackrabbit.oak.spi.blob.data.DataStoreException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.osgi.framework.BundleContext;
import org.osgi.framework.Constants;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.Collections;
import java.util.Dictionary;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

/**
 * Unit tests for AzureDataStoreWrapper — delegation, v12 feature-flag detection, and OSGi service registration.
 * <p>
 * AzureDataStoreWrapper is the OSGi component that selects between the v8 and v12 blob-store backend
 * at runtime based on config/JVM flags and exposes a single DataStore service to the rest of the system.
 * These tests verify that delegation is transparent, flag resolution precedence is correct, and the
 * OSGi service registration uses the v8 PID so existing configs keep working without migration.
 */
public class AzureDataStoreWrapperTest {

    // mockImpl implements both AbstractSharedCachingDataStore and ConfigurableDataRecordAccessProvider —
    // the same intersection both AzureDataStore (v8) and AzureDataStoreV12 satisfy at runtime.
    private AbstractSharedCachingDataStore mockImpl;
    private AzureDataStoreWrapper wrapper;

    @After
    public void tearDown() {
        System.clearProperty(AzureDataStoreWrapper.ENV_VAR_V12_ENABLED);
        System.clearProperty(AzureDataStoreWrapper.JVM_PROPERTY_V12_ENABLED);
    }

    @Before
    public void setUp() {
        mockImpl = mock(
                AbstractSharedCachingDataStore.class,
                withSettings().extraInterfaces(ConfigurableDataRecordAccessProvider.class));
        wrapper = new AzureDataStoreWrapper();
        wrapper.activeImpl = mockImpl;
    }

    @Test
    public void addRecordDelegatesToActiveImpl() throws DataStoreException {
        DataRecord dataRecord = mock(DataRecord.class);
        when(mockImpl.addRecord(any())).thenReturn(dataRecord);

        DataRecord result = wrapper.new DelegatingDataStore()
                .addRecord(new ByteArrayInputStream(new byte[]{1}));

        assertSame(dataRecord, result);
        verify(mockImpl).addRecord(any());
    }

    @Test
    public void getRecordDelegatesToActiveImpl() throws DataStoreException {
        DataRecord dataRecord = mock(DataRecord.class);
        when(mockImpl.getRecord(any())).thenReturn(dataRecord);

        DataRecord result = wrapper.new DelegatingDataStore()
                .getRecord(mock(org.apache.jackrabbit.oak.spi.blob.data.DataIdentifier.class));

        assertSame(dataRecord, result);
        verify(mockImpl).getRecord(any());
    }

    /**
     * Config setters on DelegatingDataStore must forward to activeImpl — buffering them in the wrapper would silently have no effect on the active backend.
     */
    @Test
    public void configSettersAppliedToActiveImpl() {
        AzureDataStoreWrapper.DelegatingDataStore ds = wrapper.new DelegatingDataStore();
        ds.setDirectUploadURIExpirySeconds(300);
        ds.setDirectDownloadURIExpirySeconds(600);
        ds.setDirectDownloadURICacheSize(100);
        ds.setBinaryTransferAccelerationEnabled(true);

        ConfigurableDataRecordAccessProvider provider = (ConfigurableDataRecordAccessProvider) mockImpl;
        verify(provider).setDirectUploadURIExpirySeconds(300);
        verify(provider).setDirectDownloadURIExpirySeconds(600);
        verify(provider).setDirectDownloadURICacheSize(100);
        verify(provider).setBinaryTransferAccelerationEnabled(true);
    }

    @Test
    public void closeClosesActiveImpl() throws DataStoreException {
        wrapper.new DelegatingDataStore().close();
        verify(mockImpl).close();
    }

    @Test
    public void getUseV12Value_noSysProp_noConfig_returnsFalse() {
        assertFalse(AzureDataStoreWrapper.getUseV12Value(Collections.emptyMap()));
    }

    @Test
    public void getUseV12Value_noSysProp_configTrue_returnsTrue() {
        Map<String, Object> config = Collections.singletonMap(AzureDataStoreWrapper.ENV_VAR_V12_ENABLED, true);
        assertTrue(AzureDataStoreWrapper.getUseV12Value(config));
    }

    @Test
    public void getUseV12Value_noSysProp_configFalse_returnsFalse() {
        Map<String, Object> config = Collections.singletonMap(AzureDataStoreWrapper.ENV_VAR_V12_ENABLED, false);
        assertFalse(AzureDataStoreWrapper.getUseV12Value(config));
    }

    @Test
    public void getUseV12Value_jvmPropTrue_overridesConfigFalse() {
        System.setProperty(AzureDataStoreWrapper.JVM_PROPERTY_V12_ENABLED, "true");
        Map<String, Object> config = Collections.singletonMap(AzureDataStoreWrapper.ENV_VAR_V12_ENABLED, false);
        assertTrue(AzureDataStoreWrapper.getUseV12Value(config));
    }

    @Test
    public void getUseV12Value_jvmPropFalse_overridesConfigTrue() {
        System.setProperty(AzureDataStoreWrapper.JVM_PROPERTY_V12_ENABLED, "false");
        Map<String, Object> config = Collections.singletonMap(AzureDataStoreWrapper.ENV_VAR_V12_ENABLED, true);
        assertFalse(AzureDataStoreWrapper.getUseV12Value(config));
    }

    @Test
    public void registerService_registersUnderAbstractSharedCachingDataStoreClass() {
        ComponentContext ctx = mockComponentContext();

        AzureDataStoreWrapper.registerService(ctx, mockImpl);

        verify(ctx.getBundleContext()).registerService(
                eq(AbstractSharedCachingDataStore.class), same(mockImpl), any());
    }

    /**
     * The registered service PID must match AzureDataStore (v8) — OSGi configs in existing AEM
     * installations target that PID, so changing it would orphan those configs on upgrade.
     */
    @Test
    public void registerService_usesV8PidForCompatibility() {
        ComponentContext ctx = mockComponentContext();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Dictionary<String, Object>> props = ArgumentCaptor.forClass(Dictionary.class);
        AzureDataStoreWrapper.registerService(ctx, mockImpl);

        verify(ctx.getBundleContext()).registerService(any(Class.class), any(), props.capture());
        assertEquals(AzureDataStore.class.getName(), props.getValue().get(Constants.SERVICE_PID));
    }

    @Test
    public void registerService_setsAzureBlobDescription() {
        ComponentContext ctx = mockComponentContext();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Dictionary<String, Object>> props = ArgumentCaptor.forClass(Dictionary.class);
        AzureDataStoreWrapper.registerService(ctx, mockImpl);

        verify(ctx.getBundleContext()).registerService(any(Class.class), any(), props.capture());
        assertArrayEquals(new String[]{"type=AzureBlob"},
                (String[]) props.getValue().get("oak.datastore.description"));
    }

    @Test
    public void registerService_returnsRegistrationFromBundleContext() {
        ComponentContext ctx = mockComponentContext();
        BundleContext bundleContext = ctx.getBundleContext();
        ServiceRegistration<?> reg = mock(ServiceRegistration.class);
        doReturn(reg).when(bundleContext).registerService(any(Class.class), any(), any());

        ServiceRegistration<?> result = AzureDataStoreWrapper.registerService(ctx, mockImpl);

        assertSame(reg, result);
    }

    @SuppressWarnings("unchecked")
    private ComponentContext mockComponentContext() {
        BundleContext bundleContext = mock(BundleContext.class);
        ComponentContext ctx = mock(ComponentContext.class);
        when(ctx.getBundleContext()).thenReturn(bundleContext);
        // registerService must be pre-stubbed; without this, Mockito returns null and
        // registerService() NPEs before the test can capture its arguments.
        doReturn(mock(ServiceRegistration.class)).when(bundleContext).registerService(any(Class.class), any(), any());
        return ctx;
    }

    @Test
    public void getRecordIfStoredDelegatesToActiveImpl() throws DataStoreException {
        DataRecord dataRecord = mock(DataRecord.class);
        when(mockImpl.getRecordIfStored(any())).thenReturn(dataRecord);

        DataRecord result = wrapper.new DelegatingDataStore()
                .getRecordIfStored(mock(DataIdentifier.class));

        assertSame(dataRecord, result);
        verify(mockImpl).getRecordIfStored(any());
    }

    @Test
    public void getRecordFromReferenceDelegatesToActiveImpl() throws DataStoreException {
        DataRecord dataRecord = mock(DataRecord.class);
        when(mockImpl.getRecordFromReference("ref123")).thenReturn(dataRecord);

        DataRecord result = wrapper.new DelegatingDataStore().getRecordFromReference("ref123");

        assertSame(dataRecord, result);
        verify(mockImpl).getRecordFromReference("ref123");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void getAllIdentifiersDelegatesToActiveImpl() throws DataStoreException {
        Iterator<DataIdentifier> iter = mock(Iterator.class);
        when(mockImpl.getAllIdentifiers()).thenReturn(iter);

        Iterator<DataIdentifier> result = wrapper.new DelegatingDataStore().getAllIdentifiers();

        assertSame(iter, result);
        verify(mockImpl).getAllIdentifiers();
    }

    @Test
    public void updateModifiedDateOnAccessDelegatesToActiveImpl() {
        wrapper.new DelegatingDataStore().updateModifiedDateOnAccess(12345L);
        verify(mockImpl).updateModifiedDateOnAccess(12345L);
    }

    @Test
    public void deleteAllOlderThanDelegatesToActiveImpl() throws DataStoreException {
        when(mockImpl.deleteAllOlderThan(99999L)).thenReturn(3);

        int result = wrapper.new DelegatingDataStore().deleteAllOlderThan(99999L);

        assertEquals(3, result);
        verify(mockImpl).deleteAllOlderThan(99999L);
    }

    @Test
    public void clearInUseDelegatesToActiveImpl() {
        wrapper.new DelegatingDataStore().clearInUse();
        verify(mockImpl).clearInUse();
    }

    @Test
    public void getMinRecordLengthDelegatesToActiveImpl() {
        when(mockImpl.getMinRecordLength()).thenReturn(4096);

        int result = wrapper.new DelegatingDataStore().getMinRecordLength();

        assertEquals(4096, result);
        verify(mockImpl).getMinRecordLength();
    }

    @Test
    public void initiateDataRecordUploadDelegatesToActiveImpl() throws DataRecordUploadException {
        DataRecordUpload upload = mock(DataRecordUpload.class);
        ConfigurableDataRecordAccessProvider provider = (ConfigurableDataRecordAccessProvider) mockImpl;
        when(provider.initiateDataRecordUpload(1024L, 5)).thenReturn(upload);

        DataRecordUpload result = wrapper.new DelegatingDataStore().initiateDataRecordUpload(1024L, 5);

        assertSame(upload, result);
        verify(provider).initiateDataRecordUpload(1024L, 5);
    }

    @Test
    public void completeDataRecordUploadDelegatesToActiveImpl() throws Exception {
        DataRecord dataRecord = mock(DataRecord.class);
        ConfigurableDataRecordAccessProvider provider = (ConfigurableDataRecordAccessProvider) mockImpl;
        when(provider.completeDataRecordUpload("token123")).thenReturn(dataRecord);

        DataRecord result = wrapper.new DelegatingDataStore().completeDataRecordUpload("token123");

        assertSame(dataRecord, result);
        verify(provider).completeDataRecordUpload("token123");
    }

    @Test
    public void getDownloadURIDelegatesToActiveImpl() {
        URI uri = URI.create("https://example.com/blob");
        ConfigurableDataRecordAccessProvider provider = (ConfigurableDataRecordAccessProvider) mockImpl;
        DataIdentifier id = mock(DataIdentifier.class);
        when(provider.getDownloadURI(same(id), any())).thenReturn(uri);

        URI result = wrapper.new DelegatingDataStore()
                .getDownloadURI(id, DataRecordDownloadOptions.DEFAULT);

        assertEquals(uri, result);
        verify(provider).getDownloadURI(same(id), any());
    }

    @Test
    public void createV8Store_returnsAzureDataStoreInstance() {
        AbstractSharedCachingDataStore store = AzureDataStoreWrapper.createV8Store(new java.util.Properties());
        assertNotNull(store);
        assertTrue(store instanceof AzureDataStore);
    }

    @Test
    public void createV12Store_returnsAzureDataStoreV12Instance() {
        AbstractSharedCachingDataStore store = AzureDataStoreWrapper.createV12Store(new java.util.Properties());
        assertNotNull(store);
        assertNotNull(store.getClass().getName());
        assertTrue(store.getClass().getName().contains("AzureDataStoreV12"));
    }

    // Guards against activeImpl accidentally becoming static; each wrapper must own its impl.
    @Test
    public void instancesHaveIndependentActiveImpl() throws DataStoreException {
        AbstractSharedCachingDataStore mockImplB = mock(
                AbstractSharedCachingDataStore.class,
                withSettings().extraInterfaces(ConfigurableDataRecordAccessProvider.class));

        AzureDataStoreWrapper wrapperB = new AzureDataStoreWrapper();
        wrapperB.activeImpl = mockImplB;

        DataRecord recA = mock(DataRecord.class, "recA");
        DataRecord recB = mock(DataRecord.class, "recB");
        when(mockImpl.addRecord(any())).thenReturn(recA);
        when(mockImplB.addRecord(any())).thenReturn(recB);

        DataRecord resultA = wrapper.new DelegatingDataStore()
                .addRecord(new ByteArrayInputStream(new byte[]{1}));
        DataRecord resultB = wrapperB.new DelegatingDataStore()
                .addRecord(new ByteArrayInputStream(new byte[]{2}));

        assertSame(recA, resultA);
        assertSame(recB, resultB);
        verify(mockImpl).addRecord(any());
        verify(mockImplB).addRecord(any());
    }
}
