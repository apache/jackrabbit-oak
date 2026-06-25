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
import java.util.Collections;
import java.util.Dictionary;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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
        DataRecord record = mock(DataRecord.class);
        when(mockImpl.addRecord(any())).thenReturn(record);

        DataRecord result = wrapper.new DelegatingDataStore()
                .addRecord(new ByteArrayInputStream(new byte[]{1}));

        assertSame(record, result);
        verify(mockImpl).addRecord(any());
    }

    @Test
    public void getRecordDelegatesToActiveImpl() throws DataStoreException {
        DataRecord record = mock(DataRecord.class);
        when(mockImpl.getRecord(any())).thenReturn(record);

        DataRecord result = wrapper.new DelegatingDataStore()
                .getRecord(mock(org.apache.jackrabbit.oak.spi.blob.data.DataIdentifier.class));

        assertSame(record, result);
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
                eq(AbstractSharedCachingDataStore.class.getName()), same(mockImpl), any());
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

        verify(ctx.getBundleContext()).registerService(anyString(), any(), props.capture());
        assertEquals(AzureDataStore.class.getName(), props.getValue().get(Constants.SERVICE_PID));
    }

    @Test
    public void registerService_setsAzureBlobDescription() {
        ComponentContext ctx = mockComponentContext();

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Dictionary<String, Object>> props = ArgumentCaptor.forClass(Dictionary.class);
        AzureDataStoreWrapper.registerService(ctx, mockImpl);

        verify(ctx.getBundleContext()).registerService(anyString(), any(), props.capture());
        assertArrayEquals(new String[]{"type=AzureBlob"},
                (String[]) props.getValue().get("oak.datastore.description"));
    }

    @Test
    public void registerService_returnsRegistrationFromBundleContext() {
        ComponentContext ctx = mockComponentContext();
        BundleContext bundleContext = ctx.getBundleContext();
        ServiceRegistration<?> reg = mock(ServiceRegistration.class);
        doReturn(reg).when(bundleContext).registerService(anyString(), any(), any());

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
        doReturn(mock(ServiceRegistration.class)).when(bundleContext).registerService(anyString(), any(), any());
        return ctx;
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
