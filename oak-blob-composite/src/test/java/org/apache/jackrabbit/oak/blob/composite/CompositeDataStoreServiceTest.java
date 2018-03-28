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

package org.apache.jackrabbit.oak.blob.composite;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.OakFileDataStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.DataStoreProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CompositeDataStoreServiceTest {
    private static final String DEFAULT_ROLE = "defaultRole";

    @Rule
    public final TemporaryFolder folder = new TemporaryFolder(new File("target"));

    @Rule
    public final OsgiContext context = new OsgiContext();

    @Before
    public void setup() {
        context.registerService(StatisticsProvider.class, StatisticsProvider.NOOP);
        context.registerService(DelegateMinRecordLengthSelector.class, new GuaranteedMinRecordLengthSelector());
        context.registerService(DelegateHandler.class, new IntelligentDelegateHandler());
    }

    private DataStoreProvider createDelegateDataStore(final String role) {
        return new DataStoreProvider() {
            OakFileDataStore ds = new OakFileDataStore();
            @Override
            public String getRole() {
                return role;
            }
            @Override
            public DataStore getDataStore() {
                return ds;
            }
        };
    }

    private Map<String, Object> createDsConfig(String role) {
        Map<String, Object> config = Maps.newHashMap();
        config.put(DataStoreProvider.ROLE, role);
        return config;
    }

    private Map<String, Object> createServiceConfig(String role) {
        return createServiceConfig(Lists.newArrayList(role));
    }

    private Map<String, Object> createServiceConfig(Collection<String> roles) {
        Map<String, Object> config = Maps.newHashMap();
        config.put(CompositeDataStore.ROLES, Joiner.on(",").join(roles));
        try {
            config.put("path", folder.newFolder().getAbsolutePath());
        }
        catch (IOException e) { }
        return config;
    }

    private DataStoreProvider registerDataStoreProvider(final String role) {
        Map<String, Object> dsConfig = createDsConfig(role);
        DataStoreProvider dsp = createDelegateDataStore(role);
        context.registerService(DataStoreProvider.class, dsp, dsConfig);
        return dsp;
    }

    private CompositeDataStoreService registerService(final String role)
            throws IOException{
        Map<String, Object> serviceConfig = createServiceConfig(role);
        CompositeDataStoreService service = new CompositeDataStoreService();
        context.registerInjectActivateService(service, serviceConfig);
        return service;
    }

    private void verifyServiceRegistrationState(OsgiContext context, boolean shouldBeActive) {
        CompositeDataStore ds = context.getService(CompositeDataStore.class);
        if (shouldBeActive) {
            assertNotNull(ds);
        }
        else {
            assertNull(ds);
        }
    }


    @Test
    public void testCreateCompositeDataStoreService() throws IOException {
        registerDataStoreProvider(DEFAULT_ROLE);
        registerService(DEFAULT_ROLE);

        verifyServiceRegistrationState(context, true);
    }

    @Test
    public void testCreateCompositeDataStoreServiceWithoutDataStoreProvider() throws IOException {
        registerService(DEFAULT_ROLE);

        verifyServiceRegistrationState(context, false);
    }

    @Test
    public void testCreateCompositeDataStoreServiceThenAddDataStoreProvider() throws IOException {
        registerService(DEFAULT_ROLE);
        verifyServiceRegistrationState(context, false);

        registerDataStoreProvider(DEFAULT_ROLE);
        verifyServiceRegistrationState(context, true);
    }

    @Test
    public void testCreateCompositeDataStoreServiceDeferredDataStoreCreationWaitsForAllDelegates() throws IOException {
        Map<String, Object> serviceConfig = createServiceConfig(Lists.newArrayList(DEFAULT_ROLE, "local2"));
        CompositeDataStoreService service = new CompositeDataStoreService();
        context.registerInjectActivateService(service, serviceConfig);

        DataStoreProvider dsp1 = registerDataStoreProvider(DEFAULT_ROLE);
        verifyServiceRegistrationState(context, false);

        DataStoreProvider[] providers = context.getServices(DataStoreProvider.class, null);
        assertEquals(1, providers.length);

        DataStoreProvider dsp2 = registerDataStoreProvider("local2");
        verifyServiceRegistrationState(context, true);

        providers = context.getServices(DataStoreProvider.class, null);
        assertEquals(2, providers.length);

        providers = context.getServices(DataStoreProvider.class, String.format("(role=%s)", DEFAULT_ROLE));
        assertEquals(1, providers.length);
        assertEquals(dsp1, providers[0]);

        providers = context.getServices(DataStoreProvider.class, "(role=local2)");
        assertEquals(1, providers.length);
        assertEquals(dsp2, providers[0]);
    }

    @Test
    public void testRemoveDataStoreProviderDeregistersService() throws IOException {
        DataStoreProvider dsp = registerDataStoreProvider(DEFAULT_ROLE);
        CompositeDataStoreService service = registerService(DEFAULT_ROLE);

        verifyServiceRegistrationState(context, true);

        service.removeDelegateDataStore(dsp);

        verifyServiceRegistrationState(context, false);
    }

    @Test
    public void testCreateDataStoreWithoutActiveDelegatesReturnsNull() {
        CompositeDataStoreService service = new CompositeDataStoreService();

        Map<String, Object> config = Maps.newHashMap();
        DataStore ds = service.createDataStore(context.componentContext(), config);
        assertNull(ds);

        assertNull(context.getService(CompositeDataStore.class));
    }

    @Test
    public void testDSIsInitializedIfServiceStartsFirst() throws IOException {
        registerService(DEFAULT_ROLE);
        CompositeDataStore cds = context.getService(CompositeDataStore.class);
        assertNull(cds);
        BlobStore bs = context.getService(BlobStore.class);
        assertNull(bs);
        assertNull(context.getService(CompositeDataStore.class));
        assertNull(context.getService(BlobStore.class));

        registerDataStoreProvider(DEFAULT_ROLE);
        assertNotNull(context.getService(CompositeDataStore.class));
        assertNotNull(context.getService(BlobStore.class));
    }

    @Test
    public void testDSIsInitializedIfDelegatesStartFirst() throws IOException {
        registerDataStoreProvider(DEFAULT_ROLE);
        assertNull(context.getService(CompositeDataStore.class));
        assertNull(context.getService(BlobStore.class));

        registerService(DEFAULT_ROLE);
        assertNotNull(context.getService(CompositeDataStore.class));
        assertNotNull(context.getService(BlobStore.class));
    }

    @Test
    public void testCreateServiceThenMultipleDelegatesHasCorrectNumberOfRegistrations() throws IOException {
        Map<String, Object> serviceConfig = createServiceConfig(Lists.newArrayList(DEFAULT_ROLE, "local2"));
        CompositeDataStoreService service = new CompositeDataStoreService();
        context.registerInjectActivateService(service, serviceConfig);

        CompositeDataStore[] dataStores = context.getServices(CompositeDataStore.class, null);
        assertEquals(0, dataStores.length);
        BlobStore[] blobStores = context.getServices(BlobStore.class, null);
        assertEquals(0, blobStores.length);

        DataStoreProvider dsp1 = registerDataStoreProvider(DEFAULT_ROLE);
        DataStoreProvider[] providers = context.getServices(DataStoreProvider.class, null);
        assertEquals(1, providers.length);
        dataStores = context.getServices(CompositeDataStore.class, null);
        assertEquals(0, dataStores.length);
        blobStores = context.getServices(BlobStore.class, null);
        assertEquals(0, blobStores.length);

        DataStoreProvider dsp2 = registerDataStoreProvider("local2");
        providers = context.getServices(DataStoreProvider.class, null);
        assertEquals(2, providers.length);
        dataStores = context.getServices(CompositeDataStore.class, null);
        assertEquals(1, dataStores.length);
        blobStores = context.getServices(BlobStore.class, null);
        assertEquals(1, blobStores.length);
    }
}