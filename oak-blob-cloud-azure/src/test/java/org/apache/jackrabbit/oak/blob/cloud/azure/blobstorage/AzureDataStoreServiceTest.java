/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.sas.SasTokenGenerator;
import org.apache.jackrabbit.oak.plugins.blob.AbstractSharedCachingDataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.sling.testing.mock.osgi.junit.OsgiContext;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureTestHelper.getConfigurationWithConnectionString;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureTestHelper.writeBlob;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AzureDataStoreServiceTest {
    private static final String CONTAINER_NAME = "datastore";
    
    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();
    
    @Rule
    public final OsgiContext ctx = new OsgiContext();

    @Before
    public void setUp() throws Exception {
        azurite.getContainer(CONTAINER_NAME);
    }

    @Test
    public void generateDownloadUri_InjectingCustomSasTokenGenerator() throws DataStoreException {
        ctx.registerService(SasTokenGenerator.class, (blob, policy, optionalHeaders) -> "customSasToken");
        AzureDataStore azureDataStore = registerAzureDataStoreInOsgiContext(ctx);
        DataIdentifier identifier = writeBlob(azureDataStore.getBackend());

        URI downloadURI = azureDataStore.getDownloadURI(identifier, DataRecordDownloadOptions.DEFAULT);

        assertNotNull(downloadURI);
        assertEquals("Should be custom SAS token", "customSasToken", downloadURI.getQuery());
    }

    @Test
    public void generateDownloadUri_FallbackToDefaultSasTokenGenerator() throws DataStoreException {
        AzureDataStore azureDataStore = registerAzureDataStoreInOsgiContext(ctx);
        DataIdentifier identifier = writeBlob(azureDataStore.getBackend());

        URI downloadURI = azureDataStore.getDownloadURI(identifier, DataRecordDownloadOptions.DEFAULT);
        
        assertNotNull(downloadURI);
        assertTrue("Should be valid SAS token, but was " + downloadURI.getQuery(), 
            downloadURI.getQuery().contains("sig="));
    }

    @NotNull
    private static AzureDataStore registerAzureDataStoreInOsgiContext(OsgiContext ctx) {
        ctx.registerService(StatisticsProvider.class, StatisticsProvider.NOOP);
        Map<String, Object> config = toMap(getConfigurationWithConnectionString(azurite));
        AzureDataStoreService azureDataStoreService = ctx.registerInjectActivateService(new AzureDataStoreService(), config);
        DataStore dataStore = azureDataStoreService.createDataStore(ctx.componentContext(), config);
        assertNotNull(dataStore);

        AbstractSharedCachingDataStore sharedDataStore = ctx.getService(AbstractSharedCachingDataStore.class);
        assertNotNull(sharedDataStore);
        assertTrue(sharedDataStore instanceof AzureDataStore);

        return (AzureDataStore) sharedDataStore;
    }

    private static Map<String, Object> toMap(Properties properties) {
        Map<String, Object> result = new HashMap<>();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            result.put(entry.getKey().toString(), entry.getValue());
        }
        return result;
    }
}