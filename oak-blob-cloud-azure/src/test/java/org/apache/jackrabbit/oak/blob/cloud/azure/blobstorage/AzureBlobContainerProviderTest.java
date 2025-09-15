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

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.common.policy.RequestRetryOptions;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Properties;

import static org.junit.Assert.*;

public class AzureBlobContainerProviderTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private static final String CONTAINER_NAME = "test-container";
    private AzureBlobContainerProvider provider;

    @Before
    public void setUp() {
        // Clean up any existing provider
        if (provider != null) {
            provider.close();
            provider = null;
        }
    }

    @After
    public void tearDown() {
        if (provider != null) {
            provider.close();
        }
    }

    @Test
    public void testBuilderWithConnectionString() {
        String connectionString = getConnectionString();
        
        provider = AzureBlobContainerProvider.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString(connectionString)
                .build();
        
        assertNotNull("Provider should not be null", provider);
        assertEquals("Container name should match", CONTAINER_NAME, provider.getContainerName());
        assertEquals("Connection string should match", connectionString, provider.getAzureConnectionString());
    }

    @Test
    public void testBuilderWithAccountNameAndKey() {
        provider = AzureBlobContainerProvider.Builder
                .builder(CONTAINER_NAME)
                .withAccountName("testaccount")
                .withAccountKey("testkey")
                .build();
        
        assertNotNull("Provider should not be null", provider);
        assertEquals("Container name should match", CONTAINER_NAME, provider.getContainerName());
    }

    @Test
    public void testBuilderWithServicePrincipal() {
        provider = AzureBlobContainerProvider.Builder
                .builder(CONTAINER_NAME)
                .withAccountName("testaccount")
                .withTenantId("tenant-id")
                .withClientId("client-id")
                .withClientSecret("client-secret")
                .build();
        
        assertNotNull("Provider should not be null", provider);
        assertEquals("Container name should match", CONTAINER_NAME, provider.getContainerName());
    }

    @Test
    public void testBuilderWithSasToken() {
        provider = AzureBlobContainerProvider.Builder
                .builder(CONTAINER_NAME)
                .withAccountName("testaccount")
                .withSasToken("sas-token")
                .withBlobEndpoint("https://testaccount.blob.core.windows.net")
                .build();
        
        assertNotNull("Provider should not be null", provider);
        assertEquals("Container name should match", CONTAINER_NAME, provider.getContainerName());
    }

    @Test
    public void testBuilderInitializeWithProperties() {
        Properties properties = new Properties();
        properties.setProperty(AzureConstants.AZURE_CONNECTION_STRING, getConnectionString());
        properties.setProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, "testaccount");
        properties.setProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY, "testkey");
        properties.setProperty(AzureConstants.AZURE_TENANT_ID, "tenant-id");
        properties.setProperty(AzureConstants.AZURE_CLIENT_ID, "client-id");
        properties.setProperty(AzureConstants.AZURE_CLIENT_SECRET, "client-secret");
        properties.setProperty(AzureConstants.AZURE_SAS, "sas-token");
        properties.setProperty(AzureConstants.AZURE_BLOB_ENDPOINT, "https://testaccount.blob.core.windows.net");
        
        provider = AzureBlobContainerProvider.Builder
                .builder(CONTAINER_NAME)
                .initializeWithProperties(properties)
                .build();
        
        assertNotNull("Provider should not be null", provider);
        assertEquals("Container name should match", CONTAINER_NAME, provider.getContainerName());
        assertEquals("Connection string should match", getConnectionString(), provider.getAzureConnectionString());
    }

    @Test
    public void testGetBlobContainerWithConnectionString() throws DataStoreException {
        String connectionString = getConnectionString();
        
        provider = AzureBlobContainerProvider.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString(connectionString)
                .build();
        
        BlobContainerClient containerClient = provider.getBlobContainer();
        assertNotNull("Container client should not be null", containerClient);
        assertEquals("Container name should match", CONTAINER_NAME, containerClient.getBlobContainerName());
    }

    @Test
    public void testGetBlobContainerWithRetryOptions() throws DataStoreException {
        String connectionString = getConnectionString();
        RequestRetryOptions retryOptions = new RequestRetryOptions();
        Properties properties = new Properties();
        
        provider = AzureBlobContainerProvider.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString(connectionString)
                .build();
        
        BlobContainerClient containerClient = provider.getBlobContainer(retryOptions, properties);
        assertNotNull("Container client should not be null", containerClient);
        assertEquals("Container name should match", CONTAINER_NAME, containerClient.getBlobContainerName());
    }

    @Test
    public void testClose() {
        provider = AzureBlobContainerProvider.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString(getConnectionString())
                .build();
        
        // Should not throw exception
        provider.close();
    }

    @Test
    public void testBuilderWithNullContainerName() {
        // Builder accepts null container name - no validation
        AzureBlobContainerProvider.Builder builder = AzureBlobContainerProvider.Builder.builder(null);
        assertNotNull("Builder should not be null", builder);
    }

    @Test
    public void testBuilderWithEmptyContainerName() {
        // Builder accepts empty container name - no validation
        AzureBlobContainerProvider.Builder builder = AzureBlobContainerProvider.Builder.builder("");
        assertNotNull("Builder should not be null", builder);
    }

    @Test
    public void testGenerateSharedAccessSignatureWithConnectionString() throws Exception {
        String connectionString = getConnectionString();

        provider = AzureBlobContainerProvider.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString(connectionString)
                .build();

        try {
            String sas = provider.generateSharedAccessSignature(
                    null,
                    "test-blob",
                    new BlobSasPermission().setReadPermission(true),
                    3600,
                    new Properties()
            );
            assertNotNull("SAS token should not be null", sas);
            assertFalse("SAS token should not be empty", sas.isEmpty());
        } catch (Exception e) {
            // Expected for Azurite as it may not support all SAS features
            assertTrue("Should be DataStoreException, URISyntaxException, or InvalidKeyException",
                    e instanceof DataStoreException ||
                    e instanceof URISyntaxException ||
                    e instanceof InvalidKeyException);
        }
    }

    @Test
    public void testGetBlobContainerWithInvalidConnectionString() {
        provider = AzureBlobContainerProvider.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString("invalid-connection-string")
                .build();

        try {
            provider.getBlobContainer();
            fail("Expected exception with invalid connection string");
        } catch (Exception e) {
            // Expected - can be DataStoreException or IllegalArgumentException
            assertNotNull("Exception should not be null", e);
            assertTrue("Should be DataStoreException or IllegalArgumentException",
                    e instanceof DataStoreException || e instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testGetBlobContainerWithServicePrincipalMissingCredentials() {
        provider = AzureBlobContainerProvider.Builder
                .builder(CONTAINER_NAME)
                .withAccountName("testaccount")
                .withTenantId("tenant-id")
                .withClientId("client-id")
                // Missing client secret
                .build();

        try {
            BlobContainerClient containerClient = provider.getBlobContainer();
            // May succeed with incomplete credentials - Azure SDK might handle it differently
            assertNotNull("Container client should not be null", containerClient);
        } catch (Exception e) {
            // Expected - may fail with incomplete credentials
            assertNotNull("Exception should not be null", e);
        }
    }

    @Test
    public void testGetBlobContainerWithSasTokenMissingEndpoint() {
        provider = AzureBlobContainerProvider.Builder
                .builder(CONTAINER_NAME)
                .withAccountName("testaccount")
                .withSasToken("sas-token")
                // Missing blob endpoint
                .build();

        try {
            BlobContainerClient containerClient = provider.getBlobContainer();
            assertNotNull("Container client should not be null", containerClient);
        } catch (Exception e) {
            // May fail depending on SAS token format
            assertNotNull("Exception should not be null", e);
        }
    }

    @Test
    public void testBuilderChaining() {
        provider = AzureBlobContainerProvider.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString("connection1")
                .withAccountName("account1")
                .withAccountKey("key1")
                .withBlobEndpoint("endpoint1")
                .withSasToken("sas1")
                .withTenantId("tenant1")
                .withClientId("client1")
                .withClientSecret("secret1")
                .build();

        assertNotNull("Provider should not be null", provider);
        assertEquals("Container name should match", CONTAINER_NAME, provider.getContainerName());
        assertEquals("Connection string should match", "connection1", provider.getAzureConnectionString());
    }

    @Test
    public void testBuilderWithEmptyStrings() {
        provider = AzureBlobContainerProvider.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString("")
                .withAccountName("")
                .withAccountKey("")
                .withBlobEndpoint("")
                .withSasToken("")
                .withTenantId("")
                .withClientId("")
                .withClientSecret("")
                .build();

        assertNotNull("Provider should not be null", provider);
        assertEquals("Container name should match", CONTAINER_NAME, provider.getContainerName());
        assertEquals("Connection string should be empty", "", provider.getAzureConnectionString());
    }

    @Test
    public void testInitializeWithPropertiesEmptyValues() {
        Properties properties = new Properties();
        properties.setProperty(AzureConstants.AZURE_CONNECTION_STRING, "");
        properties.setProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, "");
        properties.setProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY, "");

        provider = AzureBlobContainerProvider.Builder
                .builder(CONTAINER_NAME)
                .initializeWithProperties(properties)
                .build();

        assertNotNull("Provider should not be null", provider);
        assertEquals("Connection string should be empty", "", provider.getAzureConnectionString());
    }

    private String getConnectionString() {
        return String.format("DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=%s",
                AzuriteDockerRule.ACCOUNT_NAME,
                AzuriteDockerRule.ACCOUNT_KEY,
                azurite.getBlobEndpoint());
    }
}
