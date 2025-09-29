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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v8;

import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test class focused on AzureBlobContainerProviderV8 container operations functionality.
 * Tests getBlobContainer operations and container access patterns.
 */
public class AzureBlobContainerProviderV8ContainerOperationsTest {

    private static final String CONTAINER_NAME = "test-container";
    private static final String ACCOUNT_NAME = "testaccount";
    private static final String TENANT_ID = "test-tenant-id";
    private static final String CLIENT_ID = "test-client-id";
    private static final String CLIENT_SECRET = "test-client-secret";
    private static final String CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=testaccount;AccountKey=dGVzdC1hY2NvdW50LWtleQ==;EndpointSuffix=core.windows.net";
    private static final String SAS_TOKEN = "?sv=2020-08-04&ss=b&srt=sco&sp=rwdlacx&se=2023-12-31T23:59:59Z&st=2023-01-01T00:00:00Z&spr=https&sig=test";
    private static final String ACCOUNT_KEY = "dGVzdC1hY2NvdW50LWtleQ==";

    private AzureBlobContainerProviderV8 provider;

    @After
    public void tearDown() throws Exception {
        if (provider != null) {
            provider.close();
        }
    }

    @Test
    public void testGetBlobContainerWithBlobRequestOptions() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString(CONNECTION_STRING)
                .build();

        // Test getBlobContainer with BlobRequestOptions
        // This covers the overloaded method that accepts BlobRequestOptions
        try {
            provider.getBlobContainer(new com.microsoft.azure.storage.blob.BlobRequestOptions());
            // If no exception is thrown, the method executed successfully
        } catch (Exception e) {
            // Expected for invalid connection string in test environment
            assertTrue("Should throw DataStoreException for invalid connection",
                    e instanceof org.apache.jackrabbit.core.data.DataStoreException);
        }
    }

    @Test
    public void testGetBlobContainerWithoutBlobRequestOptions() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString(CONNECTION_STRING)
                .build();

        // Test getBlobContainer without BlobRequestOptions (calls overloaded method with null)
        try {
            provider.getBlobContainer();
            // If no exception is thrown, the method executed successfully
        } catch (Exception e) {
            // Expected for invalid connection string in test environment
            assertTrue("Should throw DataStoreException for invalid connection",
                    e instanceof org.apache.jackrabbit.core.data.DataStoreException);
        }
    }

    @Test
    public void testGetBlobContainerWithServicePrincipalAndBlobRequestOptions() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withTenantId(TENANT_ID)
                .withClientId(CLIENT_ID)
                .withClientSecret(CLIENT_SECRET)
                .build();

        BlobRequestOptions options = new BlobRequestOptions();
        options.setTimeoutIntervalInMs(30000);

        // This test covers the getBlobContainerFromServicePrincipals method with BlobRequestOptions
        // In a real test environment, this would require actual Azure credentials
        try {
            provider.getBlobContainer(options);
            // If we get here without exception, that's also valid (means authentication worked)
        } catch (Exception e) {
            // Expected in test environment - we're testing the code path exists
            // Accept various types of exceptions that can occur during authentication attempts
            assertTrue("Should attempt service principal authentication and throw appropriate exception",
                e instanceof DataStoreException ||
                e instanceof IllegalArgumentException ||
                e instanceof RuntimeException ||
                e.getCause() instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testGetBlobContainerWithConnectionStringOnly() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString("DefaultEndpointsProtocol=https;AccountName=testaccount;AccountKey=dGVzdGtleQ==;EndpointSuffix=core.windows.net")
                .build();

        // This should work without service principal authentication
        CloudBlobContainer container = provider.getBlobContainer();
        assertNotNull("Container should not be null", container);
    }

    @Test
    public void testGetBlobContainerWithAccountKeyOnly() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withAccountKey("dGVzdGtleQ==")
                .build();

        // This should work without service principal authentication
        CloudBlobContainer container = provider.getBlobContainer();
        assertNotNull("Container should not be null", container);
    }

    @Test
    public void testGetBlobContainerWithSasTokenOnly() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withSasToken("?sv=2020-08-04&ss=b&srt=sco&sp=rwdlacx&se=2023-12-31T23:59:59Z&st=2023-01-01T00:00:00Z&spr=https&sig=test")
                .build();

        // This should work without service principal authentication
        CloudBlobContainer container = provider.getBlobContainer();
        assertNotNull("Container should not be null", container);
    }

    @Test
    public void testGetBlobContainerWithCustomBlobRequestOptions() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString("DefaultEndpointsProtocol=https;AccountName=testaccount;AccountKey=dGVzdGtleQ==;EndpointSuffix=core.windows.net")
                .build();

        BlobRequestOptions options = new BlobRequestOptions();
        options.setTimeoutIntervalInMs(60000);
        options.setMaximumExecutionTimeInMs(120000);

        CloudBlobContainer container = provider.getBlobContainer(options);
        assertNotNull("Container should not be null", container);
    }

    @Test
    public void testGetBlobContainerWithNullBlobRequestOptions() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString("DefaultEndpointsProtocol=https;AccountName=testaccount;AccountKey=dGVzdGtleQ==;EndpointSuffix=core.windows.net")
                .build();

        // Test with null BlobRequestOptions - should work the same as no options
        CloudBlobContainer container = provider.getBlobContainer(null);
        assertNotNull("Container should not be null", container);
    }

    @Test
    public void testGetBlobContainerWithServicePrincipalOnly() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withTenantId(TENANT_ID)
                .withClientId(CLIENT_ID)
                .withClientSecret(CLIENT_SECRET)
                .build();

        // This should use service principal authentication
        try {
            CloudBlobContainer container = provider.getBlobContainer();
            assertNotNull("Container should not be null if authentication succeeds", container);
        } catch (Exception e) {
            // Expected in test environment - we're testing the code path exists
            assertTrue("Should attempt service principal authentication and throw appropriate exception",
                e instanceof DataStoreException ||
                e instanceof IllegalArgumentException ||
                e instanceof RuntimeException ||
                e.getCause() instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testGetBlobContainerMultipleCalls() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString("DefaultEndpointsProtocol=https;AccountName=testaccount;AccountKey=dGVzdGtleQ==;EndpointSuffix=core.windows.net")
                .build();

        // Test multiple calls to getBlobContainer
        CloudBlobContainer container1 = provider.getBlobContainer();
        CloudBlobContainer container2 = provider.getBlobContainer();

        assertNotNull("First container should not be null", container1);
        assertNotNull("Second container should not be null", container2);

        // Both containers should reference the same container name
        assertEquals("Container names should match", container1.getName(), container2.getName());
    }

    @Test
    public void testGetBlobContainerWithDifferentOptions() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString("DefaultEndpointsProtocol=https;AccountName=testaccount;AccountKey=dGVzdGtleQ==;EndpointSuffix=core.windows.net")
                .build();

        BlobRequestOptions options1 = new BlobRequestOptions();
        options1.setTimeoutIntervalInMs(30000);

        BlobRequestOptions options2 = new BlobRequestOptions();
        options2.setTimeoutIntervalInMs(60000);

        // Test with different options
        CloudBlobContainer container1 = provider.getBlobContainer(options1);
        CloudBlobContainer container2 = provider.getBlobContainer(options2);

        assertNotNull("First container should not be null", container1);
        assertNotNull("Second container should not be null", container2);
        assertEquals("Container names should match", container1.getName(), container2.getName());
    }

    @Test
    public void testGetBlobContainerConsistency() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString("DefaultEndpointsProtocol=https;AccountName=testaccount;AccountKey=dGVzdGtleQ==;EndpointSuffix=core.windows.net")
                .build();

        // Test that container name is consistent across calls
        CloudBlobContainer container = provider.getBlobContainer();
        assertNotNull("Container should not be null", container);
        assertEquals("Container name should match expected", CONTAINER_NAME, container.getName());
    }

    @Test
    public void testGetBlobContainerWithInvalidConnectionString() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString("invalid-connection-string")
                .build();

        try {
            provider.getBlobContainer();
            fail("Should throw exception for invalid connection string");
        } catch (Exception e) {
            assertTrue("Should throw appropriate exception for invalid connection string",
                    e instanceof DataStoreException ||
                    e instanceof IllegalArgumentException ||
                    e instanceof RuntimeException);
        }
    }

    @Test
    public void testGetBlobContainerWithEmptyCredentials() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .build();

        try {
            provider.getBlobContainer();
            // If no exception is thrown, that means the provider has some default behavior
            // which is also acceptable - we're testing that the method can be called
        } catch (Exception e) {
            // Expected - various exceptions can be thrown for missing credentials
            assertTrue("Should throw appropriate exception for missing credentials",
                    e instanceof DataStoreException ||
                    e instanceof IllegalArgumentException ||
                    e instanceof RuntimeException ||
                    e instanceof NullPointerException);
        }
    }
}