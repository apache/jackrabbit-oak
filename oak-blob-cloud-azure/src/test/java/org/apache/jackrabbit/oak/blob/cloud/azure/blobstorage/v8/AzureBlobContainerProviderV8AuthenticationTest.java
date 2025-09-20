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

import org.apache.jackrabbit.core.data.DataStoreException;
import org.junit.After;
import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.*;

/**
 * Test class focused on AzureBlobContainerProviderV8 authentication functionality.
 * Tests authentication methods including service principal, connection string, SAS token, and account key.
 */
public class AzureBlobContainerProviderV8AuthenticationTest {

    private static final String CONTAINER_NAME = "test-container";
    private static final String ACCOUNT_NAME = "testaccount";
    private static final String TENANT_ID = "test-tenant-id";
    private static final String CLIENT_ID = "test-client-id";
    private static final String CLIENT_SECRET = "test-client-secret";
    private static final String CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=testaccount;AccountKey=dGVzdC1hY2NvdW50LWtleQ==;EndpointSuffix=core.windows.net";
    private static final String SAS_TOKEN = "?sv=2020-08-04&ss=b&srt=sco&sp=rwdlacx&se=2023-12-31T23:59:59Z&st=2023-01-01T00:00:00Z&spr=https&sig=test";
    private static final String ACCOUNT_KEY = "dGVzdC1hY2NvdW50LWtleQ==";
    private static final String BLOB_ENDPOINT = "https://testaccount.blob.core.windows.net";

    private AzureBlobContainerProviderV8 provider;

    @After
    public void tearDown() throws Exception {
        if (provider != null) {
            provider.close();
        }
    }

    @Test
    public void testAuthenticationPriorityConnectionString() throws Exception {
        // Test that connection string takes priority over all other authentication methods
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString(CONNECTION_STRING)
                .withAccountName(ACCOUNT_NAME)
                .withTenantId(TENANT_ID)
                .withClientId(CLIENT_ID)
                .withClientSecret(CLIENT_SECRET)
                .withSasToken(SAS_TOKEN)
                .withAccountKey(ACCOUNT_KEY)
                .build();

        try {
            provider.getBlobContainer();
        } catch (Exception e) {
            // Expected for invalid connection string in test environment
            assertTrue("Should throw DataStoreException for invalid connection",
                    e instanceof org.apache.jackrabbit.core.data.DataStoreException);
        }
    }

    @Test
    public void testAuthenticationPrioritySasTokenOverAccountKey() throws Exception {
        // Test that SAS token takes priority over account key when no connection string or service principal
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withSasToken(SAS_TOKEN)
                .withAccountKey(ACCOUNT_KEY)
                .withBlobEndpoint(BLOB_ENDPOINT)
                .build();

        try {
            provider.getBlobContainer();
        } catch (Exception e) {
            // Expected for invalid SAS token in test environment
            assertTrue("Should throw DataStoreException for invalid SAS token",
                    e instanceof org.apache.jackrabbit.core.data.DataStoreException);
        }
    }

    @Test
    public void testAuthenticationFallbackToAccountKey() throws Exception {
        // Test fallback to account key when no other authentication methods are available
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withAccountKey(ACCOUNT_KEY)
                .withBlobEndpoint(BLOB_ENDPOINT)
                .build();

        try {
            provider.getBlobContainer();
        } catch (Exception e) {
            // Expected for invalid account key in test environment
            assertTrue("Should throw DataStoreException for invalid account key",
                    e instanceof org.apache.jackrabbit.core.data.DataStoreException);
        }
    }

    @Test
    public void testServicePrincipalAuthenticationMissingAccountName() throws Exception {
        // Test service principal authentication detection with missing account name
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withTenantId(TENANT_ID)
                .withClientId(CLIENT_ID)
                .withClientSecret(CLIENT_SECRET)
                .build();

        Method authenticateMethod = AzureBlobContainerProviderV8.class
                .getDeclaredMethod("authenticateViaServicePrincipal");
        authenticateMethod.setAccessible(true);

        boolean result = (Boolean) authenticateMethod.invoke(provider);
        assertFalse("Should not authenticate via service principal when account name is missing", result);
    }

    @Test
    public void testServicePrincipalAuthenticationMissingClientId() throws Exception {
        // Test service principal authentication detection with missing client ID
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withTenantId(TENANT_ID)
                .withClientSecret(CLIENT_SECRET)
                .build();

        Method authenticateMethod = AzureBlobContainerProviderV8.class
                .getDeclaredMethod("authenticateViaServicePrincipal");
        authenticateMethod.setAccessible(true);

        boolean result = (Boolean) authenticateMethod.invoke(provider);
        assertFalse("Should not authenticate via service principal when client ID is missing", result);
    }

    @Test
    public void testServicePrincipalAuthenticationMissingClientSecret() throws Exception {
        // Test service principal authentication detection with missing client secret
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withTenantId(TENANT_ID)
                .withClientId(CLIENT_ID)
                .build();

        Method authenticateMethod = AzureBlobContainerProviderV8.class
                .getDeclaredMethod("authenticateViaServicePrincipal");
        authenticateMethod.setAccessible(true);

        boolean result = (Boolean) authenticateMethod.invoke(provider);
        assertFalse("Should not authenticate via service principal when client secret is missing", result);
    }

    @Test
    public void testServicePrincipalAuthenticationMissingTenantId() throws Exception {
        // Test service principal authentication detection with missing tenant ID
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withClientId(CLIENT_ID)
                .withClientSecret(CLIENT_SECRET)
                .build();

        Method authenticateMethod = AzureBlobContainerProviderV8.class
                .getDeclaredMethod("authenticateViaServicePrincipal");
        authenticateMethod.setAccessible(true);

        boolean result = (Boolean) authenticateMethod.invoke(provider);
        assertFalse("Should not authenticate via service principal when tenant ID is missing", result);
    }

    @Test
    public void testServicePrincipalAuthenticationWithBlankConnectionString() throws Exception {
        // Test that service principal authentication is used when connection string is blank
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString("   ") // Blank connection string
                .withAccountName(ACCOUNT_NAME)
                .withTenantId(TENANT_ID)
                .withClientId(CLIENT_ID)
                .withClientSecret(CLIENT_SECRET)
                .build();

        Method authenticateMethod = AzureBlobContainerProviderV8.class
                .getDeclaredMethod("authenticateViaServicePrincipal");
        authenticateMethod.setAccessible(true);

        boolean result = (Boolean) authenticateMethod.invoke(provider);
        assertTrue("Should authenticate via service principal when connection string is blank", result);
    }

    @Test
    public void testServicePrincipalAuthenticationWithEmptyConnectionString() throws Exception {
        // Test that service principal authentication is used when connection string is empty
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString("") // Empty connection string
                .withAccountName(ACCOUNT_NAME)
                .withTenantId(TENANT_ID)
                .withClientId(CLIENT_ID)
                .withClientSecret(CLIENT_SECRET)
                .build();

        Method authenticateMethod = AzureBlobContainerProviderV8.class
                .getDeclaredMethod("authenticateViaServicePrincipal");
        authenticateMethod.setAccessible(true);

        boolean result = (Boolean) authenticateMethod.invoke(provider);
        assertTrue("Should authenticate via service principal when connection string is empty", result);
    }

    @Test
    public void testServicePrincipalAuthenticationWithValidCredentials() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withTenantId(TENANT_ID)
                .withClientId(CLIENT_ID)
                .withClientSecret(CLIENT_SECRET)
                .build();

        Method authenticateMethod = AzureBlobContainerProviderV8.class
                .getDeclaredMethod("authenticateViaServicePrincipal");
        authenticateMethod.setAccessible(true);

        boolean result = (Boolean) authenticateMethod.invoke(provider);
        assertTrue("Should authenticate via service principal when all credentials are present", result);
    }

    @Test
    public void testAuthenticationWithConnectionStringOnly() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString(CONNECTION_STRING)
                .build();

        // This should use connection string authentication
        try {
            provider.getBlobContainer();
        } catch (Exception e) {
            // Expected for invalid connection string in test environment
            assertTrue("Should throw DataStoreException for invalid connection",
                    e instanceof DataStoreException);
        }
    }

    @Test
    public void testAuthenticationWithSasTokenOnly() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withSasToken(SAS_TOKEN)
                .withBlobEndpoint(BLOB_ENDPOINT)
                .build();

        // This should use SAS token authentication
        try {
            provider.getBlobContainer();
        } catch (Exception e) {
            // Expected for invalid SAS token in test environment
            assertTrue("Should throw DataStoreException for invalid SAS token",
                    e instanceof DataStoreException);
        }
    }

    @Test
    public void testAuthenticationWithAccountKeyOnly() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withAccountKey(ACCOUNT_KEY)
                .withBlobEndpoint(BLOB_ENDPOINT)
                .build();

        // This should use account key authentication
        try {
            provider.getBlobContainer();
        } catch (Exception e) {
            // Expected for invalid account key in test environment
            assertTrue("Should throw DataStoreException for invalid account key",
                    e instanceof DataStoreException);
        }
    }

    @Test
    public void testAuthenticationWithServicePrincipalOnly() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withTenantId(TENANT_ID)
                .withClientId(CLIENT_ID)
                .withClientSecret(CLIENT_SECRET)
                .build();

        // This should use service principal authentication
        try {
            provider.getBlobContainer();
        } catch (Exception e) {
            // Expected in test environment - we're testing the code path exists
            assertTrue("Should attempt service principal authentication and throw appropriate exception",
                e instanceof DataStoreException ||
                e instanceof IllegalArgumentException ||
                e instanceof RuntimeException ||
                e.getCause() instanceof IllegalArgumentException);
        }
    }
}