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

import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.junit.After;
import org.junit.Test;
import org.mockito.MockedStatic;

import static org.junit.Assert.*;
import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

import com.microsoft.azure.storage.blob.CloudBlobContainer;

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
    private static final String CONNECTION_STRING = "some-connection-string";
    private static final String SAS_TOKEN = "some-SAS-token";
    private static final String ACCOUNT_KEY = "dGVzdC1hY2NvdW50LWtleQ==";
    private static final String BLOB_ENDPOINT = "https://some.valid.url";

    private AzureBlobContainerProviderV8 provider;

    @After
    public void tearDown() {
        if (provider != null) {
            provider.close();
        }
    }

    @Test
    public void testAuthenticationPriorityConnectionStringOverSasToken() throws DataStoreException {
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

        //spy UtilsV8
        try (MockedStatic<UtilsV8> mockedUtils = mockStatic(UtilsV8.class)) {
            mockedUtils.when(() -> UtilsV8.getBlobContainer(anyString(), anyString(), any()))
                .thenReturn(mock(CloudBlobContainer.class));

            provider.getBlobContainer();

            mockedUtils.verify(() -> UtilsV8.getBlobContainer(CONNECTION_STRING, CONTAINER_NAME, null), times(1));
            mockedUtils.verifyNoMoreInteractions();
        }
    }

    @Test
    public void testAuthenticationPrioritySasTokenOverAccountKey() throws DataStoreException {
        // Test that SAS token takes priority over account key when no connection string or service principal
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withSasToken(SAS_TOKEN)
                .withAccountKey(ACCOUNT_KEY)
                .withBlobEndpoint(BLOB_ENDPOINT)
                .build();

        //spy UtilsV8
        try (MockedStatic<UtilsV8> mockedUtils = mockStatic(UtilsV8.class)) {
            mockedUtils.when(() -> UtilsV8.getConnectionStringForSas(SAS_TOKEN, BLOB_ENDPOINT, ACCOUNT_NAME))
                .thenReturn(CONNECTION_STRING);
            mockedUtils.when(() -> UtilsV8.getBlobContainer(CONNECTION_STRING, CONTAINER_NAME, null))
                .thenReturn(mock(CloudBlobContainer.class));

            provider.getBlobContainer();

            mockedUtils.verify(() -> UtilsV8.getConnectionStringForSas(SAS_TOKEN, BLOB_ENDPOINT, ACCOUNT_NAME), times(1));
            mockedUtils.verify(() -> UtilsV8.getBlobContainer(CONNECTION_STRING, CONTAINER_NAME, null), times(1));
            mockedUtils.verifyNoMoreInteractions();
        }
    }

    @Test
    public void testAuthenticationPriorityServicePrincipalOverAccountKey() {
        // Test that service principal takes priority over account key when no connection string or SAS token
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withTenantId(TENANT_ID)
                .withClientId(CLIENT_ID)
                .withClientSecret(CLIENT_SECRET)
                .withAccountKey(ACCOUNT_KEY)
                .build();

        //spy UtilsV8
        try (MockedStatic<UtilsV8> mockedUtils = mockStatic(UtilsV8.class, CALLS_REAL_METHODS)) {
            // This should use service principal authentication
            try {
                provider.getBlobContainer();
            } catch (Exception e) {
                // Expected
            }

            // Verify that UtilsV8.getBlobContainer was not called
            // This means service principal authentication was attempted
            mockedUtils.verifyNoInteractions();
        }
    }

    @Test
    public void testAuthenticationFallbackToAccountKey() throws DataStoreException {
        // Test fallback to account key when no other authentication methods are available
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withAccountKey(ACCOUNT_KEY)
                .withBlobEndpoint(BLOB_ENDPOINT)
                .build();

        try (MockedStatic<UtilsV8> mockedUtils = mockStatic(UtilsV8.class, CALLS_REAL_METHODS)) {
            // This should use service principal authentication
            provider.getBlobContainer();

            mockedUtils.verify(() -> UtilsV8.getConnectionString(ACCOUNT_NAME, ACCOUNT_KEY, BLOB_ENDPOINT), times(1));
            mockedUtils.verify(() -> UtilsV8.getBlobContainer(anyString(), eq(CONTAINER_NAME), eq(null)), times(1));
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

        boolean result = (Boolean) MethodUtils.invokeMethod(provider, true, "authenticateViaServicePrincipal");
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

        boolean result = (Boolean) MethodUtils.invokeMethod(provider, true, "authenticateViaServicePrincipal");
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

        boolean result = (Boolean) MethodUtils.invokeMethod(provider, true, "authenticateViaServicePrincipal");
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

        boolean result = (Boolean) MethodUtils.invokeMethod(provider, true, "authenticateViaServicePrincipal");
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

        boolean result = (Boolean) MethodUtils.invokeMethod(provider, true, "authenticateViaServicePrincipal");
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

        boolean result = (Boolean) MethodUtils.invokeMethod(provider, true, "authenticateViaServicePrincipal");
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

        boolean result = (Boolean) MethodUtils.invokeMethod(provider, true, "authenticateViaServicePrincipal");
        assertTrue("Should authenticate via service principal when all credentials are present", result);
    }

    @Test
    public void testAuthenticationWithConnectionStringOnly() throws DataStoreException {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString(CONNECTION_STRING)
                .build();

        //mock UtilsV8
        try (MockedStatic<UtilsV8> mockedUtils = mockStatic(UtilsV8.class)) {
            mockedUtils.when(() -> UtilsV8.getBlobContainer(anyString(), anyString(), any()))
                .thenReturn(mock(CloudBlobContainer.class));

            provider.getBlobContainer();

            mockedUtils.verify(() -> UtilsV8.getBlobContainer(CONNECTION_STRING, CONTAINER_NAME, null), times(1));
            mockedUtils.verifyNoMoreInteractions();
        }
    }

    @Test
    public void testAuthenticationWithSasTokenOnly() throws DataStoreException {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withSasToken(SAS_TOKEN)
                .withBlobEndpoint(BLOB_ENDPOINT)
                .build();

        // This should use SAS token authentication
        try (MockedStatic<UtilsV8> mockedUtils = mockStatic(UtilsV8.class)) {
            mockedUtils.when(() -> UtilsV8.getConnectionStringForSas(SAS_TOKEN, BLOB_ENDPOINT, ACCOUNT_NAME))
                .thenReturn(CONNECTION_STRING);
            mockedUtils.when(() -> UtilsV8.getBlobContainer(CONNECTION_STRING, CONTAINER_NAME, null))
                .thenReturn(mock(CloudBlobContainer.class));

            provider.getBlobContainer();

            mockedUtils.verify(() -> UtilsV8.getConnectionStringForSas(SAS_TOKEN, BLOB_ENDPOINT, ACCOUNT_NAME), times(1));
            mockedUtils.verify(() -> UtilsV8.getBlobContainer(CONNECTION_STRING, CONTAINER_NAME, null), times(1));
            mockedUtils.verifyNoMoreInteractions();
        }
    }
}