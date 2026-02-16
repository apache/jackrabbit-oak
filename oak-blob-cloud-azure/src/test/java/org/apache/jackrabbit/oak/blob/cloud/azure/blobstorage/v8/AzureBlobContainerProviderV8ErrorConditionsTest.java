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

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.SharedAccessBlobHeaders;
import com.microsoft.azure.storage.blob.SharedAccessBlobPermissions;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.EnumSet;

import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.*;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test class specifically for testing error conditions and edge cases
 * in AzureBlobContainerProviderV8.
 */
public class AzureBlobContainerProviderV8ErrorConditionsTest {

    @Mock
    CloudBlobContainer mockContainer;

    @Mock
    CloudBlockBlob mockBlob;

    private static final String CONTAINER_NAME = "test-container";
    private static final String ACCOUNT_NAME = "testaccount";

    private AzureBlobContainerProviderV8 provider;

    private AutoCloseable closeableMockito;

    @Before
    public void setUp() {
        closeableMockito = MockitoAnnotations.openMocks(this);
    }

    @After
    public void tearDown() {
        if (provider != null) {
            provider.close();
        }
        if (closeableMockito != null) {
            try {
                closeableMockito.close();
            } catch (Exception e) {
                // Ignore
            }
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetBlobContainerWithInvalidConnectionString() throws DataStoreException {
        provider = AzureBlobContainerProviderV8.Builder
            .builder(CONTAINER_NAME)
            .withAzureConnectionString("invalid-connection-string")
            .build();

        provider.getBlobContainer();
        fail("Should throw exception for invalid connection string");
    }

    @Test(expected = DataStoreException.class)
    public void testGetBlobContainerWithInvalidAccountKey() throws DataStoreException {
        provider = AzureBlobContainerProviderV8.Builder
            .builder(CONTAINER_NAME)
            .withAccountName("invalidaccount")
            .withAccountKey("invalidkey")
            .withBlobEndpoint("https://invalidaccount.blob.core.windows.net")
            .build();

        provider.getBlobContainer();
        fail("Should throw exception for invalid account key");
    }

    @Test
    public void testGetBlobContainerWithSasToken() throws DataStoreException {
        provider = AzureBlobContainerProviderV8.Builder
            .builder(CONTAINER_NAME)
            .withSasToken("some-sas-token")
            .withBlobEndpoint("https://testaccount.blob.core.windows.net")
            .withAccountName(ACCOUNT_NAME)
            .build();

        provider.getBlobContainer();

        // Should not throw exception for SAS token
        assertTrue("Should not throw exception for SAS token", true);
    }

    @Test
    public void testGetBlobContainerWithNullBlobRequestOptions() throws DataStoreException {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .build();

            provider.getBlobContainer(null);

            assertTrue("Should not throw exception for null blob request options", true);
    }

    @Test(expected = DataStoreException.class)
    public void testGenerateSharedAccessSignatureWithInvalidKey() throws DataStoreException, URISyntaxException, InvalidKeyException, StorageException {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withAccountKey("invalid-key")
                .withBlobEndpoint("https://testaccount.blob.core.windows.net")
                .build();

            provider.generateSharedAccessSignature(
                    null,
                    "test-blob",
                    EnumSet.of(READ, WRITE),
                    3600,
                    null);

            fail("Should throw exception for invalid account key");

    }

    @Test
    public void testGenerateSharedAccessSignatureWithZeroExpiry() throws InvalidKeyException, StorageException, URISyntaxException, DataStoreException {
        provider = AzureBlobContainerProviderV8.Builder
            .builder(CONTAINER_NAME)
            .withAccountName(ACCOUNT_NAME)
            .withAccountKey("valid-key")
            .withBlobEndpoint("https://testaccount.blob.core.windows.net")
            .build();

        //mock CloudBlobContainer
        when(mockContainer.getBlockBlobReference(any())).thenReturn(mockBlob);
        when(mockBlob.generateSharedAccessSignature(any(), any())).thenReturn("mock-sas-token");
        //mock static UtilsV8
        try (MockedStatic<UtilsV8> mockedUtils = mockStatic(UtilsV8.class)) {
            mockedUtils.when(() -> UtilsV8.getBlobContainer(any(), any(), any())).thenReturn(mockContainer);

            provider.generateSharedAccessSignature(
                null,
                "test-blob",
                EnumSet.of(READ, WRITE),
                0, // Zero expiry
                null
            );

            mockedUtils.verify(() -> UtilsV8.getBlobContainer(any(), any(), any()), times(1));
        }
        verify(mockContainer, times(1)).getBlockBlobReference(any());
        verify(mockBlob, times(1)).generateSharedAccessSignature(any(), any());
    }

    @Test
    public void testGenerateSharedAccessSignatureWithNegativeExpiry() throws DataStoreException, URISyntaxException, InvalidKeyException, StorageException {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withAccountKey("valid-key")
                .withBlobEndpoint("https://testaccount.blob.core.windows.net")
                .build();

        //mock CloudBlobContainer
        when(mockContainer.getBlockBlobReference(any())).thenReturn(mockBlob);
        when(mockBlob.generateSharedAccessSignature(any(), any())).thenReturn("mock-sas-token");

        //mock static UtilsV8
        try (MockedStatic<UtilsV8> mockedUtils = mockStatic(UtilsV8.class)) {
            mockedUtils.when(() -> UtilsV8.getBlobContainer(any(), any(), any())).thenReturn(mockContainer);

        provider.generateSharedAccessSignature(
                    null,
                    "test-blob",
                    EnumSet.of(READ, WRITE),
                    -3600, // Negative expiry
                    null
            );

            mockedUtils.verify(() -> UtilsV8.getBlobContainer(any(), any(), any()), times(1));
        }
        verify(mockContainer, times(1)).getBlockBlobReference(any());
        verify(mockBlob, times(1)).generateSharedAccessSignature(any(), any());
    }

    @Test(expected = DataStoreException.class)
    public void testGenerateSharedAccessSignatureWithEmptyPermissions() throws DataStoreException, URISyntaxException, InvalidKeyException, StorageException {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withAccountKey("valid-key")
                .withBlobEndpoint("https://testaccount.blob.core.windows.net")
                .build();

            provider.generateSharedAccessSignature(
                    null,
                    "test-blob",
                    EnumSet.noneOf(SharedAccessBlobPermissions.class), // Empty permissions
                    3600,
                    null
            );
    }

    @Test(expected = DataStoreException.class)
    public void testGenerateSharedAccessSignatureWithNullKey() throws DataStoreException, URISyntaxException, InvalidKeyException, StorageException {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withAccountKey("valid-key")
                .withBlobEndpoint("https://testaccount.blob.core.windows.net")
                .build();

            provider.generateSharedAccessSignature(
                    null,
                    null, // Null key
                    EnumSet.of(READ, WRITE),
                    3600,
                    null
            );
    }

    @Test
    public void testFillEmptyHeadersWithNullHeaders() throws DataStoreException, URISyntaxException, InvalidKeyException, StorageException {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .build();

        // Test with null headers - should not crash
            provider.generateSharedAccessSignature(
                    null,
                    "test-blob",
                    EnumSet.of(READ, WRITE),
                    3600,
                    null // Null headers
            );

            assertTrue("Should not throw exception", true);
    }

    @Test(expected = DataStoreException.class)
    public void testFillEmptyHeadersWithPartiallyNullHeaders() throws DataStoreException, URISyntaxException, InvalidKeyException, StorageException {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withAccountKey("valid-key")
                .withBlobEndpoint("https://testaccount.blob.core.windows.net")
                .build();

        SharedAccessBlobHeaders headers = new SharedAccessBlobHeaders();
        headers.setContentType("application/json");
        // Leave other headers null to test fillEmptyHeaders method

            provider.generateSharedAccessSignature(
                    null,
                    "test-blob",
                    EnumSet.of(READ, WRITE),
                    3600,
                    headers
            );
    }

    @Test
    public void testCloseMultipleTimes() {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .build();

        // Should not throw exception when called multiple times
        provider.close();
        provider.close();
        provider.close();

        assertTrue("Should not throw exception", true);
    }

    @Test
    public void testCloseWithNullExecutorService() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .build();

        // Use reflection to set executor service to null
        Field executorField = AzureBlobContainerProviderV8.class
                .getDeclaredField("executorService");
        executorField.setAccessible(true);
        executorField.set(provider, null);

        // Should handle null executor service gracefully
        try {
            provider.close();
        } catch (NullPointerException e) {
            fail("Should handle null executor service gracefully");
        }
    }
}
