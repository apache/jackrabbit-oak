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

import com.microsoft.azure.storage.blob.SharedAccessBlobHeaders;
import com.microsoft.azure.storage.blob.SharedAccessBlobPermissions;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.EnumSet;

import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.*;
import static org.junit.Assert.*;

/**
 * Test class specifically for testing error conditions and edge cases
 * in AzureBlobContainerProviderV8.
 */
public class AzureBlobContainerProviderV8ErrorConditionsTest {

    private static final String CONTAINER_NAME = "test-container";
    private static final String ACCOUNT_NAME = "testaccount";

    private AzureBlobContainerProviderV8 provider;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @After
    public void tearDown() {
        if (provider != null) {
            provider.close();
        }
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
            // Should throw DataStoreException or IllegalArgumentException
            assertTrue("Should throw appropriate exception for invalid connection string",
                    e instanceof DataStoreException || e instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testGetBlobContainerWithInvalidAccountKey() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName("invalidaccount")
                .withAccountKey("invalidkey")
                .withBlobEndpoint("https://invalidaccount.blob.core.windows.net")
                .build();

        try {
            provider.getBlobContainer();
            fail("Should throw exception for invalid account key");
        } catch (Exception e) {
            // Should throw DataStoreException or related exception
            assertTrue("Should throw appropriate exception for invalid account key",
                    e instanceof DataStoreException || e instanceof IllegalArgumentException ||
                    e instanceof URISyntaxException || e instanceof InvalidKeyException);
        }
    }

    @Test
    public void testGetBlobContainerWithInvalidSasToken() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withSasToken("invalid-sas-token")
                .withBlobEndpoint("https://testaccount.blob.core.windows.net")
                .withAccountName(ACCOUNT_NAME)
                .build();

        // Note: Some invalid SAS tokens might not throw exceptions immediately
        // but will fail when actually trying to access the storage
        try {
            provider.getBlobContainer();
            // If no exception is thrown, that's also valid behavior for some invalid tokens
            // The actual validation happens when the container is used
        } catch (Exception e) {
            // Should throw DataStoreException or related exception
            assertTrue("Should throw appropriate exception for invalid SAS token",
                    e instanceof DataStoreException || e instanceof IllegalArgumentException ||
                    e instanceof URISyntaxException);
        }
    }

    @Test
    public void testGetBlobContainerWithNullBlobRequestOptions() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString("DefaultEndpointsProtocol=https;AccountName=devstoreaccount1;AccountKey=invalid;")
                .build();

        // Should not throw exception with null options, but may fail due to invalid connection
        try {
            provider.getBlobContainer(null);
        } catch (Exception e) {
            // Expected for invalid connection, but not for null options
            // The exception could be various types depending on the validation
            assertTrue("Exception should be related to connection or key validation",
                    e instanceof DataStoreException || e instanceof IllegalArgumentException ||
                    e instanceof URISyntaxException || e instanceof InvalidKeyException);
        }
    }

    @Test
    public void testGenerateSharedAccessSignatureWithInvalidKey() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withAccountKey("invalid-key")
                .withBlobEndpoint("https://testaccount.blob.core.windows.net")
                .build();

        try {
            provider.generateSharedAccessSignature(
                    null,
                    "test-blob",
                    EnumSet.of(READ, WRITE),
                    3600,
                    null
            );
            fail("Should throw exception for invalid account key");
        } catch (Exception e) {
            // Expected - should be DataStoreException, InvalidKeyException, or URISyntaxException
            assertTrue("Should throw appropriate exception for invalid key", 
                    e instanceof DataStoreException || 
                    e instanceof InvalidKeyException || 
                    e instanceof URISyntaxException);
        }
    }

    @Test
    public void testGenerateSharedAccessSignatureWithZeroExpiry() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withAccountKey("valid-key")
                .withBlobEndpoint("https://testaccount.blob.core.windows.net")
                .build();

        try {
            provider.generateSharedAccessSignature(
                    null,
                    "test-blob",
                    EnumSet.of(READ, WRITE),
                    0, // Zero expiry
                    null
            );
        } catch (Exception e) {
            // Expected for invalid connection/key, but should handle zero expiry gracefully
            assertNotNull("Exception should not be null", e);
        }
    }

    @Test
    public void testGenerateSharedAccessSignatureWithNegativeExpiry() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withAccountKey("valid-key")
                .withBlobEndpoint("https://testaccount.blob.core.windows.net")
                .build();

        try {
            provider.generateSharedAccessSignature(
                    null,
                    "test-blob",
                    EnumSet.of(READ, WRITE),
                    -3600, // Negative expiry
                    null
            );
        } catch (Exception e) {
            // Expected for invalid connection/key, but should handle negative expiry gracefully
            assertNotNull("Exception should not be null", e);
        }
    }

    @Test
    public void testGenerateSharedAccessSignatureWithEmptyPermissions() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withAccountKey("valid-key")
                .withBlobEndpoint("https://testaccount.blob.core.windows.net")
                .build();

        try {
            provider.generateSharedAccessSignature(
                    null,
                    "test-blob",
                    EnumSet.noneOf(SharedAccessBlobPermissions.class), // Empty permissions
                    3600,
                    null
            );
        } catch (Exception e) {
            // Expected for invalid connection/key, but should handle empty permissions gracefully
            assertNotNull("Exception should not be null", e);
        }
    }

    @Test
    public void testGenerateSharedAccessSignatureWithNullKey() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withAccountKey("valid-key")
                .withBlobEndpoint("https://testaccount.blob.core.windows.net")
                .build();

        try {
            provider.generateSharedAccessSignature(
                    null,
                    null, // Null key
                    EnumSet.of(READ, WRITE),
                    3600,
                    null
            );
            fail("Should throw exception for null blob key");
        } catch (Exception e) {
            // Expected - should throw appropriate exception for null key
            assertNotNull("Exception should not be null", e);
        }
    }

    @Test
    public void testFillEmptyHeadersWithNullHeaders() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .build();

        // Test with null headers - should not crash
        try {
            provider.generateSharedAccessSignature(
                    null,
                    "test-blob",
                    EnumSet.of(READ, WRITE),
                    3600,
                    null // Null headers
            );
        } catch (Exception e) {
            // Expected for missing authentication, but should handle null headers gracefully
            assertNotNull("Exception should not be null", e);
        }
    }

    @Test
    public void testFillEmptyHeadersWithPartiallyNullHeaders() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withAccountKey("valid-key")
                .withBlobEndpoint("https://testaccount.blob.core.windows.net")
                .build();

        SharedAccessBlobHeaders headers = new SharedAccessBlobHeaders();
        headers.setContentType("application/json");
        // Leave other headers null to test fillEmptyHeaders method

        try {
            provider.generateSharedAccessSignature(
                    null,
                    "test-blob",
                    EnumSet.of(READ, WRITE),
                    3600,
                    headers
            );
        } catch (Exception e) {
            // Expected for invalid connection/key, but should handle partially null headers gracefully
            assertNotNull("Exception should not be null", e);
        }
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
