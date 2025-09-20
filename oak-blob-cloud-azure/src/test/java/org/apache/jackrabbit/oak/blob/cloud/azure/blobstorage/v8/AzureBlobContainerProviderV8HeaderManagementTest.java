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
import org.junit.After;
import org.junit.Test;

import java.lang.reflect.Method;

import static org.junit.Assert.*;

/**
 * Test class focused on AzureBlobContainerProviderV8 header management functionality.
 * Tests SharedAccessBlobHeaders handling and the fillEmptyHeaders functionality.
 */
public class AzureBlobContainerProviderV8HeaderManagementTest {

    private static final String CONTAINER_NAME = "test-container";
    private static final String CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=testaccount;AccountKey=dGVzdC1hY2NvdW50LWtleQ==;EndpointSuffix=core.windows.net";
    private static final String ACCOUNT_NAME = "testaccount";
    private static final String TENANT_ID = "test-tenant-id";
    private static final String CLIENT_ID = "test-client-id";
    private static final String CLIENT_SECRET = "test-client-secret";

    private AzureBlobContainerProviderV8 provider;

    @After
    public void tearDown() throws Exception {
        if (provider != null) {
            provider.close();
        }
    }

    @Test
    public void testFillEmptyHeadersWithNullHeaders() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString(CONNECTION_STRING)
                .build();

        // Test fillEmptyHeaders with null headers
        Method fillEmptyHeadersMethod = AzureBlobContainerProviderV8.class
                .getDeclaredMethod("fillEmptyHeaders", SharedAccessBlobHeaders.class);
        fillEmptyHeadersMethod.setAccessible(true);

        // Should not throw exception when called with null
        fillEmptyHeadersMethod.invoke(provider, (SharedAccessBlobHeaders) null);
    }

    @Test
    public void testFillEmptyHeadersWithAllEmptyHeaders() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString(CONNECTION_STRING)
                .build();

        SharedAccessBlobHeaders headers = new SharedAccessBlobHeaders();
        // All headers are null/empty by default

        Method fillEmptyHeadersMethod = AzureBlobContainerProviderV8.class
                .getDeclaredMethod("fillEmptyHeaders", SharedAccessBlobHeaders.class);
        fillEmptyHeadersMethod.setAccessible(true);

        fillEmptyHeadersMethod.invoke(provider, headers);

        // Verify all headers are set to empty string
        assertEquals("Cache control should be empty string", "", headers.getCacheControl());
        assertEquals("Content disposition should be empty string", "", headers.getContentDisposition());
        assertEquals("Content encoding should be empty string", "", headers.getContentEncoding());
        assertEquals("Content language should be empty string", "", headers.getContentLanguage());
        assertEquals("Content type should be empty string", "", headers.getContentType());
    }

    @Test
    public void testFillEmptyHeadersWithSomePopulatedHeaders() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString(CONNECTION_STRING)
                .build();

        SharedAccessBlobHeaders headers = new SharedAccessBlobHeaders();
        headers.setContentType("application/json");
        headers.setCacheControl("no-cache");
        // Leave other headers null/empty

        Method fillEmptyHeadersMethod = AzureBlobContainerProviderV8.class
                .getDeclaredMethod("fillEmptyHeaders", SharedAccessBlobHeaders.class);
        fillEmptyHeadersMethod.setAccessible(true);

        fillEmptyHeadersMethod.invoke(provider, headers);

        // Verify populated headers remain unchanged
        assertEquals("Content type should remain unchanged", "application/json", headers.getContentType());
        assertEquals("Cache control should remain unchanged", "no-cache", headers.getCacheControl());

        // Verify empty headers are set to empty string
        assertEquals("Content disposition should be empty string", "", headers.getContentDisposition());
        assertEquals("Content encoding should be empty string", "", headers.getContentEncoding());
        assertEquals("Content language should be empty string", "", headers.getContentLanguage());
    }

    @Test
    public void testFillEmptyHeadersWithNullHeadersAdvanced() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withTenantId(TENANT_ID)
                .withClientId(CLIENT_ID)
                .withClientSecret(CLIENT_SECRET)
                .build();

        Method fillEmptyHeadersMethod = AzureBlobContainerProviderV8.class
                .getDeclaredMethod("fillEmptyHeaders", SharedAccessBlobHeaders.class);
        fillEmptyHeadersMethod.setAccessible(true);

        // Test with null headers - should not throw exception
        fillEmptyHeadersMethod.invoke(provider, (SharedAccessBlobHeaders) null);
    }

    @Test
    public void testFillEmptyHeadersWithPartiallyFilledHeaders() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withTenantId(TENANT_ID)
                .withClientId(CLIENT_ID)
                .withClientSecret(CLIENT_SECRET)
                .build();

        Method fillEmptyHeadersMethod = AzureBlobContainerProviderV8.class
                .getDeclaredMethod("fillEmptyHeaders", SharedAccessBlobHeaders.class);
        fillEmptyHeadersMethod.setAccessible(true);

        SharedAccessBlobHeaders headers = new SharedAccessBlobHeaders();
        headers.setContentType("text/plain"); // Set one header
        // Leave others null/blank

        fillEmptyHeadersMethod.invoke(provider, headers);

        // Verify that empty headers were filled with empty strings
        assertEquals("Content type should remain unchanged", "text/plain", headers.getContentType());
        assertEquals("Cache control should be empty string", "", headers.getCacheControl());
        assertEquals("Content disposition should be empty string", "", headers.getContentDisposition());
        assertEquals("Content encoding should be empty string", "", headers.getContentEncoding());
        assertEquals("Content language should be empty string", "", headers.getContentLanguage());
    }

    @Test
    public void testFillEmptyHeadersWithBlankHeaders() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withTenantId(TENANT_ID)
                .withClientId(CLIENT_ID)
                .withClientSecret(CLIENT_SECRET)
                .build();

        Method fillEmptyHeadersMethod = AzureBlobContainerProviderV8.class
                .getDeclaredMethod("fillEmptyHeaders", SharedAccessBlobHeaders.class);
        fillEmptyHeadersMethod.setAccessible(true);

        SharedAccessBlobHeaders headers = new SharedAccessBlobHeaders();
        headers.setContentType("   "); // Set blank header
        headers.setCacheControl(""); // Set empty header

        fillEmptyHeadersMethod.invoke(provider, headers);

        // Verify that blank headers were replaced with empty strings
        assertEquals("Content type should be empty string", "", headers.getContentType());
        assertEquals("Cache control should be empty string", "", headers.getCacheControl());
        assertEquals("Content disposition should be empty string", "", headers.getContentDisposition());
        assertEquals("Content encoding should be empty string", "", headers.getContentEncoding());
        assertEquals("Content language should be empty string", "", headers.getContentLanguage());
    }

    @Test
    public void testFillEmptyHeadersWithMixedHeaders() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString(CONNECTION_STRING)
                .build();

        Method fillEmptyHeadersMethod = AzureBlobContainerProviderV8.class
                .getDeclaredMethod("fillEmptyHeaders", SharedAccessBlobHeaders.class);
        fillEmptyHeadersMethod.setAccessible(true);

        SharedAccessBlobHeaders headers = new SharedAccessBlobHeaders();
        headers.setContentType("application/pdf"); // Valid header
        headers.setCacheControl("   "); // Blank header
        headers.setContentDisposition(""); // Empty header
        // Leave content encoding and language as null

        fillEmptyHeadersMethod.invoke(provider, headers);

        // Verify results
        assertEquals("Content type should remain unchanged", "application/pdf", headers.getContentType());
        assertEquals("Cache control should be empty string", "", headers.getCacheControl());
        assertEquals("Content disposition should be empty string", "", headers.getContentDisposition());
        assertEquals("Content encoding should be empty string", "", headers.getContentEncoding());
        assertEquals("Content language should be empty string", "", headers.getContentLanguage());
    }

    @Test
    public void testFillEmptyHeadersWithAllValidHeaders() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString(CONNECTION_STRING)
                .build();

        Method fillEmptyHeadersMethod = AzureBlobContainerProviderV8.class
                .getDeclaredMethod("fillEmptyHeaders", SharedAccessBlobHeaders.class);
        fillEmptyHeadersMethod.setAccessible(true);

        SharedAccessBlobHeaders headers = new SharedAccessBlobHeaders();
        headers.setContentType("text/html");
        headers.setCacheControl("max-age=3600");
        headers.setContentDisposition("inline");
        headers.setContentEncoding("gzip");
        headers.setContentLanguage("en-US");

        fillEmptyHeadersMethod.invoke(provider, headers);

        // Verify all headers remain unchanged when they have valid values
        assertEquals("Content type should remain unchanged", "text/html", headers.getContentType());
        assertEquals("Cache control should remain unchanged", "max-age=3600", headers.getCacheControl());
        assertEquals("Content disposition should remain unchanged", "inline", headers.getContentDisposition());
        assertEquals("Content encoding should remain unchanged", "gzip", headers.getContentEncoding());
        assertEquals("Content language should remain unchanged", "en-US", headers.getContentLanguage());
    }

    @Test
    public void testFillEmptyHeadersWithWhitespaceOnlyHeaders() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString(CONNECTION_STRING)
                .build();

        Method fillEmptyHeadersMethod = AzureBlobContainerProviderV8.class
                .getDeclaredMethod("fillEmptyHeaders", SharedAccessBlobHeaders.class);
        fillEmptyHeadersMethod.setAccessible(true);

        SharedAccessBlobHeaders headers = new SharedAccessBlobHeaders();
        headers.setContentType("\t\n\r "); // Whitespace only
        headers.setCacheControl("  \t  "); // Tabs and spaces
        headers.setContentDisposition("\n\r"); // Newlines only

        fillEmptyHeadersMethod.invoke(provider, headers);

        // Verify that whitespace-only headers are replaced with empty strings
        assertEquals("Content type should be empty string", "", headers.getContentType());
        assertEquals("Cache control should be empty string", "", headers.getCacheControl());
        assertEquals("Content disposition should be empty string", "", headers.getContentDisposition());
        assertEquals("Content encoding should be empty string", "", headers.getContentEncoding());
        assertEquals("Content language should be empty string", "", headers.getContentLanguage());
    }

    @Test
    public void testFillEmptyHeadersIdempotency() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString(CONNECTION_STRING)
                .build();

        Method fillEmptyHeadersMethod = AzureBlobContainerProviderV8.class
                .getDeclaredMethod("fillEmptyHeaders", SharedAccessBlobHeaders.class);
        fillEmptyHeadersMethod.setAccessible(true);

        SharedAccessBlobHeaders headers = new SharedAccessBlobHeaders();
        headers.setContentType("application/xml");
        // Leave others null/empty

        // Call fillEmptyHeaders twice
        fillEmptyHeadersMethod.invoke(provider, headers);
        fillEmptyHeadersMethod.invoke(provider, headers);

        // Verify results are the same after multiple calls
        assertEquals("Content type should remain unchanged", "application/xml", headers.getContentType());
        assertEquals("Cache control should be empty string", "", headers.getCacheControl());
        assertEquals("Content disposition should be empty string", "", headers.getContentDisposition());
        assertEquals("Content encoding should be empty string", "", headers.getContentEncoding());
        assertEquals("Content language should be empty string", "", headers.getContentLanguage());
    }
}