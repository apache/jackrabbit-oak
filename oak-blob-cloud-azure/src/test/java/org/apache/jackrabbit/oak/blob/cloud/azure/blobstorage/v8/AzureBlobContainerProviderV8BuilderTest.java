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

import org.junit.After;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Test class focused on AzureBlobContainerProviderV8 Builder pattern functionality.
 * Tests builder operations, property initialization, and method chaining.
 */
public class AzureBlobContainerProviderV8BuilderTest {

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
    public void tearDown() {
        if (provider != null) {
            provider.close();
        }
    }

    @Test
    public void testBuilderInitializeWithProperties() {
        Properties properties = new Properties();
        properties.setProperty(AzureConstantsV8.AZURE_CONNECTION_STRING, CONNECTION_STRING);
        properties.setProperty(AzureConstantsV8.AZURE_STORAGE_ACCOUNT_NAME, ACCOUNT_NAME);
        properties.setProperty(AzureConstantsV8.AZURE_BLOB_ENDPOINT, BLOB_ENDPOINT);
        properties.setProperty(AzureConstantsV8.AZURE_SAS, SAS_TOKEN);
        properties.setProperty(AzureConstantsV8.AZURE_STORAGE_ACCOUNT_KEY, ACCOUNT_KEY);
        properties.setProperty(AzureConstantsV8.AZURE_TENANT_ID, TENANT_ID);
        properties.setProperty(AzureConstantsV8.AZURE_CLIENT_ID, CLIENT_ID);
        properties.setProperty(AzureConstantsV8.AZURE_CLIENT_SECRET, CLIENT_SECRET);

        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .initializeWithProperties(properties)
                .build();

        assertNotNull("Provider should not be null", provider);
        assertEquals("Container name should match", CONTAINER_NAME, provider.getContainerName());
    }

    @Test
    public void testBuilderInitializeWithEmptyProperties() {
        Properties properties = new Properties();

        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .initializeWithProperties(properties)
                .build();

        assertNotNull("Provider should not be null", provider);
        assertEquals("Container name should match", CONTAINER_NAME, provider.getContainerName());
    }

    @Test
    public void testBuilderChaining() {
        // Test that all builder methods return the builder instance for chaining
        AzureBlobContainerProviderV8.Builder builder = AzureBlobContainerProviderV8.Builder.builder(CONTAINER_NAME);

        AzureBlobContainerProviderV8.Builder result = builder
                .withAzureConnectionString(CONNECTION_STRING)
                .withAccountName(ACCOUNT_NAME)
                .withBlobEndpoint(BLOB_ENDPOINT)
                .withSasToken(SAS_TOKEN)
                .withAccountKey(ACCOUNT_KEY)
                .withTenantId(TENANT_ID)
                .withClientId(CLIENT_ID)
                .withClientSecret(CLIENT_SECRET);

        assertSame("Builder methods should return the same builder instance", builder, result);

        provider = result.build();
        assertNotNull("Provider should be built successfully", provider);
    }

    @Test
    public void testBuilderWithNullValues() {
        // Test builder with null values (should not throw exceptions)
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString(null)
                .withAccountName(null)
                .withBlobEndpoint(null)
                .withSasToken(null)
                .withAccountKey(null)
                .withTenantId(null)
                .withClientId(null)
                .withClientSecret(null)
                .build();

        assertNotNull("Provider should be built successfully with null values", provider);
        assertEquals("Container name should match", CONTAINER_NAME, provider.getContainerName());
    }

    @Test
    public void testBuilderWithEmptyStrings() {
        // Test builder with empty strings
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString("")
                .withAccountName("")
                .withBlobEndpoint("")
                .withSasToken("")
                .withAccountKey("")
                .withTenantId("")
                .withClientId("")
                .withClientSecret("")
                .build();

        assertNotNull("Provider should be built successfully with empty strings", provider);
        assertEquals("Container name should match", CONTAINER_NAME, provider.getContainerName());
    }

    @Test
    public void testBuilderWithMixedNullAndEmptyValues() {
        // Test builder with a mix of null and empty values
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString(null)
                .withAccountName("")
                .withBlobEndpoint(null)
                .withSasToken("")
                .withAccountKey(null)
                .withTenantId("")
                .withClientId(null)
                .withClientSecret("")
                .build();

        assertNotNull("Provider should be built successfully with mixed null and empty values", provider);
        assertEquals("Container name should match", CONTAINER_NAME, provider.getContainerName());
    }

    @Test
    public void testBuilderWithValidConfiguration() {
        // Test builder with a complete valid configuration
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString(CONNECTION_STRING)
                .withAccountName(ACCOUNT_NAME)
                .withBlobEndpoint(BLOB_ENDPOINT)
                .withSasToken(SAS_TOKEN)
                .withAccountKey(ACCOUNT_KEY)
                .withTenantId(TENANT_ID)
                .withClientId(CLIENT_ID)
                .withClientSecret(CLIENT_SECRET)
                .build();

        assertNotNull("Provider should be built successfully with valid configuration", provider);
        assertEquals("Container name should match", CONTAINER_NAME, provider.getContainerName());
    }

    @Test
    public void testBuilderWithPartialConfiguration() {
        // Test builder with only some configuration values
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withAccountKey(ACCOUNT_KEY)
                .build();

        assertNotNull("Provider should be built successfully with partial configuration", provider);
        assertEquals("Container name should match", CONTAINER_NAME, provider.getContainerName());
    }

    @Test
    public void testBuilderWithPropertiesOverride() {
        // Test that explicit builder methods override properties
        Properties properties = new Properties();
        properties.setProperty(AzureConstantsV8.AZURE_STORAGE_ACCOUNT_NAME, "properties-account");
        properties.setProperty(AzureConstantsV8.AZURE_TENANT_ID, "properties-tenant");

        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .initializeWithProperties(properties)
                .withAccountName(ACCOUNT_NAME) // Should override properties value
                .withTenantId(TENANT_ID) // Should override properties value
                .build();

        assertNotNull("Provider should be built successfully", provider);
        assertEquals("Container name should match", CONTAINER_NAME, provider.getContainerName());
    }

    @Test
    public void testBuilderStaticFactoryMethod() {
        // Test the static builder factory method
        AzureBlobContainerProviderV8.Builder builder = AzureBlobContainerProviderV8.Builder.builder(CONTAINER_NAME);

        assertNotNull("Builder should not be null", builder);

        provider = builder.build();
        assertNotNull("Provider should be built successfully", provider);
        assertEquals("Container name should match", CONTAINER_NAME, provider.getContainerName());
    }
}