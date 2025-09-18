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

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.microsoft.azure.storage.StorageCredentialsToken;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.OffsetDateTime;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Test class specifically for testing token refresh functionality and service principal authentication
 * in AzureBlobContainerProviderV8.
 */
public class AzureBlobContainerProviderV8TokenRefreshTest {

    private static final String CONTAINER_NAME = "test-container";
    private static final String ACCOUNT_NAME = "testaccount";
    private static final String TENANT_ID = "test-tenant-id";
    private static final String CLIENT_ID = "test-client-id";
    private static final String CLIENT_SECRET = "test-client-secret";

    @Mock
    private ClientSecretCredential mockClientSecretCredential;

    @Mock
    private AccessToken mockAccessToken;

    @Mock
    private AccessToken mockNewAccessToken;

    @Mock
    private StorageCredentialsToken mockStorageCredentialsToken;

    @Mock
    private ScheduledExecutorService mockExecutorService;

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
    public void testServicePrincipalAuthenticationDetection() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withTenantId(TENANT_ID)
                .withClientId(CLIENT_ID)
                .withClientSecret(CLIENT_SECRET)
                .build();

        // Use reflection to test the private authenticateViaServicePrincipal method
        Method authenticateMethod = AzureBlobContainerProviderV8.class
                .getDeclaredMethod("authenticateViaServicePrincipal");
        authenticateMethod.setAccessible(true);
        
        boolean result = (Boolean) authenticateMethod.invoke(provider);
        assertTrue("Should authenticate via service principal when all credentials are present", result);
    }

    @Test
    public void testServicePrincipalAuthenticationNotDetectedWithConnectionString() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString("test-connection-string")
                .withAccountName(ACCOUNT_NAME)
                .withTenantId(TENANT_ID)
                .withClientId(CLIENT_ID)
                .withClientSecret(CLIENT_SECRET)
                .build();

        Method authenticateMethod = AzureBlobContainerProviderV8.class
                .getDeclaredMethod("authenticateViaServicePrincipal");
        authenticateMethod.setAccessible(true);
        
        boolean result = (Boolean) authenticateMethod.invoke(provider);
        assertFalse("Should not authenticate via service principal when connection string is present", result);
    }

    @Test
    public void testServicePrincipalAuthenticationNotDetectedWithMissingCredentials() throws Exception {
        provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(ACCOUNT_NAME)
                .withTenantId(TENANT_ID)
                .withClientId(CLIENT_ID)
                // Missing client secret
                .build();

        Method authenticateMethod = AzureBlobContainerProviderV8.class
                .getDeclaredMethod("authenticateViaServicePrincipal");
        authenticateMethod.setAccessible(true);
        
        boolean result = (Boolean) authenticateMethod.invoke(provider);
        assertFalse("Should not authenticate via service principal when credentials are missing", result);
    }

    @Test
    public void testTokenRefreshConstants() {
        // Test that the token refresh constants have expected values
        try {
            Field initialDelayField = AzureBlobContainerProviderV8.class
                    .getDeclaredField("TOKEN_REFRESHER_INITIAL_DELAY");
            initialDelayField.setAccessible(true);
            long initialDelay = (Long) initialDelayField.get(null);
            assertEquals("Initial delay should be 45 minutes", 45L, initialDelay);

            Field delayField = AzureBlobContainerProviderV8.class
                    .getDeclaredField("TOKEN_REFRESHER_DELAY");
            delayField.setAccessible(true);
            long delay = (Long) delayField.get(null);
            assertEquals("Delay should be 1 minute", 1L, delay);
        } catch (Exception e) {
            fail("Failed to access token refresh constants: " + e.getMessage());
        }
    }

    @Test
    public void testDefaultEndpointSuffixConstant() {
        try {
            Field endpointSuffixField = AzureBlobContainerProviderV8.class
                    .getDeclaredField("DEFAULT_ENDPOINT_SUFFIX");
            endpointSuffixField.setAccessible(true);
            String endpointSuffix = (String) endpointSuffixField.get(null);
            assertEquals("Default endpoint suffix should be core.windows.net", 
                    "core.windows.net", endpointSuffix);
        } catch (Exception e) {
            fail("Failed to access default endpoint suffix constant: " + e.getMessage());
        }
    }

    @Test
    public void testAzureDefaultScopeConstant() {
        try {
            Field scopeField = AzureBlobContainerProviderV8.class
                    .getDeclaredField("AZURE_DEFAULT_SCOPE");
            scopeField.setAccessible(true);
            String scope = (String) scopeField.get(null);
            assertEquals("Azure default scope should be https://storage.azure.com/.default", 
                    "https://storage.azure.com/.default", scope);
        } catch (Exception e) {
            fail("Failed to access Azure default scope constant: " + e.getMessage());
        }
    }

    @Test
    public void testInitializeWithPropertiesAllFields() {
        Properties properties = new Properties();
        properties.setProperty(AzureConstants.AZURE_CONNECTION_STRING, "test-connection");
        properties.setProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, "testaccount");
        properties.setProperty(AzureConstants.AZURE_BLOB_ENDPOINT, "https://test.blob.core.windows.net");
        properties.setProperty(AzureConstants.AZURE_SAS, "test-sas");
        properties.setProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY, "test-key");
        properties.setProperty(AzureConstants.AZURE_TENANT_ID, "test-tenant");
        properties.setProperty(AzureConstants.AZURE_CLIENT_ID, "test-client");
        properties.setProperty(AzureConstants.AZURE_CLIENT_SECRET, "test-secret");

        AzureBlobContainerProviderV8.Builder builder = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME);
        
        AzureBlobContainerProviderV8.Builder result = builder.initializeWithProperties(properties);
        
        assertSame("Builder should return itself for method chaining", builder, result);
        
        provider = builder.build();
        assertNotNull("Provider should not be null", provider);
        assertEquals("Container name should match", CONTAINER_NAME, provider.getContainerName());
    }

    @Test
    public void testInitializeWithPropertiesEmptyValues() {
        Properties properties = new Properties();
        // Set empty values to test default behavior
        properties.setProperty(AzureConstants.AZURE_CONNECTION_STRING, "");
        properties.setProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, "");

        AzureBlobContainerProviderV8.Builder builder = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME);
        
        builder.initializeWithProperties(properties);
        provider = builder.build();
        
        assertNotNull("Provider should not be null", provider);
        assertEquals("Container name should match", CONTAINER_NAME, provider.getContainerName());
    }

    @Test
    public void testBuilderMethodChaining() {
        AzureBlobContainerProviderV8.Builder builder = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME);

        // Test that all builder methods return the builder for method chaining
        assertSame("withAzureConnectionString should return builder", builder, 
                builder.withAzureConnectionString("test"));
        assertSame("withAccountName should return builder", builder, 
                builder.withAccountName("test"));
        assertSame("withBlobEndpoint should return builder", builder, 
                builder.withBlobEndpoint("test"));
        assertSame("withSasToken should return builder", builder, 
                builder.withSasToken("test"));
        assertSame("withAccountKey should return builder", builder, 
                builder.withAccountKey("test"));
        assertSame("withTenantId should return builder", builder, 
                builder.withTenantId("test"));
        assertSame("withClientId should return builder", builder, 
                builder.withClientId("test"));
        assertSame("withClientSecret should return builder", builder, 
                builder.withClientSecret("test"));

        provider = builder.build();
        assertNotNull("Provider should not be null", provider);
    }

    @Test
    public void testBuilderStaticFactoryMethod() {
        AzureBlobContainerProviderV8.Builder builder = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME);
        
        assertNotNull("Builder should not be null", builder);
        
        provider = builder.build();
        assertNotNull("Provider should not be null", provider);
        assertEquals("Container name should match", CONTAINER_NAME, provider.getContainerName());
    }
}
