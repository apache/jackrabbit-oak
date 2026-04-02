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

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.RetryExponentialRetry;
import com.microsoft.azure.storage.RetryNoRetry;
import com.microsoft.azure.storage.RetryPolicy;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.jackrabbit.oak.spi.blob.data.DataStoreException;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants;
import org.junit.After;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class UtilsV8Test {

    private static final String VALID_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=testaccount;AccountKey=dGVzdGtleQ==;EndpointSuffix=core.windows.net";
    private static final String TEST_CONTAINER_NAME = "test-container";

    @After
    public void tearDown() {
        // Reset the default proxy after each test
        OperationContext.setDefaultProxy(null);
    }

    // ========== Constants Tests ==========

    @Test
    public void testConstants() {
        assertEquals("azure.properties", UtilsV8.DEFAULT_CONFIG_FILE);
        assertEquals("-", UtilsV8.DASH);
    }

    @Test
    public void testPrivateConstructor() throws Exception {
        Constructor<UtilsV8> constructor = UtilsV8.class.getDeclaredConstructor();
        assertTrue("Constructor should be private", java.lang.reflect.Modifier.isPrivate(constructor.getModifiers()));

        constructor.setAccessible(true);
        UtilsV8 instance = constructor.newInstance();
        assertNotNull("Should be able to create instance via reflection", instance);
    }

    // ========== Connection String Tests ==========

    @Test
    public void testConnectionStringIsBasedOnProperty() {
        Properties properties = new Properties();
        properties.put(AzureConstants.AZURE_CONNECTION_STRING, "DefaultEndpointsProtocol=https;AccountName=accountName;AccountKey=accountKey");
        String connectionString = UtilsV8.getConnectionStringFromProperties(properties);
        assertEquals("DefaultEndpointsProtocol=https;AccountName=accountName;AccountKey=accountKey", connectionString);
    }

    @Test
    public void testConnectionStringIsBasedOnSAS() {
        Properties properties = new Properties();
        properties.put(AzureConstants.AZURE_SAS, "sas");
        properties.put(AzureConstants.AZURE_BLOB_ENDPOINT, "endpoint");
        String connectionString = UtilsV8.getConnectionStringFromProperties(properties);
        assertEquals(connectionString,
                String.format("BlobEndpoint=%s;SharedAccessSignature=%s", "endpoint", "sas"));
    }

    @Test
    public void testConnectionStringIsBasedOnSASWithoutEndpoint() {
        Properties properties = new Properties();
        properties.put(AzureConstants.AZURE_SAS, "sas");
        properties.put(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, "account");
        String connectionString = UtilsV8.getConnectionStringFromProperties(properties);
        assertEquals(connectionString,
                String.format("AccountName=%s;SharedAccessSignature=%s", "account", "sas"));
    }

    @Test
    public void testConnectionStringIsBasedOnAccessKeyIfSASMissing() {
        Properties properties = new Properties();
        properties.put(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, "accessKey");
        properties.put(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY, "secretKey");

        String connectionString = UtilsV8.getConnectionStringFromProperties(properties);
        assertEquals(connectionString,
                String.format("DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s","accessKey","secretKey"));
    }

    @Test
    public void testConnectionStringSASIsPriority() {
        Properties properties = new Properties();
        properties.put(AzureConstants.AZURE_SAS, "sas");
        properties.put(AzureConstants.AZURE_BLOB_ENDPOINT, "endpoint");

        properties.put(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, "accessKey");
        properties.put(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY, "secretKey");

        String connectionString = UtilsV8.getConnectionStringFromProperties(properties);
        assertEquals(connectionString,
                String.format("BlobEndpoint=%s;SharedAccessSignature=%s", "endpoint", "sas"));
    }

    @Test
    public void testConnectionStringFromPropertiesWithEmptyProperties() {
        Properties properties = new Properties();
        String connectionString = UtilsV8.getConnectionStringFromProperties(properties);
        assertEquals("DefaultEndpointsProtocol=https;AccountName=;AccountKey=", connectionString);
    }

    @Test
    public void testConnectionStringFromPropertiesWithNullValues() {
        Properties properties = new Properties();
        // Properties.put() doesn't accept null values, so we test with empty strings instead
        properties.put(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, "");
        properties.put(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY, "");
        String connectionString = UtilsV8.getConnectionStringFromProperties(properties);
        assertEquals("DefaultEndpointsProtocol=https;AccountName=;AccountKey=", connectionString);
    }

    // ========== Connection String Builder Tests ==========

    @Test
    public void testGetConnectionStringWithAccountNameAndKey() {
        String connectionString = UtilsV8.getConnectionString("testAccount", "testKey");
        assertEquals("DefaultEndpointsProtocol=https;AccountName=testAccount;AccountKey=testKey", connectionString);
    }

    @Test
    public void testGetConnectionStringWithAccountNameKeyAndEndpoint() {
        String connectionString = UtilsV8.getConnectionString("testAccount", "testKey", "https://custom.endpoint.com");
        assertEquals("DefaultEndpointsProtocol=https;AccountName=testAccount;AccountKey=testKey;BlobEndpoint=https://custom.endpoint.com", connectionString);
    }

    @Test
    public void testGetConnectionStringWithNullEndpoint() {
        String connectionString = UtilsV8.getConnectionString("testAccount", "testKey", null);
        assertEquals("DefaultEndpointsProtocol=https;AccountName=testAccount;AccountKey=testKey", connectionString);
    }

    @Test
    public void testGetConnectionStringWithEmptyEndpoint() {
        String connectionString = UtilsV8.getConnectionString("testAccount", "testKey", "");
        assertEquals("DefaultEndpointsProtocol=https;AccountName=testAccount;AccountKey=testKey", connectionString);
    }

    @Test
    public void testGetConnectionStringForSasWithEndpoint() {
        String connectionString = UtilsV8.getConnectionStringForSas("sasToken", "https://endpoint.com", "account");
        assertEquals("BlobEndpoint=https://endpoint.com;SharedAccessSignature=sasToken", connectionString);
    }

    @Test
    public void testGetConnectionStringForSasWithoutEndpoint() {
        String connectionString = UtilsV8.getConnectionStringForSas("sasToken", "", "account");
        assertEquals("AccountName=account;SharedAccessSignature=sasToken", connectionString);
    }

    @Test
    public void testGetConnectionStringForSasWithNullEndpoint() {
        String connectionString = UtilsV8.getConnectionStringForSas("sasToken", null, "account");
        assertEquals("AccountName=account;SharedAccessSignature=sasToken", connectionString);
    }

    // ========== Retry Policy Tests ==========

    @Test
    public void testGetRetryPolicyWithNegativeRetries() {
        RetryPolicy policy = UtilsV8.getRetryPolicy("-1");
        assertNull("Should return null for negative retries", policy);
    }

    @Test
    public void testGetRetryPolicyWithZeroRetries() {
        RetryPolicy policy = UtilsV8.getRetryPolicy("0");
        assertNotNull("Should return RetryNoRetry for zero retries", policy);
        assertTrue("Should be instance of RetryNoRetry", policy instanceof RetryNoRetry);
    }

    @Test
    public void testGetRetryPolicyWithPositiveRetries() {
        RetryPolicy policy = UtilsV8.getRetryPolicy("3");
        assertNotNull("Should return RetryExponentialRetry for positive retries", policy);
        assertTrue("Should be instance of RetryExponentialRetry", policy instanceof RetryExponentialRetry);
    }

    @Test
    public void testGetRetryPolicyWithInvalidString() {
        RetryPolicy policy = UtilsV8.getRetryPolicy("invalid");
        assertNull("Should return null for invalid string", policy);
    }

    @Test
    public void testGetRetryPolicyWithNullString() {
        RetryPolicy policy = UtilsV8.getRetryPolicy(null);
        assertNull("Should return null for null string", policy);
    }

    @Test
    public void testGetRetryPolicyWithEmptyString() {
        RetryPolicy policy = UtilsV8.getRetryPolicy("");
        assertNull("Should return null for empty string", policy);
    }

    // ========== Proxy Configuration Tests ==========

    @Test
    public void testSetProxyIfNeededWithValidProxySettings() {
        Properties properties = new Properties();
        properties.setProperty(AzureConstants.PROXY_HOST, "proxy.example.com");
        properties.setProperty(AzureConstants.PROXY_PORT, "8080");

        UtilsV8.setProxyIfNeeded(properties);

        // After the bug fix, proxy should now be set correctly
        Proxy proxy = OperationContext.getDefaultProxy();
        assertNotNull("Proxy should be set with valid host and port", proxy);
        assertEquals("Proxy type should be HTTP", Proxy.Type.HTTP, proxy.type());

        InetSocketAddress address = (InetSocketAddress) proxy.address();
        assertEquals("Proxy host should match", "proxy.example.com", address.getHostName());
        assertEquals("Proxy port should match", 8080, address.getPort());
    }

    @Test
    public void testSetProxyIfNeededWithMissingProxyHost() {
        Properties properties = new Properties();
        properties.setProperty(AzureConstants.PROXY_PORT, "8080");

        UtilsV8.setProxyIfNeeded(properties);
        assertNull("Proxy should not be set when host is missing", OperationContext.getDefaultProxy());
    }

    @Test
    public void testSetProxyIfNeededWithMissingProxyPort() {
        Properties properties = new Properties();
        properties.setProperty(AzureConstants.PROXY_HOST, "proxy.example.com");
        // Missing port property - proxy should not be set

        UtilsV8.setProxyIfNeeded(properties);
        assertNull("Proxy should not be set when port is missing", OperationContext.getDefaultProxy());
    }

    @Test
    public void testSetProxyIfNeededWithEmptyProperties() {
        Properties properties = new Properties();
        UtilsV8.setProxyIfNeeded(properties);
        assertNull("Proxy should not be set with empty properties", OperationContext.getDefaultProxy());
    }

    @Test
    public void testSetProxyIfNeededWithNullHost() {
        Properties properties = new Properties();
        properties.setProperty(AzureConstants.PROXY_HOST, "");
        properties.setProperty(AzureConstants.PROXY_PORT, "8080");

        UtilsV8.setProxyIfNeeded(properties);
        assertNull("Proxy should not be set with empty host", OperationContext.getDefaultProxy());
    }

    @Test
    public void testSetProxyIfNeededWithEmptyPort() {
        Properties properties = new Properties();
        properties.setProperty(AzureConstants.PROXY_HOST, "proxy.example.com");
        properties.setProperty(AzureConstants.PROXY_PORT, "");
        // Empty port string - proxy should not be set

        UtilsV8.setProxyIfNeeded(properties);
        assertNull("Proxy should not be set with empty port", OperationContext.getDefaultProxy());
    }

    @Test(expected = NumberFormatException.class)
    public void testSetProxyIfNeededWithInvalidPort() {
        Properties properties = new Properties();
        properties.setProperty(AzureConstants.PROXY_HOST, "proxy.example.com");
        properties.setProperty(AzureConstants.PROXY_PORT, "invalid");

        // After the bug fix, this should now throw NumberFormatException
        UtilsV8.setProxyIfNeeded(properties);
    }

    // ========== Blob Client Tests ==========

    @Test
    public void testGetBlobClientWithConnectionString() throws Exception {
        try (MockedStatic<CloudStorageAccount> mockedAccount = mockStatic(CloudStorageAccount.class)) {
            CloudStorageAccount mockAccount = mock(CloudStorageAccount.class);
            CloudBlobClient mockClient = mock(CloudBlobClient.class);

            mockedAccount.when(() -> CloudStorageAccount.parse(VALID_CONNECTION_STRING))
                    .thenReturn(mockAccount);
            when(mockAccount.createCloudBlobClient()).thenReturn(mockClient);

            CloudBlobClient result = UtilsV8.getBlobClient(VALID_CONNECTION_STRING);

            assertNotNull("Should return a CloudBlobClient", result);
            assertEquals("Should return the mocked client", mockClient, result);
            verify(mockAccount).createCloudBlobClient();
            verify(mockClient, never()).setDefaultRequestOptions(any());
        }
    }

    @Test
    public void testGetBlobClientWithConnectionStringAndRequestOptions() throws Exception {
        try (MockedStatic<CloudStorageAccount> mockedAccount = mockStatic(CloudStorageAccount.class)) {
            CloudStorageAccount mockAccount = mock(CloudStorageAccount.class);
            CloudBlobClient mockClient = mock(CloudBlobClient.class);
            BlobRequestOptions requestOptions = new BlobRequestOptions();

            mockedAccount.when(() -> CloudStorageAccount.parse(VALID_CONNECTION_STRING))
                    .thenReturn(mockAccount);
            when(mockAccount.createCloudBlobClient()).thenReturn(mockClient);

            CloudBlobClient result = UtilsV8.getBlobClient(VALID_CONNECTION_STRING, requestOptions);

            assertNotNull("Should return a CloudBlobClient", result);
            assertEquals("Should return the mocked client", mockClient, result);
            verify(mockAccount).createCloudBlobClient();
            verify(mockClient).setDefaultRequestOptions(requestOptions);
        }
    }

    @Test
    public void testGetBlobClientWithConnectionStringAndNullRequestOptions() throws Exception {
        try (MockedStatic<CloudStorageAccount> mockedAccount = mockStatic(CloudStorageAccount.class)) {
            CloudStorageAccount mockAccount = mock(CloudStorageAccount.class);
            CloudBlobClient mockClient = mock(CloudBlobClient.class);

            mockedAccount.when(() -> CloudStorageAccount.parse(VALID_CONNECTION_STRING))
                    .thenReturn(mockAccount);
            when(mockAccount.createCloudBlobClient()).thenReturn(mockClient);

            CloudBlobClient result = UtilsV8.getBlobClient(VALID_CONNECTION_STRING, null);

            assertNotNull("Should return a CloudBlobClient", result);
            assertEquals("Should return the mocked client", mockClient, result);
            verify(mockAccount).createCloudBlobClient();
            verify(mockClient, never()).setDefaultRequestOptions(any());
        }
    }

    @Test(expected = URISyntaxException.class)
    public void testGetBlobClientWithInvalidConnectionString() throws Exception {
        try (MockedStatic<CloudStorageAccount> mockedAccount = mockStatic(CloudStorageAccount.class)) {
            mockedAccount.when(() -> CloudStorageAccount.parse("invalid-connection-string"))
                    .thenThrow(new URISyntaxException("invalid-connection-string", "Invalid URI"));

            UtilsV8.getBlobClient("invalid-connection-string");
        }
    }

    @Test(expected = InvalidKeyException.class)
    public void testGetBlobClientWithInvalidKey() throws Exception {
        try (MockedStatic<CloudStorageAccount> mockedAccount = mockStatic(CloudStorageAccount.class)) {
            mockedAccount.when(() -> CloudStorageAccount.parse(anyString()))
                    .thenThrow(new InvalidKeyException("Invalid key"));

            UtilsV8.getBlobClient("DefaultEndpointsProtocol=https;AccountName=test;AccountKey=invalid");
        }
    }

    // ========== Blob Container Tests ==========

    @Test
    public void testGetBlobContainerWithConnectionStringAndContainerName() throws Exception {
        try (MockedStatic<CloudStorageAccount> mockedAccount = mockStatic(CloudStorageAccount.class)) {
            CloudStorageAccount mockAccount = mock(CloudStorageAccount.class);
            CloudBlobClient mockClient = mock(CloudBlobClient.class);
            CloudBlobContainer mockContainer = mock(CloudBlobContainer.class);

            mockedAccount.when(() -> CloudStorageAccount.parse(VALID_CONNECTION_STRING))
                    .thenReturn(mockAccount);
            when(mockAccount.createCloudBlobClient()).thenReturn(mockClient);
            when(mockClient.getContainerReference(TEST_CONTAINER_NAME)).thenReturn(mockContainer);

            CloudBlobContainer result = UtilsV8.getBlobContainer(VALID_CONNECTION_STRING, TEST_CONTAINER_NAME);

            assertNotNull("Should return a CloudBlobContainer", result);
            assertEquals("Should return the mocked container", mockContainer, result);
            verify(mockClient).getContainerReference(TEST_CONTAINER_NAME);
        }
    }

    @Test
    public void testGetBlobContainerWithConnectionStringContainerNameAndRequestOptions() throws Exception {
        try (MockedStatic<CloudStorageAccount> mockedAccount = mockStatic(CloudStorageAccount.class)) {
            CloudStorageAccount mockAccount = mock(CloudStorageAccount.class);
            CloudBlobClient mockClient = mock(CloudBlobClient.class);
            CloudBlobContainer mockContainer = mock(CloudBlobContainer.class);
            BlobRequestOptions requestOptions = new BlobRequestOptions();

            mockedAccount.when(() -> CloudStorageAccount.parse(VALID_CONNECTION_STRING))
                    .thenReturn(mockAccount);
            when(mockAccount.createCloudBlobClient()).thenReturn(mockClient);
            when(mockClient.getContainerReference(TEST_CONTAINER_NAME)).thenReturn(mockContainer);

            CloudBlobContainer result = UtilsV8.getBlobContainer(VALID_CONNECTION_STRING, TEST_CONTAINER_NAME, requestOptions);

            assertNotNull("Should return a CloudBlobContainer", result);
            assertEquals("Should return the mocked container", mockContainer, result);
            verify(mockClient).getContainerReference(TEST_CONTAINER_NAME);
            verify(mockClient).setDefaultRequestOptions(requestOptions);
        }
    }

    @Test
    public void testGetBlobContainerWithNullRequestOptions() throws Exception {
        try (MockedStatic<CloudStorageAccount> mockedAccount = mockStatic(CloudStorageAccount.class)) {
            CloudStorageAccount mockAccount = mock(CloudStorageAccount.class);
            CloudBlobClient mockClient = mock(CloudBlobClient.class);
            CloudBlobContainer mockContainer = mock(CloudBlobContainer.class);

            mockedAccount.when(() -> CloudStorageAccount.parse(VALID_CONNECTION_STRING))
                    .thenReturn(mockAccount);
            when(mockAccount.createCloudBlobClient()).thenReturn(mockClient);
            when(mockClient.getContainerReference(TEST_CONTAINER_NAME)).thenReturn(mockContainer);

            CloudBlobContainer result = UtilsV8.getBlobContainer(VALID_CONNECTION_STRING, TEST_CONTAINER_NAME, null);

            assertNotNull("Should return a CloudBlobContainer", result);
            assertEquals("Should return the mocked container", mockContainer, result);
            verify(mockClient).getContainerReference(TEST_CONTAINER_NAME);
            verify(mockClient, never()).setDefaultRequestOptions(any());
        }
    }

    @Test(expected = DataStoreException.class)
    public void testGetBlobContainerWithInvalidConnectionString() throws Exception {
        try (MockedStatic<CloudStorageAccount> mockedAccount = mockStatic(CloudStorageAccount.class)) {
            mockedAccount.when(() -> CloudStorageAccount.parse("invalid-connection-string"))
                    .thenThrow(new URISyntaxException("invalid-connection-string", "Invalid URI"));

            UtilsV8.getBlobContainer("invalid-connection-string", TEST_CONTAINER_NAME);
        }
    }

    @Test(expected = DataStoreException.class)
    public void testGetBlobContainerWithInvalidKey() throws Exception {
        try (MockedStatic<CloudStorageAccount> mockedAccount = mockStatic(CloudStorageAccount.class)) {
            mockedAccount.when(() -> CloudStorageAccount.parse(anyString()))
                    .thenThrow(new InvalidKeyException("Invalid key"));

            UtilsV8.getBlobContainer("DefaultEndpointsProtocol=https;AccountName=test;AccountKey=invalid", TEST_CONTAINER_NAME);
        }
    }

    @Test(expected = DataStoreException.class)
    public void testGetBlobContainerWithStorageException() throws Exception {
        try (MockedStatic<CloudStorageAccount> mockedAccount = mockStatic(CloudStorageAccount.class)) {
            CloudStorageAccount mockAccount = mock(CloudStorageAccount.class);
            CloudBlobClient mockClient = mock(CloudBlobClient.class);

            mockedAccount.when(() -> CloudStorageAccount.parse(VALID_CONNECTION_STRING))
                    .thenReturn(mockAccount);
            when(mockAccount.createCloudBlobClient()).thenReturn(mockClient);
            when(mockClient.getContainerReference(TEST_CONTAINER_NAME))
                    .thenThrow(new com.microsoft.azure.storage.StorageException("Storage error", "Storage error", 500, null, null));

            UtilsV8.getBlobContainer(VALID_CONNECTION_STRING, TEST_CONTAINER_NAME);
        }
    }

    // ========== Edge Cases and Integration Tests ==========

    @Test
    public void testConnectionStringPriorityOrder() {
        // Test that connection string has highest priority
        Properties properties = new Properties();
        properties.put(AzureConstants.AZURE_CONNECTION_STRING, "connection-string-value");
        properties.put(AzureConstants.AZURE_SAS, "sas-value");
        properties.put(AzureConstants.AZURE_BLOB_ENDPOINT, "endpoint-value");
        properties.put(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, "account-value");
        properties.put(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY, "key-value");

        String result = UtilsV8.getConnectionStringFromProperties(properties);
        assertEquals("connection-string-value", result);
    }

    @Test
    public void testSASPriorityOverAccountKey() {
        // Test that SAS has priority over account key
        Properties properties = new Properties();
        properties.put(AzureConstants.AZURE_SAS, "sas-value");
        properties.put(AzureConstants.AZURE_BLOB_ENDPOINT, "endpoint-value");
        properties.put(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, "account-value");
        properties.put(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY, "key-value");

        String result = UtilsV8.getConnectionStringFromProperties(properties);
        assertEquals("BlobEndpoint=endpoint-value;SharedAccessSignature=sas-value", result);
    }

    @Test
    public void testFallbackToAccountKey() {
        // Test fallback to account key when no connection string or SAS
        Properties properties = new Properties();
        properties.put(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, "account-value");
        properties.put(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY, "key-value");
        properties.put(AzureConstants.AZURE_BLOB_ENDPOINT, "endpoint-value");

        String result = UtilsV8.getConnectionStringFromProperties(properties);
        assertEquals("DefaultEndpointsProtocol=https;AccountName=account-value;AccountKey=key-value;BlobEndpoint=endpoint-value", result);
    }

}