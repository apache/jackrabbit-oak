/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.common.policy.RequestRetryOptions;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;

public class UtilsTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testConnectionStringIsBasedOnProperty() {
        Properties properties = new Properties();
        properties.put(AzureConstants.AZURE_CONNECTION_STRING, "DefaultEndpointsProtocol=https;AccountName=accountName;AccountKey=accountKey");
        String connectionString = Utils.getConnectionStringFromProperties(properties);
        assertEquals("DefaultEndpointsProtocol=https;AccountName=accountName;AccountKey=accountKey", connectionString);
    }

    @Test
    public void testConnectionStringIsBasedOnSAS() {
        Properties properties = new Properties();
        properties.put(AzureConstants.AZURE_SAS, "sas");
        properties.put(AzureConstants.AZURE_BLOB_ENDPOINT, "endpoint");
        String connectionString = Utils.getConnectionStringFromProperties(properties);
        assertEquals(connectionString,
                String.format("BlobEndpoint=%s;SharedAccessSignature=%s", "endpoint", "sas"));
    }

    @Test
    public void testConnectionStringIsBasedOnSASWithoutEndpoint() {
        Properties properties = new Properties();
        properties.put(AzureConstants.AZURE_SAS, "sas");
        properties.put(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, "account");
        String connectionString = Utils.getConnectionStringFromProperties(properties);
        assertEquals(connectionString,
                String.format("AccountName=%s;SharedAccessSignature=%s", "account", "sas"));
    }

    @Test
    public void testConnectionStringIsBasedOnAccessKeyIfSASMissing() {
        Properties properties = new Properties();
        properties.put(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, "accessKey");
        properties.put(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY, "secretKey");

        String connectionString = Utils.getConnectionStringFromProperties(properties);
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

        String connectionString = Utils.getConnectionStringFromProperties(properties);
        assertEquals(connectionString,
                String.format("BlobEndpoint=%s;SharedAccessSignature=%s", "endpoint", "sas"));
    }

    @Test
    public void testReadConfig() throws IOException {
        File tempFile = folder.newFile("test.properties");
        try(FileWriter writer = new FileWriter(tempFile)) {
            writer.write("key1=value1\n");
            writer.write("key2=value2\n");
        }

        Properties properties = Utils.readConfig(tempFile.getAbsolutePath());
        assertEquals("value1", properties.getProperty("key1"));
        assertEquals("value2", properties.getProperty("key2"));
    }

    @Test
    public void testReadConfig_exception() {
        assertThrows(IOException.class, () -> Utils.readConfig("non-existent-file"));
    }

    @Test
    public void testGetBlobContainer() throws IOException, DataStoreException {
        File tempFile = folder.newFile("azure.properties");
        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("proxyHost=127.0.0.1\n");
            writer.write("proxyPort=8888\n");
        }

        Properties properties = new Properties();
        properties.load(new FileInputStream(tempFile));

        String connectionString = Utils.getConnectionString(AzuriteDockerRule.ACCOUNT_NAME, AzuriteDockerRule.ACCOUNT_KEY, "http://127.0.0.1:10000/devstoreaccount1" );
        String containerName = "test-container";
        RequestRetryOptions retryOptions = Utils.getRetryOptions("3", 3, null);

        BlobContainerClient containerClient = Utils.getBlobContainer(connectionString, containerName, retryOptions, properties);
        assertNotNull(containerClient);
    }

    @Test
    public void testGetRetryOptions() {
        RequestRetryOptions retryOptions = Utils.getRetryOptions("3", 3, null);
        assertNotNull(retryOptions);
        assertEquals(3, retryOptions.getMaxTries());
    }

    @Test
    public void testGetRetryOptionsNoRetry() {
        RequestRetryOptions retryOptions = Utils.getRetryOptions("0",3,  null);
        assertNotNull(retryOptions);
        assertEquals(1, retryOptions.getMaxTries());
    }

    @Test
    public void testGetRetryOptionsInvalid() {
        RequestRetryOptions retryOptions = Utils.getRetryOptions("-1", 3, null);
        assertNull(retryOptions);
    }

    @Test
    public void testGetConnectionString() {
        String accountName = "testaccount";
        String accountKey = "testkey";
        String blobEndpoint = "https://testaccount.blob.core.windows.net";

        String connectionString = Utils.getConnectionString(accountName, accountKey, blobEndpoint);
        String expected = "DefaultEndpointsProtocol=https;AccountName=testaccount;AccountKey=testkey;BlobEndpoint=https://testaccount.blob.core.windows.net";
        assertEquals("Connection string should match expected format", expected, connectionString);
    }

    @Test
    public void testGetConnectionStringWithoutEndpoint() {
        String accountName = "testaccount";
        String accountKey = "testkey";

        String connectionString = Utils.getConnectionString(accountName, accountKey, null);
        String expected = "DefaultEndpointsProtocol=https;AccountName=testaccount;AccountKey=testkey";
        assertEquals("Connection string should match expected format without endpoint", expected, connectionString);
    }

    @Test
    public void testGetConnectionStringWithEmptyEndpoint() {
        String accountName = "testaccount";
        String accountKey = "testkey";

        String connectionString = Utils.getConnectionString(accountName, accountKey, "");
        String expected = "DefaultEndpointsProtocol=https;AccountName=testaccount;AccountKey=testkey";
        assertEquals("Connection string should match expected format with empty endpoint", expected, connectionString);
    }

    @Test
    public void testGetConnectionStringForSas() {
        String sasUri = "sas-token";
        String blobEndpoint = "https://testaccount.blob.core.windows.net";
        String accountName = "testaccount";

        String connectionString = Utils.getConnectionStringForSas(sasUri, blobEndpoint, accountName);
        String expected = "BlobEndpoint=https://testaccount.blob.core.windows.net;SharedAccessSignature=sas-token";
        assertEquals("SAS connection string should match expected format", expected, connectionString);
    }

    @Test
    public void testGetConnectionStringForSasWithoutEndpoint() {
        String sasUri = "sas-token";
        String accountName = "testaccount";

        String connectionString = Utils.getConnectionStringForSas(sasUri, null, accountName);
        String expected = "AccountName=testaccount;SharedAccessSignature=sas-token";
        assertEquals("SAS connection string should use account name when endpoint is null", expected, connectionString);
    }

    @Test
    public void testGetConnectionStringForSasWithEmptyEndpoint() {
        String sasUri = "sas-token";
        String accountName = "testaccount";

        String connectionString = Utils.getConnectionStringForSas(sasUri, "", accountName);
        String expected = "AccountName=testaccount;SharedAccessSignature=sas-token";
        assertEquals("SAS connection string should use account name when endpoint is empty", expected, connectionString);
    }

    @Test
    public void testComputeProxyOptionsWithBothHostAndPort() {
        Properties properties = new Properties();
        properties.setProperty(AzureConstants.PROXY_HOST, "proxy.example.com");
        properties.setProperty(AzureConstants.PROXY_PORT, "8080");

        com.azure.core.http.ProxyOptions proxyOptions = Utils.computeProxyOptions(properties);
        assertNotNull("Proxy options should not be null", proxyOptions);
        assertEquals("Proxy host should match", "proxy.example.com", proxyOptions.getAddress().getHostName());
        assertEquals("Proxy port should match", 8080, proxyOptions.getAddress().getPort());
    }

    @Test(expected = NumberFormatException.class)
    public void testComputeProxyOptionsWithInvalidPort() {
        Properties properties = new Properties();
        properties.setProperty(AzureConstants.PROXY_HOST, "proxy.example.com");
        properties.setProperty(AzureConstants.PROXY_PORT, "invalid");

      Utils.computeProxyOptions(properties);
      fail("Expected NumberFormatException when port is invalid");
    }

    @Test
    public void testComputeProxyOptionsWithHostOnly() {
        Properties properties = new Properties();
        properties.setProperty(AzureConstants.PROXY_HOST, "proxy.example.com");

        com.azure.core.http.ProxyOptions proxyOptions = Utils.computeProxyOptions(properties);
        assertNull("Proxy options should be null when port is missing", proxyOptions);
    }

    @Test
    public void testComputeProxyOptionsWithPortOnly() {
        Properties properties = new Properties();
        properties.setProperty(AzureConstants.PROXY_PORT, "8080");

        com.azure.core.http.ProxyOptions proxyOptions = Utils.computeProxyOptions(properties);
        assertNull("Proxy options should be null when host is missing", proxyOptions);
    }

    @Test
    public void testComputeProxyOptionsWithEmptyProperties() {
        Properties properties = new Properties();

        com.azure.core.http.ProxyOptions proxyOptions = Utils.computeProxyOptions(properties);
        assertNull("Proxy options should be null with empty properties", proxyOptions);
    }

    @Test
    public void testComputeProxyOptionsWithEmptyValues() {
        Properties properties = new Properties();
        properties.setProperty(AzureConstants.PROXY_HOST, "");
        properties.setProperty(AzureConstants.PROXY_PORT, "");

        com.azure.core.http.ProxyOptions proxyOptions = Utils.computeProxyOptions(properties);
        assertNull("Proxy options should be null with empty values", proxyOptions);
    }

    @Test
    public void testGetBlobContainerFromConnectionString() {
        String connectionString = getConnectionString();
        String containerName = "test-container";

        BlobContainerClient containerClient = Utils.getBlobContainerFromConnectionString(connectionString, containerName);
        assertNotNull("Container client should not be null", containerClient);
        assertEquals("Container name should match", containerName, containerClient.getBlobContainerName());
    }

    @Test
    public void testGetRetryOptionsWithSecondaryLocation() {
        String secondaryLocation = "https://testaccount-secondary.blob.core.windows.net";
        RequestRetryOptions retryOptions = Utils.getRetryOptions("3", 30, secondaryLocation);
        assertNotNull("Retry options should not be null", retryOptions);
        assertEquals("Max tries should be 3", 3, retryOptions.getMaxTries());
    }

    @Test
    public void testGetRetryOptionsWithNullValues() {
        RequestRetryOptions retryOptions = Utils.getRetryOptions(null, null, null);
        assertNull("Retry options should be null with null max retry count", retryOptions);
    }

    private String getConnectionString() {
        return String.format("DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=%s",
                AzuriteDockerRule.ACCOUNT_NAME,
                AzuriteDockerRule.ACCOUNT_KEY,
                "http://127.0.0.1:10000/devstoreaccount1");
    }
}
