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
import static org.junit.Assert.assertTrue;

public class UtilsTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testConnectionStringIsBasedOnProperty() {
        Properties properties = new Properties();
        properties.put(AzureConstants.AZURE_CONNECTION_STRING, "DefaultEndpointsProtocol=https;AccountName=accountName;AccountKey=accountKey");
        String connectionString = Utils.getConnectionStringFromProperties(properties);
        assertEquals(connectionString,"DefaultEndpointsProtocol=https;AccountName=accountName;AccountKey=accountKey");
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
}
