/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v12;

import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Unit tests for AzureBlobContainerProviderV12.Builder — no Azure connection required.
 */
public class AzureBlobContainerProviderV12Test {

    @Test
    public void builder_withConnectionString_setsFields() {
        String connectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=key;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1";
        AzureBlobContainerProviderV12 provider = AzureBlobContainerProviderV12.Builder
                .builder("my-container")
                .withAzureConnectionString(connectionString)
                .build();

        assertEquals("my-container", provider.getContainerName());
        assertEquals(connectionString, provider.getAzureConnectionString());
    }

    @Test
    public void initializeWithProperties_connectionString_readsFromProps() {
        Properties props = new Properties();
        String connectionString = "DefaultEndpointsProtocol=http;AccountName=test;AccountKey=key;BlobEndpoint=http://127.0.0.1:10000/test";
        props.setProperty(AzureConstantsV12.AZURE_CONNECTION_STRING, connectionString);

        AzureBlobContainerProviderV12 provider = AzureBlobContainerProviderV12.Builder
                .builder("test-container")
                .initializeWithProperties(props)
                .build();

        assertEquals("test-container", provider.getContainerName());
        assertEquals(connectionString, provider.getAzureConnectionString());
    }

    @Test
    public void initializeWithProperties_emptyProps_connectionStringIsBlank() {
        AzureBlobContainerProviderV12 provider = AzureBlobContainerProviderV12.Builder
                .builder("empty-container")
                .initializeWithProperties(new Properties())
                .build();

        assertEquals("empty-container", provider.getContainerName());
        assertNotNull(provider);
    }

    @Test
    public void initializeWithProperties_noServicePrincipalFields_buildsSuccessfully() {
        Properties props = new Properties();
        props.setProperty(AzureConstantsV12.AZURE_CONNECTION_STRING, "test-conn-string");
        props.setProperty(AzureConstantsV12.AZURE_BLOB_CONTAINER_NAME, "container1");

        AzureBlobContainerProviderV12 provider = AzureBlobContainerProviderV12.Builder
                .builder("container1")
                .initializeWithProperties(props)
                .build();

        assertNotNull(provider);
        assertEquals("test-conn-string", provider.getAzureConnectionString());
    }

    @Test
    public void builder_fluentSettersReturnBuilder() {
        AzureBlobContainerProviderV12.Builder builder = AzureBlobContainerProviderV12.Builder.builder("c");
        assertNotNull(builder.withAccountName("acc"));
        assertNotNull(builder.withBlobEndpoint("http://endpoint"));
        assertNotNull(builder.withSasToken("sastoken"));
        assertNotNull(builder.withAccountKey("accountkey"));
        assertNotNull(builder.withTenantId("tenant"));
        assertNotNull(builder.withClientId("client"));
        assertNotNull(builder.withClientSecret("secret"));
        assertNotNull(builder.withAzureConnectionString("conn"));
    }
}
