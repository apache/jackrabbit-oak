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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AzureBlobContainerProviderTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private static final String CONTAINER_NAME = "test-delete-container";

    private String connectionString;

    @Before
    public void setUp() {
        connectionString = String.format(
                "DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=%s",
                AzuriteDockerRule.ACCOUNT_NAME, AzuriteDockerRule.ACCOUNT_KEY, azurite.getBlobEndpoint());
    }

    @Test
    public void deleteContainerIfExists_existingContainer_returnsTrue() throws Exception {
        azurite.getContainer(CONTAINER_NAME); // creates the container

        try (AzureBlobContainerProvider provider = AzureBlobContainerProvider.Builder.builder(CONTAINER_NAME)
                .withAzureConnectionString(connectionString)
                .build()) {
            assertTrue(provider.deleteContainerIfExists());
        }
    }

    @Test
    public void deleteContainerIfExists_nonExistingContainer_returnsFalse() throws Exception {
        // ensure it doesn't exist
        try (AzureBlobContainerProvider cleanup = AzureBlobContainerProvider.Builder.builder(CONTAINER_NAME)
                .withAzureConnectionString(connectionString)
                .build()) {
            cleanup.deleteContainerIfExists();
        }

        try (AzureBlobContainerProvider provider = AzureBlobContainerProvider.Builder.builder(CONTAINER_NAME)
                .withAzureConnectionString(connectionString)
                .build()) {
            assertFalse(provider.deleteContainerIfExists());
        }
    }
}
