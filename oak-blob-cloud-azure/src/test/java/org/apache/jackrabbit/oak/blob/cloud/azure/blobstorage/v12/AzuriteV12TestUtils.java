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

import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzuriteDockerRule;

import java.util.Properties;

/**
 * Shared test utilities for v12 Azurite integration tests.
 */
class AzuriteV12TestUtils {

    private AzuriteV12TestUtils() {}

    /**
     * Builds a Properties object that points a v12 backend at an Azurite container.
     *
     * @param containerName unique container name for the test
     * @param blobEndpoint  Azurite blob endpoint URL (from {@code AZURITE.getBlobEndpoint()})
     */
    static Properties azuriteProps(String containerName, String blobEndpoint) {
        Properties p = new Properties();
        p.setProperty(AzureConstantsV12.AZURE_CONNECTION_STRING,
                "DefaultEndpointsProtocol=http" +
                        ";AccountName=" + AzuriteDockerRule.ACCOUNT_NAME +
                        ";AccountKey=" + AzuriteDockerRule.ACCOUNT_KEY +
                        ";BlobEndpoint=" + blobEndpoint);
        p.setProperty(AzureConstantsV12.AZURE_BLOB_CONTAINER_NAME, containerName);
        p.setProperty(AzureConstantsV12.AZURE_CREATE_CONTAINER, "true");
        p.setProperty(AzureConstantsV12.AZURE_BLOB_ENDPOINT, blobEndpoint);
        return p;
    }
}
