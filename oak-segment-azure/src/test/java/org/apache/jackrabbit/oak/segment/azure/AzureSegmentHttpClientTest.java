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
package org.apache.jackrabbit.oak.segment.azure;

import java.io.IOException;

import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureHttpClientTest;
import org.junit.Assert;
import org.junit.Test;

/**
 * Runs the proxy compatibility tests with the dependencies used by {@link AzurePersistenceManager}.
 */
public class AzureSegmentHttpClientTest extends AzureHttpClientTest {

    /**
     * Builds Oak's SAS-authenticated clients, including its logging and retry policies, without sending requests.
     */
    @Test
    public void buildsPersistenceWithSasThroughProductionFactory() throws IOException {
        AzurePersistence persistence = AzurePersistenceManager.createAzurePersistenceFrom(
                "account", "container", "root", "sig=test");

        assertClientEndpoints(persistence);
    }

    /**
     * Builds Oak's service-principal clients without acquiring a token or creating a remote container.
     */
    @Test
    public void buildsPersistenceWithServicePrincipalThroughProductionFactory() {
        AzurePersistence persistence = AzurePersistenceManager.createPersistenceFromServicePrincipalCredentials(
                "account", "container", "root", "test-client", "test-secret", "test-tenant", false, false);

        assertClientEndpoints(persistence);
    }

    private static void assertClientEndpoints(AzurePersistence persistence) {
        String expected = "https://account.blob.core.windows.net/container";
        Assert.assertEquals(expected, persistence.readBlobContainerClient.getBlobContainerUrl());
        Assert.assertEquals(expected, persistence.writeBlobContainerClient.getBlobContainerUrl());
        Assert.assertEquals(expected, persistence.noRetryBlobContainerClient.getBlobContainerUrl());
        Assert.assertNotNull(persistence.azureHttpRequestLoggingPolicy);
    }
}
