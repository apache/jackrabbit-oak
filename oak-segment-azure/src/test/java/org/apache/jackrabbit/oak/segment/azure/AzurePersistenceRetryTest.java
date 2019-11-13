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
package org.apache.jackrabbit.oak.segment.azure;

import com.azure.storage.blob.BlobContainerClient;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.apache.jackrabbit.oak.segment.azure.AzureStorageMonitorPolicyTest.SimpleRemoteStoreMonitor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AzurePersistenceRetryTest {
    static {
        // Because the settings are statically written and loaded, they can not be changed between tests.
        System.getProperties().setProperty("segment.azure.retry.attempts", "2");
        System.getProperties().setProperty("segment.azure.retry.backoff", "3");
    }

    @Test
    public void testRetryCount() {
        SimpleRemoteStoreMonitor monitor = runInvalidExecution();

        assertEquals(2, monitor.error);
        assertEquals(0, monitor.success);
    }

    @Test
    public void testRetryBackoff() {
        SimpleRemoteStoreMonitor monitor = runInvalidExecution();

        // 2 attempts = 1 original request + 1 retry
        System.out.println("Total delay: " + monitor.totalDurationMs);
        assertTrue(monitor.totalDurationMs > 3_000);
        assertTrue(monitor.totalDurationMs < 4_000);
    }

    @NotNull
    public AzureStorageMonitorPolicyTest.SimpleRemoteStoreMonitor runInvalidExecution() {
        // Create invalid connection
        String invalidConnectionString = "DefaultEndpointsProtocol=http;AccountName=invalid;AccountKey=invalid;BlobEndpoint=http://127.0.0.1:0/invalid;";
        BlobContainerClient container = AzurePersistence.createBlobContainerClient(invalidConnectionString, "oak-test");

        // Attach monitor
        SimpleRemoteStoreMonitor monitor = new SimpleRemoteStoreMonitor();
        AzurePersistence.findAzureStorageMonitorPolicy(container).setMonitor(monitor);

        try {
            container.getBlobClient("myblob").openInputStream();
        } catch (Exception ignored) {
        }
        return monitor;
    }

}
