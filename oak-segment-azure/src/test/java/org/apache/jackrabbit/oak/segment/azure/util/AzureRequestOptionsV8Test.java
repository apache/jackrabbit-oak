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

package org.apache.jackrabbit.oak.segment.azure.util;

import com.microsoft.azure.storage.blob.BlobRequestOptions;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class AzureRequestOptionsV8Test {

    private BlobRequestOptions blobRequestOptions;

    @Before
    public void setUp() {
        blobRequestOptions = new BlobRequestOptions();
    }

    @Test
    public void testApplyDefaultRequestOptions() {
        AzureRequestOptionsV8.applyDefaultRequestOptions(blobRequestOptions);
        assertEquals(Long.valueOf(TimeUnit.SECONDS.toMillis(AzureRequestOptionsV8.DEFAULT_TIMEOUT_EXECUTION)), Long.valueOf(blobRequestOptions.getMaximumExecutionTimeInMs()));
        assertEquals(Long.valueOf(TimeUnit.SECONDS.toMillis(AzureRequestOptionsV8.DEFAULT_TIMEOUT_INTERVAL)), Long.valueOf(blobRequestOptions.getTimeoutIntervalInMs()));
    }

    @Test
    public void testApplyDefaultRequestOptionsWithCustomTimeouts() {
        System.setProperty(AzureRequestOptionsV8.TIMEOUT_EXECUTION_PROP, "10");
        System.setProperty(AzureRequestOptionsV8.TIMEOUT_INTERVAL_PROP, "5");

        AzureRequestOptionsV8.applyDefaultRequestOptions(blobRequestOptions);
        assertEquals(Long.valueOf(TimeUnit.SECONDS.toMillis(10)), Long.valueOf(blobRequestOptions.getMaximumExecutionTimeInMs()));
        assertEquals(Long.valueOf(TimeUnit.SECONDS.toMillis(5)), Long.valueOf(blobRequestOptions.getTimeoutIntervalInMs()));

        System.clearProperty(AzureRequestOptionsV8.TIMEOUT_EXECUTION_PROP);
        System.clearProperty(AzureRequestOptionsV8.TIMEOUT_INTERVAL_PROP);
    }

    @Test
    public void testOptimiseForWriteOperations() {
        BlobRequestOptions writeBlobRequestoptions = AzureRequestOptionsV8.optimiseForWriteOperations(blobRequestOptions);
        assertEquals(Long.valueOf(TimeUnit.SECONDS.toMillis(AzureRequestOptionsV8.DEFAULT_TIMEOUT_EXECUTION)), Long.valueOf(writeBlobRequestoptions.getMaximumExecutionTimeInMs()));
        assertEquals(Long.valueOf(TimeUnit.SECONDS.toMillis(AzureRequestOptionsV8.DEFAULT_TIMEOUT_INTERVAL)), Long.valueOf(writeBlobRequestoptions.getTimeoutIntervalInMs()));
    }

    @Test
    public void testOptimiseForWriteOperationsWithCustomTimeouts() {
        System.setProperty(AzureRequestOptionsV8.WRITE_TIMEOUT_EXECUTION_PROP, "10");
        System.setProperty(AzureRequestOptionsV8.WRITE_TIMEOUT_INTERVAL_PROP, "5");

        BlobRequestOptions writeBlobRequestoptions = AzureRequestOptionsV8.optimiseForWriteOperations(blobRequestOptions);
        assertEquals(Long.valueOf(TimeUnit.SECONDS.toMillis(10)), Long.valueOf(writeBlobRequestoptions.getMaximumExecutionTimeInMs()));
        assertEquals(Long.valueOf(TimeUnit.SECONDS.toMillis(5)), Long.valueOf(writeBlobRequestoptions.getTimeoutIntervalInMs()));

        System.clearProperty(AzureRequestOptionsV8.WRITE_TIMEOUT_EXECUTION_PROP);
        System.clearProperty(AzureRequestOptionsV8.WRITE_TIMEOUT_INTERVAL_PROP);
    }
}
