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

import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v8.AzureConstantsV8;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v12.AzureConstantsV12;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class AzureConstantsTest {

    @Test
    public void testSharedStringConstants() {
        assertEquals("blob.azure.v12.enabled", AzureConstants.AZURE_V12_ENABLED_PROPERTY);
        assertEquals("accessKey", AzureConstants.AZURE_STORAGE_ACCOUNT_NAME);
        assertEquals("secretKey", AzureConstants.AZURE_STORAGE_ACCOUNT_KEY);
        assertEquals("azureConnectionString", AzureConstants.AZURE_CONNECTION_STRING);
        assertEquals("azureSas", AzureConstants.AZURE_SAS);
        assertEquals("tenantId", AzureConstants.AZURE_TENANT_ID);
        assertEquals("clientId", AzureConstants.AZURE_CLIENT_ID);
        assertEquals("clientSecret", AzureConstants.AZURE_CLIENT_SECRET);
        assertEquals("azureBlobEndpoint", AzureConstants.AZURE_BLOB_ENDPOINT);
        assertEquals("container", AzureConstants.AZURE_BLOB_CONTAINER_NAME);
    }

    // =====================================================================
    // Cross-contamination guards (CSO Prevention)
    //
    // V8 and V12 runtime constants must ALL differ. If any match,
    // one version's values have leaked into the other.
    // These tests live in the shared parent package because they
    // deliberately reference both v8 and v12 constants.
    // =====================================================================

    @Test
    public void testV8AndV12MaxPartSizeDiffer() {
        assertNotEquals("MAX_MULTIPART_UPLOAD_PART_SIZE must differ between V8 and V12",
                AzureConstantsV8.AZURE_BLOB_MAX_MULTIPART_UPLOAD_PART_SIZE,
                AzureConstantsV12.AZURE_BLOB_MAX_MULTIPART_UPLOAD_PART_SIZE);
    }

    @Test
    public void testV8AndV12MinPartSizeDiffer() {
        assertNotEquals("MIN_MULTIPART_UPLOAD_PART_SIZE must differ between V8 and V12",
                AzureConstantsV8.AZURE_BLOB_MIN_MULTIPART_UPLOAD_PART_SIZE,
                AzureConstantsV12.AZURE_BLOB_MIN_MULTIPART_UPLOAD_PART_SIZE);
    }

    @Test
    public void testV8AndV12BufferedStreamThresholdDiffer() {
        assertNotEquals("BUFFERED_STREAM_THRESHOLD must differ between V8 and V12",
                AzureConstantsV8.AZURE_BLOB_BUFFERED_STREAM_THRESHOLD,
                AzureConstantsV12.AZURE_BLOB_BUFFERED_STREAM_THRESHOLD);
    }

    @Test
    public void testV8AndV12DefaultConcurrencyDiffer() {
        assertNotEquals("DEFAULT_CONCURRENT_REQUEST_COUNT must differ between V8 and V12",
                AzureConstantsV8.AZURE_BLOB_DEFAULT_CONCURRENT_REQUEST_COUNT,
                AzureConstantsV12.AZURE_BLOB_DEFAULT_CONCURRENT_REQUEST_COUNT);
    }

    @Test
    public void testV8AndV12MaxConcurrencyDiffer() {
        assertNotEquals("MAX_CONCURRENT_REQUEST_COUNT must differ between V8 and V12",
                AzureConstantsV8.AZURE_BLOB_MAX_CONCURRENT_REQUEST_COUNT,
                AzureConstantsV12.AZURE_BLOB_MAX_CONCURRENT_REQUEST_COUNT);
    }

    @Test
    public void testV8AndV12MaxBinaryUploadSizeDiffer() {
        assertNotEquals("MAX_BINARY_UPLOAD_SIZE must differ between V8 and V12",
                AzureConstantsV8.AZURE_BLOB_MAX_BINARY_UPLOAD_SIZE,
                AzureConstantsV12.AZURE_BLOB_MAX_BINARY_UPLOAD_SIZE);
    }
}
