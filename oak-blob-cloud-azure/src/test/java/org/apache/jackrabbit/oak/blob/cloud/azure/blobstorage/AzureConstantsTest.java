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

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Test class for AzureConstants to ensure constant values are correct
 * and that changes don't break existing functionality.
 */
public class AzureConstantsTest {

    @Test
    public void testMetadataDirectoryConstant() {
        // Test that the metadata directory name constant has the expected value
        assertEquals("META directory name should be 'META'", "META", AzureConstants.AZURE_BlOB_META_DIR_NAME);
        
        // Ensure the constant is not null or empty
        assertNotNull("META directory name should not be null", AzureConstants.AZURE_BlOB_META_DIR_NAME);
        assertFalse("META directory name should not be empty", AzureConstants.AZURE_BlOB_META_DIR_NAME.isEmpty());
    }

    @Test
    public void testMetadataKeyPrefixConstant() {
        // Test that the metadata key prefix is correctly constructed
        String expectedPrefix = AzureConstants.AZURE_BlOB_META_DIR_NAME + "/";
        assertEquals("META key prefix should be constructed correctly", expectedPrefix, AzureConstants.AZURE_BLOB_META_KEY_PREFIX);
        
        // Verify it ends with a slash for directory structure
        assertTrue("META key prefix should end with '/'", AzureConstants.AZURE_BLOB_META_KEY_PREFIX.endsWith("/"));
        
        // Verify it starts with the directory name
        assertTrue("META key prefix should start with directory name", 
                AzureConstants.AZURE_BLOB_META_KEY_PREFIX.startsWith(AzureConstants.AZURE_BlOB_META_DIR_NAME));
    }

    @Test
    public void testMetadataKeyPrefixValue() {
        // Test the exact expected value
        assertEquals("META key prefix should be 'META/'", "META/", AzureConstants.AZURE_BLOB_META_KEY_PREFIX);
        
        // Ensure the constant is not null or empty
        assertNotNull("META key prefix should not be null", AzureConstants.AZURE_BLOB_META_KEY_PREFIX);
        assertFalse("META key prefix should not be empty", AzureConstants.AZURE_BLOB_META_KEY_PREFIX.isEmpty());
    }

    @Test
    public void testBlobReferenceKeyConstant() {
        // Test that the blob reference key constant has the expected value
        assertEquals("Blob reference key should be 'reference.key'", "reference.key", AzureConstants.AZURE_BLOB_REF_KEY);
        
        // Ensure the constant is not null or empty
        assertNotNull("Blob reference key should not be null", AzureConstants.AZURE_BLOB_REF_KEY);
        assertFalse("Blob reference key should not be empty", AzureConstants.AZURE_BLOB_REF_KEY.isEmpty());
    }

    @Test
    public void testConstantConsistency() {
        // Test that the constants are consistent with each other
        String expectedKeyPrefix = AzureConstants.AZURE_BlOB_META_DIR_NAME + "/";
        assertEquals("Key prefix should be consistent with directory name", 
                expectedKeyPrefix, AzureConstants.AZURE_BLOB_META_KEY_PREFIX);
    }

    @Test
    public void testConstantImmutability() {
        // Test that constants are final and cannot be changed (compile-time check)
        // This test verifies that the constants exist and have expected types
        assertTrue("META directory name should be a String", AzureConstants.AZURE_BlOB_META_DIR_NAME instanceof String);
        assertTrue("META key prefix should be a String", AzureConstants.AZURE_BLOB_META_KEY_PREFIX instanceof String);
        assertTrue("Blob reference key should be a String", AzureConstants.AZURE_BLOB_REF_KEY instanceof String);
    }

    @Test
    public void testMetadataPathConstruction() {
        // Test that metadata paths can be constructed correctly using the constants
        String testFileName = "test-metadata.json";
        String expectedPath = AzureConstants.AZURE_BLOB_META_KEY_PREFIX + testFileName;
        String actualPath = AzureConstants.AZURE_BlOB_META_DIR_NAME + "/" + testFileName;
        
        assertEquals("Metadata path construction should be consistent", expectedPath, actualPath);
        assertEquals("Metadata path should start with META/", "META/" + testFileName, expectedPath);
    }

    @Test
    public void testConstantNamingConvention() {
        // Test that the constant names follow expected naming conventions

      // These tests verify that the constants exist by accessing them
        // If the constant names were changed incorrectly, these would fail at compile time
        assertNotNull("Directory constant should exist", AzureConstants.AZURE_BlOB_META_DIR_NAME);
        assertNotNull("Prefix constant should exist", AzureConstants.AZURE_BLOB_META_KEY_PREFIX);
        assertNotNull("Reference key constant should exist", AzureConstants.AZURE_BLOB_REF_KEY);
    }

    @Test
    public void testBackwardCompatibility() {
        // Test that the constant values maintain backward compatibility
        // These values should not change as they affect stored data structure
        assertEquals("META directory name must remain 'META' for backward compatibility",
                "META", AzureConstants.AZURE_BlOB_META_DIR_NAME);
        assertEquals("Reference key must remain 'reference.key' for backward compatibility",
                "reference.key", AzureConstants.AZURE_BLOB_REF_KEY);
    }

    @Test
    public void testAllStringConstants() {
        // Test all string constants have expected values
        assertEquals("accessKey", AzureConstants.AZURE_STORAGE_ACCOUNT_NAME);
        assertEquals("secretKey", AzureConstants.AZURE_STORAGE_ACCOUNT_KEY);
        assertEquals("azureConnectionString", AzureConstants.AZURE_CONNECTION_STRING);
        assertEquals("azureSas", AzureConstants.AZURE_SAS);
        assertEquals("tenantId", AzureConstants.AZURE_TENANT_ID);
        assertEquals("clientId", AzureConstants.AZURE_CLIENT_ID);
        assertEquals("clientSecret", AzureConstants.AZURE_CLIENT_SECRET);
        assertEquals("azureBlobEndpoint", AzureConstants.AZURE_BLOB_ENDPOINT);
        assertEquals("container", AzureConstants.AZURE_BLOB_CONTAINER_NAME);
        assertEquals("azureCreateContainer", AzureConstants.AZURE_CREATE_CONTAINER);
        assertEquals("socketTimeout", AzureConstants.AZURE_BLOB_REQUEST_TIMEOUT);
        assertEquals("maxErrorRetry", AzureConstants.AZURE_BLOB_MAX_REQUEST_RETRY);
        assertEquals("maxConnections", AzureConstants.AZURE_BLOB_CONCURRENT_REQUESTS_PER_OPERATION);
        assertEquals("enableSecondaryLocation", AzureConstants.AZURE_BLOB_ENABLE_SECONDARY_LOCATION_NAME);
        assertEquals("proxyHost", AzureConstants.PROXY_HOST);
        assertEquals("proxyPort", AzureConstants.PROXY_PORT);
        assertEquals("lastModified", AzureConstants.AZURE_BLOB_LAST_MODIFIED_KEY);
        assertEquals("refOnInit", AzureConstants.AZURE_REF_ON_INIT);
    }

    @Test
    public void testPresignedURIConstants() {
        // Test presigned URI related constants
        assertEquals("presignedHttpUploadURIExpirySeconds", AzureConstants.PRESIGNED_HTTP_UPLOAD_URI_EXPIRY_SECONDS);
        assertEquals("presignedHttpDownloadURIExpirySeconds", AzureConstants.PRESIGNED_HTTP_DOWNLOAD_URI_EXPIRY_SECONDS);
        assertEquals("presignedHttpDownloadURICacheMaxSize", AzureConstants.PRESIGNED_HTTP_DOWNLOAD_URI_CACHE_MAX_SIZE);
        assertEquals("presignedHttpDownloadURIVerifyExists", AzureConstants.PRESIGNED_HTTP_DOWNLOAD_URI_VERIFY_EXISTS);
        assertEquals("presignedHttpDownloadURIDomainOverride", AzureConstants.PRESIGNED_HTTP_DOWNLOAD_URI_DOMAIN_OVERRIDE);
        assertEquals("presignedHttpUploadURIDomainOverride", AzureConstants.PRESIGNED_HTTP_UPLOAD_URI_DOMAIN_OVERRIDE);
    }

    @Test
    public void testNumericConstants() {
        // Test all numeric constants have expected values
        assertFalse("Secondary location should be disabled by default", AzureConstants.AZURE_BLOB_ENABLE_SECONDARY_LOCATION_DEFAULT);
        assertEquals("Buffered stream threshold should be 8MB", 8L * 1024L * 1024L, AzureConstants.AZURE_BLOB_BUFFERED_STREAM_THRESHOLD);
        assertEquals("Min multipart upload part size should be 256KB", 256L * 1024L, AzureConstants.AZURE_BLOB_MIN_MULTIPART_UPLOAD_PART_SIZE);
        assertEquals("Max multipart upload part size should be 4000MB", 4000L * 1024L * 1024L, AzureConstants.AZURE_BLOB_MAX_MULTIPART_UPLOAD_PART_SIZE);
        assertEquals("Max single PUT upload size should be 256MB", 256L * 1024L * 1024L, AzureConstants.AZURE_BLOB_MAX_SINGLE_PUT_UPLOAD_SIZE);
        assertEquals("Max binary upload size should be ~190.7TB", 190L * 1024L * 1024L * 1024L * 1024L, AzureConstants.AZURE_BLOB_MAX_BINARY_UPLOAD_SIZE);
        assertEquals("Max allowable upload URIs should be 50000", 50000, AzureConstants.AZURE_BLOB_MAX_ALLOWABLE_UPLOAD_URIS);
        assertEquals("Max unique record tries should be 10", 10, AzureConstants.AZURE_BLOB_MAX_UNIQUE_RECORD_TRIES);
        assertEquals("Default concurrent request count should be 5", 5, AzureConstants.AZURE_BLOB_DEFAULT_CONCURRENT_REQUEST_COUNT);
        assertEquals("Max concurrent request count should be 10", 10, AzureConstants.AZURE_BLOB_MAX_CONCURRENT_REQUEST_COUNT);
        assertEquals("Max block size should be 100MB", 100L * 1024L * 1024L, AzureConstants.AZURE_BLOB_MAX_BLOCK_SIZE);
        assertEquals("Max retry requests should be 4", 4, AzureConstants.AZURE_BLOB_MAX_RETRY_REQUESTS);
        assertEquals("Default timeout should be 60 seconds", 60, AzureConstants.AZURE_BLOB_DEFAULT_TIMEOUT_SECONDS);
    }

    @Test
    public void testMetadataKeyPrefixConstruction() {
        // Test that the metadata key prefix is correctly constructed
        String expectedPrefix = AzureConstants.AZURE_BlOB_META_DIR_NAME + "/";
        assertEquals("Metadata key prefix should be META/", expectedPrefix, AzureConstants.AZURE_BLOB_META_KEY_PREFIX);
    }

    @Test
    public void testConstantsAreNotNull() {
        // Ensure no constants are null
        assertNotNull("AZURE_STORAGE_ACCOUNT_NAME should not be null", AzureConstants.AZURE_STORAGE_ACCOUNT_NAME);
        assertNotNull("AZURE_STORAGE_ACCOUNT_KEY should not be null", AzureConstants.AZURE_STORAGE_ACCOUNT_KEY);
        assertNotNull("AZURE_CONNECTION_STRING should not be null", AzureConstants.AZURE_CONNECTION_STRING);
        assertNotNull("AZURE_SAS should not be null", AzureConstants.AZURE_SAS);
        assertNotNull("AZURE_TENANT_ID should not be null", AzureConstants.AZURE_TENANT_ID);
        assertNotNull("AZURE_CLIENT_ID should not be null", AzureConstants.AZURE_CLIENT_ID);
        assertNotNull("AZURE_CLIENT_SECRET should not be null", AzureConstants.AZURE_CLIENT_SECRET);
        assertNotNull("AZURE_BLOB_ENDPOINT should not be null", AzureConstants.AZURE_BLOB_ENDPOINT);
        assertNotNull("AZURE_BLOB_CONTAINER_NAME should not be null", AzureConstants.AZURE_BLOB_CONTAINER_NAME);
        assertNotNull("AZURE_BlOB_META_DIR_NAME should not be null", AzureConstants.AZURE_BlOB_META_DIR_NAME);
        assertNotNull("AZURE_BLOB_META_KEY_PREFIX should not be null", AzureConstants.AZURE_BLOB_META_KEY_PREFIX);
        assertNotNull("AZURE_BLOB_REF_KEY should not be null", AzureConstants.AZURE_BLOB_REF_KEY);
        assertNotNull("AZURE_BLOB_LAST_MODIFIED_KEY should not be null", AzureConstants.AZURE_BLOB_LAST_MODIFIED_KEY);
    }

    @Test
    public void testConstantsAreNotEmpty() {
        // Ensure no string constants are empty
        assertFalse("AZURE_STORAGE_ACCOUNT_NAME should not be empty", AzureConstants.AZURE_STORAGE_ACCOUNT_NAME.isEmpty());
        assertFalse("AZURE_STORAGE_ACCOUNT_KEY should not be empty", AzureConstants.AZURE_STORAGE_ACCOUNT_KEY.isEmpty());
        assertFalse("AZURE_CONNECTION_STRING should not be empty", AzureConstants.AZURE_CONNECTION_STRING.isEmpty());
        assertFalse("AZURE_SAS should not be empty", AzureConstants.AZURE_SAS.isEmpty());
        assertFalse("AZURE_TENANT_ID should not be empty", AzureConstants.AZURE_TENANT_ID.isEmpty());
        assertFalse("AZURE_CLIENT_ID should not be empty", AzureConstants.AZURE_CLIENT_ID.isEmpty());
        assertFalse("AZURE_CLIENT_SECRET should not be empty", AzureConstants.AZURE_CLIENT_SECRET.isEmpty());
        assertFalse("AZURE_BLOB_ENDPOINT should not be empty", AzureConstants.AZURE_BLOB_ENDPOINT.isEmpty());
        assertFalse("AZURE_BLOB_CONTAINER_NAME should not be empty", AzureConstants.AZURE_BLOB_CONTAINER_NAME.isEmpty());
        assertFalse("AZURE_BlOB_META_DIR_NAME should not be empty", AzureConstants.AZURE_BlOB_META_DIR_NAME.isEmpty());
        assertFalse("AZURE_BLOB_META_KEY_PREFIX should not be empty", AzureConstants.AZURE_BLOB_META_KEY_PREFIX.isEmpty());
        assertFalse("AZURE_BLOB_REF_KEY should not be empty", AzureConstants.AZURE_BLOB_REF_KEY.isEmpty());
        assertFalse("AZURE_BLOB_LAST_MODIFIED_KEY should not be empty", AzureConstants.AZURE_BLOB_LAST_MODIFIED_KEY.isEmpty());
    }

    @Test
    public void testSizeConstants() {
        // Test that size constants are reasonable and in correct order
        assertTrue("Min part size should be less than max part size",
                AzureConstants.AZURE_BLOB_MIN_MULTIPART_UPLOAD_PART_SIZE < AzureConstants.AZURE_BLOB_MAX_MULTIPART_UPLOAD_PART_SIZE);
        assertTrue("Max single PUT size should be less than max binary size",
                AzureConstants.AZURE_BLOB_MAX_SINGLE_PUT_UPLOAD_SIZE < AzureConstants.AZURE_BLOB_MAX_BINARY_UPLOAD_SIZE);
        assertTrue("Buffered stream threshold should be reasonable",
                AzureConstants.AZURE_BLOB_BUFFERED_STREAM_THRESHOLD > 0 && AzureConstants.AZURE_BLOB_BUFFERED_STREAM_THRESHOLD < 100L * 1024L * 1024L);
        assertTrue("Max block size should be reasonable",
                AzureConstants.AZURE_BLOB_MAX_BLOCK_SIZE > 0 && AzureConstants.AZURE_BLOB_MAX_BLOCK_SIZE <= 1024L * 1024L * 1024L);
    }

    @Test
    public void testConcurrencyConstants() {
        // Test concurrency-related constants
        assertTrue("Default concurrent request count should be positive", AzureConstants.AZURE_BLOB_DEFAULT_CONCURRENT_REQUEST_COUNT > 0);
        assertTrue("Max concurrent request count should be greater than or equal to default",
                AzureConstants.AZURE_BLOB_MAX_CONCURRENT_REQUEST_COUNT >= AzureConstants.AZURE_BLOB_DEFAULT_CONCURRENT_REQUEST_COUNT);
        assertTrue("Max allowable upload URIs should be positive", AzureConstants.AZURE_BLOB_MAX_ALLOWABLE_UPLOAD_URIS > 0);
        assertTrue("Max unique record tries should be positive", AzureConstants.AZURE_BLOB_MAX_UNIQUE_RECORD_TRIES > 0);
        assertTrue("Max retry requests should be positive", AzureConstants.AZURE_BLOB_MAX_RETRY_REQUESTS > 0);
        assertTrue("Default timeout should be positive", AzureConstants.AZURE_BLOB_DEFAULT_TIMEOUT_SECONDS > 0);
    }
}
