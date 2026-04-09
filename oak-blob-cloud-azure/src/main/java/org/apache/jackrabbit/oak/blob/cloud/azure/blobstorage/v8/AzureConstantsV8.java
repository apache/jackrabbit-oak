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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v8;

/**
 * Constants for the Azure Blob Storage v8 SDK backend.
 * <p>
 * These values are self-contained and match the constants shipped in Oak 1.90.0.
 * They must not reference or delegate to the shared {@code AzureConstants} class,
 * so that the v8 package remains an exact behavioural match to the pre-refactoring code.
 */
public final class AzureConstantsV8 {

    // --- Configuration property names (match 1.90.0 AzureConstants) ---

    public static final String AZURE_STORAGE_ACCOUNT_NAME = "accessKey";
    public static final String AZURE_STORAGE_ACCOUNT_KEY = "secretKey";
    public static final String AZURE_CONNECTION_STRING = "azureConnectionString";
    public static final String AZURE_SAS = "azureSas";
    public static final String AZURE_TENANT_ID = "tenantId";
    public static final String AZURE_CLIENT_ID = "clientId";
    public static final String AZURE_CLIENT_SECRET = "clientSecret";
    public static final String AZURE_BLOB_ENDPOINT = "azureBlobEndpoint";
    public static final String AZURE_BLOB_CONTAINER_NAME = "container";
    public static final String AZURE_CREATE_CONTAINER = "azureCreateContainer";
    public static final String AZURE_BLOB_REQUEST_TIMEOUT = "socketTimeout";
    public static final String AZURE_BLOB_MAX_REQUEST_RETRY = "maxErrorRetry";
    public static final String AZURE_BLOB_CONCURRENT_REQUESTS_PER_OPERATION = "maxConnections";
    public static final String AZURE_BLOB_ENABLE_SECONDARY_LOCATION_NAME = "enableSecondaryLocation";
    public static final boolean AZURE_BLOB_ENABLE_SECONDARY_LOCATION_DEFAULT = false;
    public static final String PROXY_HOST = "proxyHost";
    public static final String PROXY_PORT = "proxyPort";
    public static final String PRESIGNED_HTTP_UPLOAD_URI_EXPIRY_SECONDS = "presignedHttpUploadURIExpirySeconds";
    public static final String PRESIGNED_HTTP_DOWNLOAD_URI_EXPIRY_SECONDS = "presignedHttpDownloadURIExpirySeconds";
    public static final String PRESIGNED_HTTP_DOWNLOAD_URI_CACHE_MAX_SIZE = "presignedHttpDownloadURICacheMaxSize";
    public static final String PRESIGNED_HTTP_DOWNLOAD_URI_VERIFY_EXISTS = "presignedHttpDownloadURIVerifyExists";
    public static final String PRESIGNED_HTTP_DOWNLOAD_URI_DOMAIN_OVERRIDE = "presignedHttpDownloadURIDomainOverride";
    public static final String PRESIGNED_HTTP_UPLOAD_URI_DOMAIN_OVERRIDE = "presignedHttpUploadURIDomainOverride";
    public static final String AZURE_REF_ON_INIT = "refOnInit";

    // --- v8-specific runtime constants (match 1.90.0 AzureBlobStoreBackend) ---
    // These values reflect Azure SDK v8 limits. See AzureConstantsV12 for the v12 equivalents.

    public static final String AZURE_BLOB_META_DIR_NAME = "META";
    public static final String AZURE_BLOB_META_KEY_PREFIX = AZURE_BLOB_META_DIR_NAME + "/";
    public static final String AZURE_BLOB_REF_KEY = "reference.key";
    public static final String AZURE_BLOB_LAST_MODIFIED_KEY = "lastModified";
    public static final long AZURE_BLOB_BUFFERED_STREAM_THRESHOLD = 1024L * 1024L; // 1 MiB
    public static final long AZURE_BLOB_MIN_MULTIPART_UPLOAD_PART_SIZE = 1024L * 1024L * 10L; // 10 MiB
    public static final long AZURE_BLOB_MAX_MULTIPART_UPLOAD_PART_SIZE = 1024L * 1024L * 100L; // 100 MiB (SDK v8 maximum block size)
    public static final long AZURE_BLOB_MAX_SINGLE_PUT_UPLOAD_SIZE = 1024L * 1024L * 256L; // 256 MiB
    public static final long AZURE_BLOB_MAX_BINARY_UPLOAD_SIZE =
            (long) Math.floor(1024L * 1024L * 1024L * 1024L * 4.75); // ~4.75 TiB (50,000 blocks x 100 MiB)
    public static final int AZURE_BLOB_MAX_ALLOWABLE_UPLOAD_URIS = 50000;
    public static final int AZURE_BLOB_MAX_UNIQUE_RECORD_TRIES = 10;
    public static final int AZURE_BLOB_DEFAULT_CONCURRENT_REQUEST_COUNT = 2;
    public static final int AZURE_BLOB_MAX_CONCURRENT_REQUEST_COUNT = 50;

    private AzureConstantsV8() {
    }
}
