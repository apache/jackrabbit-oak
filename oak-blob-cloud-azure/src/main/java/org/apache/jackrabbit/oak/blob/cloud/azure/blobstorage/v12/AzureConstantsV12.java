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

/**
 * Constants for the Azure Blob Storage v12 SDK backend.
 * <p>
 * These values are self-contained within the v12 package. They must not reference
 * or delegate to the shared {@code AzureConstants} class, so that the v12 package
 * remains fully independent of other packages.
 */
public final class AzureConstantsV12 {

    // --- Configuration property names ---

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

    // --- v12-specific runtime constants ---
    // These values differ from V8 to leverage Azure SDK v12 capabilities:
    // - Higher concurrency default (5 vs V8's 2) with lower max (10 vs V8's 50) for better default throughput
    // - Larger buffer threshold (8 MB vs V8's 1 MB) to reduce small-buffer overhead
    // - Smaller min part size (4 MB vs V8's 10 MB) matching SDK v12's minimum block size
    // - Max upload size of 190 TiB (50,000 blocks x 4000 MiB) vs V8's ~4.75 TiB limit

    public static final String AZURE_BLOB_META_DIR_NAME = "META";
    public static final String AZURE_BLOB_META_KEY_PREFIX = AZURE_BLOB_META_DIR_NAME + "/";
    public static final String AZURE_BLOB_REF_KEY = "reference.key";
    public static final String AZURE_BLOB_LAST_MODIFIED_KEY = "lastModified";
    public static final long AZURE_BLOB_BUFFERED_STREAM_THRESHOLD = 8L * 1024L * 1024L; // 8 MiB
    public static final long AZURE_BLOB_MIN_MULTIPART_UPLOAD_PART_SIZE = 4L * 1024L * 1024L; // 4 MiB (SDK v12 minimum block size)
    public static final long AZURE_BLOB_MAX_MULTIPART_UPLOAD_PART_SIZE = 4000L * 1024L * 1024L; // 4000 MiB (SDK v12 maximum block size)
    public static final long AZURE_BLOB_MAX_SINGLE_PUT_UPLOAD_SIZE = 256L * 1024L * 1024L; // 256 MiB
    public static final long AZURE_BLOB_MAX_BINARY_UPLOAD_SIZE = 190L * 1024L * 1024L * 1024L * 1024L; // ~190 TiB (50,000 blocks x 4000 MiB)
    public static final int AZURE_BLOB_MAX_ALLOWABLE_UPLOAD_URIS = 50000;
    public static final int AZURE_BLOB_DEFAULT_CONCURRENT_REQUEST_COUNT = 5;
    public static final int AZURE_BLOB_MAX_CONCURRENT_REQUEST_COUNT = 10;
    public static final long AZURE_BLOB_MAX_BLOCK_SIZE = 100L * 1024L * 1024L; // 100 MiB
    public static final int AZURE_BLOB_MAX_UNIQUE_RECORD_TRIES = 10;

    private AzureConstantsV12() {
    }
}