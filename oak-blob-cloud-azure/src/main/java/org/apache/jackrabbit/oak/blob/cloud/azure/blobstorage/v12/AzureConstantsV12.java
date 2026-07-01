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

final class AzureConstantsV12 {
    /**
     * Directory name for storing metadata files in the blob storage
     */
    public static final String AZURE_BLOB_META_DIR_NAME = "META";

    /**
     * Key prefix for metadata entries, includes trailing slash for directory structure
     */
    public static final String AZURE_BLOB_META_KEY_PREFIX = AZURE_BLOB_META_DIR_NAME + "/";

    /**
     * Blob name (under META/) for the shared HMAC-SHA1 secret used to sign and verify upload tokens.
     * All cluster nodes must read this key from storage so their tokens are mutually valid.
     */
    public static final String AZURE_BLOB_REF_KEY = "reference.key";

    /**
     * Key name for storing last modified timestamp metadata
     */
    public static final String AZURE_BLOB_LAST_MODIFIED_KEY = "lastModified";

    /**
     * Threshold size (8 MiB) above which streams are buffered to disk during upload operations
     */
    public static final long AZURE_BLOB_BUFFERED_STREAM_THRESHOLD = 8L * 1024L * 1024L;

    /**
     * Minimum part size (10 MiB) for presigned URI generation (Direct Binary Access).
     * Aligns with V8 SDK behavior. Smaller values (e.g., 256 KiB) generate ~40x more URIs
     * (e.g., ~41k URIs for 10 GB), creating large JSON payloads with downstream impact.
     * Reference: CSO Release 24893 (GRANITE-66069) — V8->V12 upgrade URI explosion.
     */
    public static final long AZURE_BLOB_MIN_MULTIPART_UPLOAD_PART_SIZE = 10L * 1024L * 1024L;

    /**
     * Maximum part size (4000 MiB / 4 GiB) allowed by Azure Blob Storage for multipart uploads.
     * This is the Azure REST API limit for a single block in block-blob uploads.
     * Used as a validator for presigned URI generation, NOT as the actual block size for internal uploads.
     * Reference: Azure Blob Storage limits (50,000 blocks max, 4000 MiB max per block).
     */
    public static final long AZURE_BLOB_MAX_MULTIPART_UPLOAD_PART_SIZE = 4000L * 1024L * 1024L;

    /**
     * Block size (64 MiB) used for internal file uploads via uploadFromFileWithResponse.
     * Balances throughput (larger blocks) vs. memory usage (bounded concurrent staging).
     * Memory overhead = AZURE_BLOB_UPLOAD_BLOCK_SIZE × concurrentRequestCount
     * At 64 MiB × 5 = 320 MiB max, regardless of file size.
     * Reference: CSO Release 24893 (ASSETS-65164) — OOM from 4 GB block size.
     */
    public static final long AZURE_BLOB_UPLOAD_BLOCK_SIZE = 64L * 1024L * 1024L;

    /**
     * Maximum size (256 MiB) for single PUT operations in Azure Blob Storage
     */
    public static final long AZURE_BLOB_MAX_SINGLE_PUT_UPLOAD_SIZE = 256L * 1024L * 1024L;

    /**
     * Maximum total binary size (~190.7 TiB) that can be uploaded to Azure Blob Storage
     */
    public static final long AZURE_BLOB_MAX_BINARY_UPLOAD_SIZE = 190L * 1024L * 1024L * 1024L * 1024L;

    /**
     * Maximum number of blocks (50,000) allowed per blob in Azure Blob Storage
     */
    public static final int AZURE_BLOB_MAX_ALLOWABLE_UPLOAD_URIS = 50000;

    /**
     * Default number of concurrent requests for Azure Blob Storage operations
     */
    public static final int AZURE_BLOB_DEFAULT_CONCURRENT_REQUEST_COUNT = 5;

    /**
     * Maximum number of concurrent requests for Azure Blob Storage operations
     */
    public static final int AZURE_BLOB_MAX_CONCURRENT_REQUEST_COUNT = 10;

    /**
     * Block size (4 MiB) used for parallel streaming uploads via BlobOutputStream
     */
    public static final long AZURE_BLOB_PARALLEL_UPLOAD_BLOCK_SIZE = 4L * 1024L * 1024L;

    /**
     * Number of concurrent block upload requests for parallel streaming uploads
     */
    public static final int AZURE_BLOB_PARALLEL_UPLOAD_MAX_CONCURRENCY = 4;

    /**
     * Default request timeout (3 minutes) for Azure Blob Storage operations
     */
    public static final int AZURE_BLOB_DEFAULT_REQUEST_TIMEOUT = 3;

    // Auth / connection
    static final String AZURE_STORAGE_ACCOUNT_NAME = "accessKey";
    static final String AZURE_STORAGE_ACCOUNT_KEY = "secretKey";
    static final String AZURE_CONNECTION_STRING = "azureConnectionString";
    static final String AZURE_SAS = "azureSas";
    static final String AZURE_TENANT_ID = "tenantId";
    static final String AZURE_CLIENT_ID = "clientId";
    static final String AZURE_CLIENT_SECRET = "clientSecret";
    static final String AZURE_BLOB_ENDPOINT = "azureBlobEndpoint";
    static final String AZURE_BLOB_CONTAINER_NAME = "container";
    // Behavior
    static final String AZURE_CREATE_CONTAINER = "azureCreateContainer";
    static final String AZURE_BLOB_REQUEST_TIMEOUT = "socketTimeout";
    static final String AZURE_BLOB_MAX_REQUEST_RETRY = "maxErrorRetry";
    static final String AZURE_BLOB_CONCURRENT_REQUESTS_PER_OPERATION = "maxConnections";
    static final String AZURE_BLOB_ENABLE_SECONDARY_LOCATION_NAME = "enableSecondaryLocation";
    static final boolean AZURE_BLOB_ENABLE_SECONDARY_LOCATION_DEFAULT = false;
    // Proxy
    static final String PROXY_HOST = "proxyHost";
    static final String PROXY_PORT = "proxyPort";
    // Presigned URIs
    static final String PRESIGNED_HTTP_UPLOAD_URI_EXPIRY_SECONDS = "presignedHttpUploadURIExpirySeconds";
    static final String PRESIGNED_HTTP_DOWNLOAD_URI_EXPIRY_SECONDS = "presignedHttpDownloadURIExpirySeconds";
    static final String PRESIGNED_HTTP_DOWNLOAD_URI_CACHE_MAX_SIZE = "presignedHttpDownloadURICacheMaxSize";
    static final String PRESIGNED_HTTP_DOWNLOAD_URI_VERIFY_EXISTS = "presignedHttpDownloadURIVerifyExists";
    static final String PRESIGNED_HTTP_DOWNLOAD_URI_DOMAIN_OVERRIDE = "presignedHttpDownloadURIDomainOverride";
    static final String PRESIGNED_HTTP_UPLOAD_URI_DOMAIN_OVERRIDE = "presignedHttpUploadURIDomainOverride";
    static final String AZURE_REF_ON_INIT = "refOnInit";

    private AzureConstantsV12() {
    }
}
