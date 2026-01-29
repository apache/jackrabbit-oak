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

public final class AzureConstants {
    /**
     * Azure storage account name
     */
    public static final String AZURE_STORAGE_ACCOUNT_NAME = "accessKey";

    /**
     * Azure storage account key
     */
    public static final String AZURE_STORAGE_ACCOUNT_KEY = "secretKey";

    /**
     * Azure connection string (overrides {@link #AZURE_SAS} and {@link #AZURE_BLOB_ENDPOINT}).
     */
    public static final String AZURE_CONNECTION_STRING = "azureConnectionString";

    /**
     * Azure shared access signature token
     */
    public static final String AZURE_SAS = "azureSas";

    /**
     * Azure active directory
     */
    public static final String AZURE_TENANT_ID = "tenantId";

    /**
     * Azure service principal id
     */
    public static final String AZURE_CLIENT_ID = "clientId";

    /**
     * Azure service principal password
     */
    public static final String AZURE_CLIENT_SECRET = "clientSecret";

    /**
     * Azure blob endpoint
     */
    public static final String AZURE_BLOB_ENDPOINT = "azureBlobEndpoint";

    /**
     * Azure blob storage container name
     */
    public static final String AZURE_BLOB_CONTAINER_NAME = "container";

    /**
     * Azure create container if doesn't exist
     */
    public static final String AZURE_CREATE_CONTAINER = "azureCreateContainer";

    /**
     * Azure blob storage request timeout
     */
    public static final String AZURE_BLOB_REQUEST_TIMEOUT = "socketTimeout";

    /**
     * Azure blob storage maximum retries per request
     */
    public static final String AZURE_BLOB_MAX_REQUEST_RETRY = "maxErrorRetry";

    /**
     * Azure blob storage maximum connections per operation (default 1)
     */
    public static final String AZURE_BLOB_CONCURRENT_REQUESTS_PER_OPERATION = "maxConnections";

    /**
     * Azure blob storage enable fallback to secondary location for reads
     */
    public static final String AZURE_BLOB_ENABLE_SECONDARY_LOCATION_NAME = "enableSecondaryLocation";

    /**
     * Is reading from the secondary location enabled by default
     */
    public static final boolean AZURE_BLOB_ENABLE_SECONDARY_LOCATION_DEFAULT = false;

    /**
     *  Proxy host
     */
    public static final String PROXY_HOST = "proxyHost";

    /**
     *  Proxy port
     */
    public static final String PROXY_PORT = "proxyPort";

    /**
     * TTL for presigned HTTP upload URIs - default is 0 (disabled)
     */
    public static final String PRESIGNED_HTTP_UPLOAD_URI_EXPIRY_SECONDS = "presignedHttpUploadURIExpirySeconds";

    /**
     * TTL for presigned HTTP download URIs - default is 0 (disabled)
     */
    public static final String PRESIGNED_HTTP_DOWNLOAD_URI_EXPIRY_SECONDS = "presignedHttpDownloadURIExpirySeconds";

    /**
     * Maximum size of presigned HTTP download URI cache - default is 0 (no cache)
     */
    public static final String PRESIGNED_HTTP_DOWNLOAD_URI_CACHE_MAX_SIZE = "presignedHttpDownloadURICacheMaxSize";

    /**
     * Boolean flag to allow disabling of verification check on download URI
     * generation.  Default is true (the existence check is performed).
     *
     * Some installations may prefer to disable async uploads, in which case it
     * is possible to disable the existence check and thus greatly speed up the
     * generation of presigned download URIs.  See OAK-7998 which describes why
     * the existence check was added to understand how async uploading relates
     * to this feature.
     */
    public static final String PRESIGNED_HTTP_DOWNLOAD_URI_VERIFY_EXISTS = "presignedHttpDownloadURIVerifyExists";

    /**
     * Domain name to use for direct downloads instead of the default Azure blob storage domain.
     * This is usually used when an installation has configured a CDN domain for binary downloads.
     */
    public static final String PRESIGNED_HTTP_DOWNLOAD_URI_DOMAIN_OVERRIDE = "presignedHttpDownloadURIDomainOverride";

    /**
     * Domain name to use for direct uploads instead of the default Azure blob storage domain.
     * This is usually used when an installation has configured a CDN domain for binary uploads.
     */
    public static final String PRESIGNED_HTTP_UPLOAD_URI_DOMAIN_OVERRIDE = "presignedHttpUploadURIDomainOverride";

    /**
     * Property to enable/disable creation of reference secret on init.
     */
    public static final String AZURE_REF_ON_INIT = "refOnInit";

    /**
     * Directory name for storing metadata files in the blob storage
     */
    public static final String AZURE_BlOB_META_DIR_NAME = "META";

    /**
     * Key prefix for metadata entries, includes trailing slash for directory structure
     */
    public static final String AZURE_BLOB_META_KEY_PREFIX = AZURE_BlOB_META_DIR_NAME + "/";

    /**
     * Key name for storing blob reference information
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
     * Minimum part size (256 KiB) required for Azure Blob Storage multipart uploads
     */
    public static final long AZURE_BLOB_MIN_MULTIPART_UPLOAD_PART_SIZE = 256L * 1024L;

    /**
     * Maximum part size (4000 MiB / 4 GiB) allowed by Azure Blob Storage for multipart uploads
     */
    public static final long AZURE_BLOB_MAX_MULTIPART_UPLOAD_PART_SIZE = 4000L * 1024L * 1024L;

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
     * Maximum number of attempts to generate a unique record identifier
     */
    public static final int AZURE_BLOB_MAX_UNIQUE_RECORD_TRIES = 10;

    /**
     * Default number of concurrent requests (5) for optimal performance with Azure SDK 12
     */
    public static final int AZURE_BLOB_DEFAULT_CONCURRENT_REQUEST_COUNT = 5;

    /**
     * Maximum recommended concurrent requests (10) for optimal performance with Azure SDK 12
     */
    public static final int AZURE_BLOB_MAX_CONCURRENT_REQUEST_COUNT = 10;

    /**
     * Maximum block size (100 MiB) supported by Azure Blob Storage SDK 12
     */
    public static final long AZURE_BLOB_MAX_BLOCK_SIZE = 100L * 1024L * 1024L;

    /**
     * Default number of retry attempts (4) for failed requests in Azure SDK 12
     */
    public static final int AZURE_BLOB_MAX_RETRY_REQUESTS = 4;

    /**
     * Default timeout duration (60 seconds) for Azure Blob Storage operations
     */
    public static final int AZURE_BLOB_DEFAULT_TIMEOUT_SECONDS = 60;

    private AzureConstants() { }
}
