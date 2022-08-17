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
    
    private AzureConstants() { }
}
