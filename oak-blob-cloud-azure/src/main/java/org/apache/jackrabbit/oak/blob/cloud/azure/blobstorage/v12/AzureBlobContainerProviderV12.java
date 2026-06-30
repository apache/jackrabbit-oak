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

import com.azure.core.http.HttpClient;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.UserDelegationKey;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.policy.RequestRetryOptions;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.oak.spi.blob.data.DataStoreException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

class AzureBlobContainerProviderV12 {
    private static final Logger log = LoggerFactory.getLogger(AzureBlobContainerProviderV12.class);
    private static final String DEFAULT_ENDPOINT_SUFFIX = "core.windows.net";
    private final String azureConnectionString;
    private final String accountName;
    private final String containerName;
    private final String blobEndpoint;
    private final String sasToken;
    private final String accountKey;
    private final String tenantId;
    private final String clientId;
    private final String clientSecret;
    // Cached credential — token cache is per-instance, recreating on every SAS call would
    // force a new OAuth round-trip each time.
    private final ClientSecretCredential clientSecretCredential;
    // Cached service client for user-delegation SAS generation — avoids allocating a new Netty
    // event loop and connection pool on every SAS call.
    private final AtomicReference<BlobServiceClient> cachedBlobServiceClient = new AtomicReference<>();

    private AzureBlobContainerProviderV12(Builder builder) {
        this.azureConnectionString = builder.azureConnectionString;
        this.accountName = builder.accountName;
        this.containerName = builder.containerName;
        this.blobEndpoint = builder.blobEndpoint;
        this.sasToken = builder.sasToken;
        this.accountKey = builder.accountKey;
        this.tenantId = builder.tenantId;
        this.clientId = builder.clientId;
        this.clientSecret = builder.clientSecret;
        this.clientSecretCredential = StringUtils.isNoneBlank(builder.clientId, builder.clientSecret, builder.tenantId)
                ? new ClientSecretCredentialBuilder()
                .clientId(builder.clientId)
                .clientSecret(builder.clientSecret)
                .tenantId(builder.tenantId)
                .build()
                : null;
    }

    /**
     * Constructs the Azure Storage endpoint URL.
     * If a custom blobEndpoint is configured, it will be used.
     * Otherwise, constructs the default endpoint using the account name.
     *
     * @param accountName        the storage account name
     * @param customBlobEndpoint optional custom blob endpoint (can be null or empty)
     * @return the endpoint URL to use
     */
    @NotNull
    private static String getEndpointUrl(String accountName, String customBlobEndpoint) {
        if (StringUtils.isNotBlank(customBlobEndpoint)) {
            // Use custom endpoint (e.g., for private endpoints)
            // Ensure it starts with https:// if not already present
            if (!customBlobEndpoint.startsWith("http://") && !customBlobEndpoint.startsWith("https://")) {
                return "https://" + customBlobEndpoint;
            }
            return customBlobEndpoint;
        }
        // Default public endpoint
        return String.format("https://%s.blob.%s", accountName, DEFAULT_ENDPOINT_SUFFIX);
    }

    public String getContainerName() {
        return containerName;
    }

    public String getAzureConnectionString() {
        return azureConnectionString;
    }

    @NotNull
    public BlobContainerClient getBlobContainer() throws DataStoreException {
        return this.getBlobContainer(null, new Properties());
    }

    @NotNull
    public BlobContainerClient getBlobContainer(@Nullable RequestRetryOptions retryOptions, Properties properties) throws DataStoreException {
        // connection string will be given preference over service principals / sas / account key
        if (StringUtils.isNotBlank(azureConnectionString)) {
            log.debug("connecting to azure blob storage via azureConnectionString");
            return UtilsV12.getBlobContainerFromConnectionString(getAzureConnectionString(), containerName, retryOptions, properties);
        } else if (authenticateViaServicePrincipal()) {
            log.debug("connecting to azure blob storage via service principal credentials");
            return getBlobContainerFromServicePrincipals(accountName, retryOptions, properties);
        } else if (StringUtils.isNotBlank(sasToken)) {
            log.debug("connecting to azure blob storage via sas token");
            final String connectionStringWithSasToken = UtilsV12.getConnectionStringForSas(sasToken, blobEndpoint, accountName);
            return UtilsV12.getBlobContainer(connectionStringWithSasToken, containerName, retryOptions, properties);
        }
        log.debug("connecting to azure blob storage via access key");
        final String connectionStringWithAccountKey = UtilsV12.getConnectionString(accountName, accountKey, blobEndpoint);
        return UtilsV12.getBlobContainer(connectionStringWithAccountKey, containerName, retryOptions, properties);
    }

    @NotNull
    public String generateSharedAccessSignature(RequestRetryOptions retryOptions,
                                                String key,
                                                BlobSasPermission blobSasPermissions,
                                                int expirySeconds,
                                                Properties properties) throws DataStoreException, URISyntaxException, InvalidKeyException {
        return generateSharedAccessSignature(retryOptions, key, blobSasPermissions, expirySeconds, properties, null);
    }

    /**
     * Generates a shared access signature (SAS) for the specified blob with optional headers.
     * This is the Azure SDK 12 equivalent of the V8 method that accepted {@code SharedAccessBlobHeaders}.
     *
     * @param retryOptions       retry options for the request
     * @param key                the blob key
     * @param blobSasPermissions the permissions for the SAS
     * @param expirySeconds      the number of seconds until the SAS expires
     * @param properties         additional properties
     * @param optionalHeaders    optional headers to include in the SAS (can be null)
     * @return the SAS query string
     * @throws DataStoreException  if an error occurs
     * @throws URISyntaxException  if the URI is invalid
     * @throws InvalidKeyException if the key is invalid
     */
    @NotNull
    public String generateSharedAccessSignature(RequestRetryOptions retryOptions,
                                                String key,
                                                BlobSasPermission blobSasPermissions,
                                                int expirySeconds,
                                                Properties properties,
                                                @Nullable BlobSasHeadersV12 optionalHeaders) throws DataStoreException, URISyntaxException, InvalidKeyException {

        OffsetDateTime expiry = OffsetDateTime.now(ZoneOffset.UTC).plusSeconds(expirySeconds);
        BlobServiceSasSignatureValues serviceSasSignatureValues = new BlobServiceSasSignatureValues(expiry, blobSasPermissions);

        // Apply headers if provided
        if (optionalHeaders != null) {
            optionalHeaders.applyTo(serviceSasSignatureValues);
        }

        BlockBlobClient blob = getBlobContainer(retryOptions, properties).getBlobClient(key).getBlockBlobClient();

        if (authenticateViaServicePrincipal()) {
            return generateUserDelegationKeySignedSas(blob, serviceSasSignatureValues, expiry, properties);
        }
        return generateSas(blob, serviceSasSignatureValues);
    }

    @NotNull
    public String generateUserDelegationKeySignedSas(BlockBlobClient blobClient,
                                                     BlobServiceSasSignatureValues serviceSasSignatureValues,
                                                     OffsetDateTime expiryTime,
                                                     Properties properties) {

        BlobServiceClient blobServiceClient = getOrCreateBlobServiceClient(properties);
        OffsetDateTime startTime = OffsetDateTime.now(ZoneOffset.UTC);
        UserDelegationKey userDelegationKey = blobServiceClient.getUserDelegationKey(startTime, expiryTime);
        return blobClient.generateUserDelegationSas(serviceSasSignatureValues, userDelegationKey);
    }

    private boolean authenticateViaServicePrincipal() {
        return StringUtils.isBlank(azureConnectionString) &&
                StringUtils.isNoneBlank(accountName, tenantId, clientId, clientSecret);
    }

    private BlobServiceClient getOrCreateBlobServiceClient(Properties properties) {
        BlobServiceClient client = cachedBlobServiceClient.get();
        if (client == null) {
            synchronized (this) {
                client = cachedBlobServiceClient.get();
                if (client == null) {
                    client = new BlobServiceClientBuilder()
                            .endpoint(getEndpointUrl(accountName, blobEndpoint))
                            .credential(getClientSecretCredential())
                            .addPolicy(new AzureHttpRequestLoggingPolicyV12())
                            .httpClient(new NettyAsyncHttpClientBuilder()
                                    .proxy(UtilsV12.computeProxyOptions(properties))
                                    .build())
                            .buildClient();
                    cachedBlobServiceClient.set(client);
                }
            }
        }
        return client;
    }

    private ClientSecretCredential getClientSecretCredential() {
        return clientSecretCredential;
    }

    @NotNull
    private BlobContainerClient getBlobContainerFromServicePrincipals(String accountName, RequestRetryOptions retryOptions, Properties properties) {
        ClientSecretCredential credential = getClientSecretCredential();
        AzureHttpRequestLoggingPolicyV12 loggingPolicy = new AzureHttpRequestLoggingPolicyV12();

        String endpoint = getEndpointUrl(accountName, blobEndpoint);
        HttpClient httpClient = new NettyAsyncHttpClientBuilder()
                .proxy(UtilsV12.computeProxyOptions(properties))
                .build();
        BlobContainerClientBuilder builder = new BlobContainerClientBuilder()
                .endpoint(endpoint)
                .containerName(containerName)
                .credential(credential)
                .addPolicy(loggingPolicy)
                .httpClient(httpClient);
        if (retryOptions != null) {
            builder.retryOptions(retryOptions);
        }
        return builder.buildClient();
    }

    @NotNull
    private String generateSas(BlockBlobClient blob,
                               BlobServiceSasSignatureValues blobServiceSasSignatureValues) {
        return blob.generateSas(blobServiceSasSignatureValues, null);
    }

    public static class Builder {
        private final String containerName;
        private String azureConnectionString;
        private String accountName;
        private String blobEndpoint;
        private String sasToken;
        private String accountKey;
        private String tenantId;
        private String clientId;
        private String clientSecret;

        private Builder(String containerName) {
            this.containerName = containerName;
        }

        public static Builder builder(String containerName) {
            return new Builder(containerName);
        }

        public Builder withAzureConnectionString(String azureConnectionString) {
            this.azureConnectionString = azureConnectionString;
            return this;
        }

        public Builder withAccountName(String accountName) {
            this.accountName = accountName;
            return this;
        }

        public Builder withBlobEndpoint(String blobEndpoint) {
            this.blobEndpoint = blobEndpoint;
            return this;
        }

        public Builder withSasToken(String sasToken) {
            this.sasToken = sasToken;
            return this;
        }

        public Builder withAccountKey(String accountKey) {
            this.accountKey = accountKey;
            return this;
        }

        public Builder withTenantId(String tenantId) {
            this.tenantId = tenantId;
            return this;
        }

        public Builder withClientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder withClientSecret(String clientSecret) {
            this.clientSecret = clientSecret;
            return this;
        }

        public Builder initializeWithProperties(Properties properties) {
            withAzureConnectionString(properties.getProperty(AzureConstantsV12.AZURE_CONNECTION_STRING, ""));
            withAccountName(properties.getProperty(AzureConstantsV12.AZURE_STORAGE_ACCOUNT_NAME, ""));
            withBlobEndpoint(properties.getProperty(AzureConstantsV12.AZURE_BLOB_ENDPOINT, ""));
            withSasToken(properties.getProperty(AzureConstantsV12.AZURE_SAS, ""));
            withAccountKey(properties.getProperty(AzureConstantsV12.AZURE_STORAGE_ACCOUNT_KEY, ""));
            withTenantId(properties.getProperty(AzureConstantsV12.AZURE_TENANT_ID, ""));
            withClientId(properties.getProperty(AzureConstantsV12.AZURE_CLIENT_ID, ""));
            withClientSecret(properties.getProperty(AzureConstantsV12.AZURE_CLIENT_SECRET, ""));
            return this;
        }

        public AzureBlobContainerProviderV12 build() {
            return new AzureBlobContainerProviderV12(this);
        }
    }
}
