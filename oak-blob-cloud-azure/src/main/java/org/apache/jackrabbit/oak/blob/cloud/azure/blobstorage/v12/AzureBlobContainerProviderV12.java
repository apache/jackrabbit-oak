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
import java.time.Duration;
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
    // Shared HTTP client — one Netty event loop per provider instance, reused across all Azure SDK
    // client builds. Proxy settings are fixed at activation time so one client suffices.
    private final HttpClient httpClient;
    // Cached credential — token cache is per-instance, recreating on every SAS call would
    // force a new OAuth round-trip each time.
    private final ClientSecretCredential clientSecretCredential;
    // Cached service client for user-delegation SAS generation — avoids allocating a new Netty
    // event loop and connection pool on every SAS call.
    private final AtomicReference<BlobServiceClient> cachedBlobServiceClient = new AtomicReference<>();
    // Cached container client for non-SP SAS signing — signing is local HMAC, so one client
    // per activation is sufficient regardless of how many SAS calls are made.
    private final AtomicReference<BlobContainerClient> cachedContainerForSigning = new AtomicReference<>();
    // Cached user delegation key — Azure issues one key per round-trip; reusing it across all
    // presigned URI generations in an upload/download avoids O(N) calls to the userdelegationkey
    // endpoint (N = number of parts). Azure allows keys valid up to 7 days.
    // Package-private for test injection.
    final AtomicReference<CachedDelegationKey> cachedDelegationKey = new AtomicReference<>();

    // Request keys for the full 7-day window so they cover any SAS expiry we'd generate.
    private static final Duration DELEGATION_KEY_LIFETIME = Duration.ofDays(7);
    // Renew early enough to cover clock skew between this host and Azure.
    private static final Duration DELEGATION_KEY_RENEWAL_BUFFER = Duration.ofSeconds(60);

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
        this.httpClient = new NettyAsyncHttpClientBuilder()
                .proxy(UtilsV12.computeProxyOptions(builder.proxyHost, builder.proxyPort))
                .build();
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
            if (!customBlobEndpoint.startsWith("http://") && !customBlobEndpoint.startsWith("https://")) {
                return "https://" + customBlobEndpoint;
            }
            if (customBlobEndpoint.startsWith("http://")) {
                log.warn("Custom blob endpoint uses cleartext HTTP — credentials and data will be transmitted unencrypted: {}", customBlobEndpoint);
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
        return this.getBlobContainer(null);
    }

    @NotNull
    public BlobContainerClient getBlobContainer(@Nullable RequestRetryOptions retryOptions) throws DataStoreException {
        // connection string will be given preference over service principals / sas / account key
        if (StringUtils.isNotBlank(azureConnectionString)) {
            log.debug("connecting to azure blob storage via azureConnectionString");
            return UtilsV12.getBlobContainerFromConnectionString(getAzureConnectionString(), containerName, retryOptions, httpClient);
        } else if (authenticateViaServicePrincipal()) {
            log.debug("connecting to azure blob storage via service principal credentials");
            return getBlobContainerFromServicePrincipals(accountName, retryOptions);
        } else if (StringUtils.isNotBlank(sasToken)) {
            log.debug("connecting to azure blob storage via sas token");
            final String connectionStringWithSasToken = UtilsV12.getConnectionStringForSas(sasToken, blobEndpoint, accountName);
            return UtilsV12.getBlobContainerFromConnectionString(connectionStringWithSasToken, containerName, retryOptions, httpClient);
        }
        log.debug("connecting to azure blob storage via access key");
        final String connectionStringWithAccountKey = UtilsV12.getConnectionString(accountName, accountKey, blobEndpoint);
        return UtilsV12.getBlobContainerFromConnectionString(connectionStringWithAccountKey, containerName, retryOptions, httpClient);
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

        // SAS signing is a local HMAC operation — no HTTP call is made. Use a cached client
        // instead of getBlobContainer() which would allocate a new Netty event loop per call.
        BlockBlobClient blob = getBlockBlobClientForSigning(key);

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

        BlobServiceClient blobServiceClient = getOrCreateBlobServiceClient();
        UserDelegationKey userDelegationKey = getOrRefreshDelegationKey(blobServiceClient, expiryTime);
        return blobClient.generateUserDelegationSas(serviceSasSignatureValues, userDelegationKey);
    }

    /**
     * Returns a cached {@link UserDelegationKey} valid past {@code sasExpiry}, fetching a fresh
     * one when the cache is cold or the cached key would expire too soon. The key is always
     * requested for {@link #DELEGATION_KEY_LIFETIME} (7 days) so it covers any SAS expiry we
     * would generate without frequent round-trips to Azure.
     */
    UserDelegationKey getOrRefreshDelegationKey(BlobServiceClient blobServiceClient, OffsetDateTime sasExpiry) {
        // Fast path: cached key still covers sasExpiry with headroom.
        CachedDelegationKey cached = cachedDelegationKey.get();
        if (cached != null && cached.expiry.isAfter(sasExpiry.plus(DELEGATION_KEY_RENEWAL_BUFFER))) {
            return cached.key;
        }
        synchronized (this) {
            // Re-check inside the lock — another thread may have refreshed while we waited.
            cached = cachedDelegationKey.get();
            if (cached != null && cached.expiry.isAfter(sasExpiry.plus(DELEGATION_KEY_RENEWAL_BUFFER))) {
                return cached.key;
            }
            OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
            OffsetDateTime keyExpiry = now.plus(DELEGATION_KEY_LIFETIME);
            UserDelegationKey newKey = blobServiceClient.getUserDelegationKey(now, keyExpiry);
            cachedDelegationKey.set(new CachedDelegationKey(newKey, keyExpiry));
            log.debug("Refreshed user delegation key, valid until {}", keyExpiry);
            return newKey;
        }
    }

    /**
     * Returns a {@link BlockBlobClient} for SAS signing without creating a new Netty event loop.
     * SAS generation is a local HMAC operation — no HTTP connection is needed. For SP auth, the
     * cached {@link BlobServiceClient} pipeline is reused. For other auth types, one
     * {@link BlobContainerClient} is created and cached for the provider's lifetime.
     */
    private BlockBlobClient getBlockBlobClientForSigning(String key) throws DataStoreException {
        if (authenticateViaServicePrincipal()) {
            // BlobServiceClient.getBlobContainerClient() shares the existing pipeline — no new Netty client.
            return getOrCreateBlobServiceClient()
                    .getBlobContainerClient(containerName)
                    .getBlobClient(key)
                    .getBlockBlobClient();
        }
        // Non-SP auth: cache one container client per activation (signing never makes HTTP calls).
        BlobContainerClient container = cachedContainerForSigning.get();
        if (container == null) {
            synchronized (this) {
                container = cachedContainerForSigning.get();
                if (container == null) {
                    container = getBlobContainer(null);
                    cachedContainerForSigning.set(container);
                }
            }
        }
        return container.getBlobClient(key).getBlockBlobClient();
    }

    /**
     * Releases cached Azure clients. The underlying Netty event loops are not eagerly shut down
     * (the Azure SDK {@link com.azure.core.http.HttpClient} interface has no close contract), but
     * clearing the references allows GC to reclaim them, preventing accumulation across OSGi
     * restart cycles.
     */
    public void close() {
        cachedBlobServiceClient.set(null);
        cachedContainerForSigning.set(null);
        cachedDelegationKey.set(null);
        log.debug("AzureBlobContainerProviderV12 closed; cached Azure clients released");
    }

    private boolean authenticateViaServicePrincipal() {
        return StringUtils.isBlank(azureConnectionString) &&
                StringUtils.isNoneBlank(accountName, tenantId, clientId, clientSecret);
    }

    private BlobServiceClient getOrCreateBlobServiceClient() {
        BlobServiceClient client = cachedBlobServiceClient.get();
        if (client == null) {
            synchronized (this) {
                client = cachedBlobServiceClient.get();
                if (client == null) {
                    client = new BlobServiceClientBuilder()
                            .endpoint(getEndpointUrl(accountName, blobEndpoint))
                            .credential(clientSecretCredential)
                            .addPolicy(AzureHttpRequestLoggingPolicyV12.INSTANCE)
                            .httpClient(httpClient)
                            .buildClient();
                    cachedBlobServiceClient.set(client);
                }
            }
        }
        return client;
    }

    @NotNull
    private BlobContainerClient getBlobContainerFromServicePrincipals(String accountName, RequestRetryOptions retryOptions) {
        BlobContainerClientBuilder builder = new BlobContainerClientBuilder()
                .endpoint(getEndpointUrl(accountName, blobEndpoint))
                .containerName(containerName)
                .credential(clientSecretCredential)
                .addPolicy(AzureHttpRequestLoggingPolicyV12.INSTANCE)
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

    /** Holds a {@link UserDelegationKey} alongside the expiry we requested it with. */
    static final class CachedDelegationKey {
        final UserDelegationKey key;
        final OffsetDateTime expiry;

        CachedDelegationKey(UserDelegationKey key, OffsetDateTime expiry) {
            this.key = key;
            this.expiry = expiry;
        }
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
        private String proxyHost;
        private String proxyPort;

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

        public Builder withProxyHost(String proxyHost) {
            this.proxyHost = proxyHost;
            return this;
        }

        public Builder withProxyPort(String proxyPort) {
            this.proxyPort = proxyPort;
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
            withProxyHost(properties.getProperty(AzureConstantsV12.PROXY_HOST, ""));
            withProxyPort(properties.getProperty(AzureConstantsV12.PROXY_PORT, ""));
            return this;
        }

        public AzureBlobContainerProviderV12 build() {
            return new AzureBlobContainerProviderV12(this);
        }
    }
}
