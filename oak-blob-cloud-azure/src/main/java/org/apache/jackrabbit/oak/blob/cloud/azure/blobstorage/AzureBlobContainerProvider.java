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
import org.apache.jackrabbit.core.data.DataStoreException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Properties;

public class AzureBlobContainerProvider {
    private static final Logger log = LoggerFactory.getLogger(AzureBlobContainerProvider.class);
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

    private AzureBlobContainerProvider(Builder builder) {
        this.azureConnectionString = builder.azureConnectionString;
        this.accountName = builder.accountName;
        this.containerName = builder.containerName;
        this.blobEndpoint = builder.blobEndpoint;
        this.sasToken = builder.sasToken;
        this.accountKey = builder.accountKey;
        this.tenantId = builder.tenantId;
        this.clientId = builder.clientId;
        this.clientSecret = builder.clientSecret;
    }

    public static class Builder {
        private final String containerName;

        private Builder(String containerName) {
            this.containerName = containerName;
        }

        public static Builder builder(String containerName) {
            return new Builder(containerName);
        }

        private String azureConnectionString;
        private String accountName;
        private String blobEndpoint;
        private String sasToken;
        private String accountKey;
        private String tenantId;
        private String clientId;
        private String clientSecret;

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
            withAzureConnectionString(properties.getProperty(AzureConstants.AZURE_CONNECTION_STRING, ""));
            withAccountName(properties.getProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, ""));
            withBlobEndpoint(properties.getProperty(AzureConstants.AZURE_BLOB_ENDPOINT, ""));
            withSasToken(properties.getProperty(AzureConstants.AZURE_SAS, ""));
            withAccountKey(properties.getProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY, ""));
            withTenantId(properties.getProperty(AzureConstants.AZURE_TENANT_ID, ""));
            withClientId(properties.getProperty(AzureConstants.AZURE_CLIENT_ID, ""));
            withClientSecret(properties.getProperty(AzureConstants.AZURE_CLIENT_SECRET, ""));
            return this;
        }

        public AzureBlobContainerProvider build() {
            return new AzureBlobContainerProvider(this);
        }
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
            return Utils.getBlobContainerFromConnectionString(getAzureConnectionString(), containerName);
        } else if (authenticateViaServicePrincipal()) {
            log.debug("connecting to azure blob storage via service principal credentials");
            return getBlobContainerFromServicePrincipals(accountName, retryOptions);
        } else if (StringUtils.isNotBlank(sasToken)) {
            log.debug("connecting to azure blob storage via sas token");
            final String connectionStringWithSasToken = Utils.getConnectionStringForSas(sasToken, blobEndpoint, accountName);
            return Utils.getBlobContainer(connectionStringWithSasToken, containerName, retryOptions, properties);
        }
        log.debug("connecting to azure blob storage via access key");
        final String connectionStringWithAccountKey = Utils.getConnectionString(accountName, accountKey, blobEndpoint);
        return Utils.getBlobContainer(connectionStringWithAccountKey, containerName, retryOptions, properties);
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
     * @param retryOptions retry options for the request
     * @param key the blob key
     * @param blobSasPermissions the permissions for the SAS
     * @param expirySeconds the number of seconds until the SAS expires
     * @param properties additional properties
     * @param optionalHeaders optional headers to include in the SAS (can be null)
     * @return the SAS query string
     * @throws DataStoreException if an error occurs
     * @throws URISyntaxException if the URI is invalid
     * @throws InvalidKeyException if the key is invalid
     */
    @NotNull
    public String generateSharedAccessSignature(RequestRetryOptions retryOptions,
                                                String key,
                                                BlobSasPermission blobSasPermissions,
                                                int expirySeconds,
                                                Properties properties,
                                                @Nullable BlobSasHeaders optionalHeaders) throws DataStoreException, URISyntaxException, InvalidKeyException {

        OffsetDateTime expiry = OffsetDateTime.now().plusSeconds(expirySeconds);
        BlobServiceSasSignatureValues serviceSasSignatureValues = new BlobServiceSasSignatureValues(expiry, blobSasPermissions);

        // Apply headers if provided
        if (optionalHeaders != null) {
            optionalHeaders.applyTo(serviceSasSignatureValues);
        }

        BlockBlobClient blob = getBlobContainer(retryOptions, properties).getBlobClient(key).getBlockBlobClient();

        if (authenticateViaServicePrincipal()) {
            return generateUserDelegationKeySignedSas(blob, serviceSasSignatureValues, expiry);
        }
        return generateSas(blob, serviceSasSignatureValues);
    }

    @NotNull
    public String generateUserDelegationKeySignedSas(BlockBlobClient blobClient,
                                                     BlobServiceSasSignatureValues serviceSasSignatureValues,
                                                     OffsetDateTime expiryTime) {

        AzureHttpRequestLoggingPolicy loggingPolicy = new AzureHttpRequestLoggingPolicy();

        String endpoint = getEndpointUrl(accountName, blobEndpoint);
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .endpoint(endpoint)
                .credential(getClientSecretCredential())
                .addPolicy(loggingPolicy)
                .buildClient();
        OffsetDateTime startTime = OffsetDateTime.now(ZoneOffset.UTC);
        UserDelegationKey userDelegationKey = blobServiceClient.getUserDelegationKey(startTime, expiryTime);
        return blobClient.generateUserDelegationSas(serviceSasSignatureValues, userDelegationKey);
    }

    private boolean authenticateViaServicePrincipal() {
        return StringUtils.isBlank(azureConnectionString) &&
                StringUtils.isNoneBlank(accountName, tenantId, clientId, clientSecret);
    }

    private ClientSecretCredential getClientSecretCredential() {
        return new ClientSecretCredentialBuilder()
                .clientId(clientId)
                .clientSecret(clientSecret)
                .tenantId(tenantId)
                .build();
    }

    @NotNull
    private BlobContainerClient getBlobContainerFromServicePrincipals(String accountName, RequestRetryOptions retryOptions) {
        ClientSecretCredential clientSecretCredential = getClientSecretCredential();
        AzureHttpRequestLoggingPolicy loggingPolicy = new AzureHttpRequestLoggingPolicy();

        String endpoint = getEndpointUrl(accountName, blobEndpoint);
        return new BlobContainerClientBuilder()
                .endpoint(endpoint)
                .containerName(containerName)
                .credential(clientSecretCredential)
                .retryOptions(retryOptions)
                .addPolicy(loggingPolicy)
                .buildClient();
    }

    @NotNull
    private String generateSas(BlockBlobClient blob,
                               BlobServiceSasSignatureValues blobServiceSasSignatureValues) {
        return blob.generateSas(blobServiceSasSignatureValues, null);
    }

    /**
     * Constructs the Azure Storage endpoint URL.
     * If a custom blobEndpoint is configured, it will be used.
     * Otherwise, constructs the default endpoint using the account name.
     *
     * @param accountName the storage account name
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
}