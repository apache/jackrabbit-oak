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

import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentialsToken;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.UserDelegationKey;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.SharedAccessBlobHeaders;
import com.microsoft.azure.storage.blob.SharedAccessBlobPermissions;
import com.microsoft.azure.storage.blob.SharedAccessBlobPolicy;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.time.Instant;
import java.util.Date;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Properties;

public class AzureDataStoreAccessManager {
    private static final String DEFAULT_ENDPOINT_SUFFIX = "core.windows.net";
    private static final String AZURE_DEFAULT_SCOPE = "https://storage.azure.com/.default";
    private final String azureConnectionString;
    private final String accountName;
    private final String containerName;
    private final String blobEndpoint;
    private final String sasToken;
    private final String accountKey;
    private final String tenantId;
    private final String clientId;
    private final String clientSecret;

    private AzureDataStoreAccessManager(Builder builder) {
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

        public AzureDataStoreAccessManager build() {
            return new AzureDataStoreAccessManager(this);
        }
    }

    public String getContainerName() {
        return containerName;
    }

    @NotNull
    public CloudBlobContainer getBlobContainer() throws DataStoreException {
        return this.getBlobContainer(null);
    }

    @NotNull
    public CloudBlobContainer getBlobContainer(@Nullable BlobRequestOptions blobRequestOptions) throws DataStoreException {
        // connection string will be given preference over service principals / sas / account key
        if (StringUtils.isNotBlank(azureConnectionString)) {
            return Utils.getBlobContainer(azureConnectionString, containerName, blobRequestOptions);
        } else if (authenticateViaServicePrincipal()) {
            return getBlobContainerFromServicePrincipals(blobRequestOptions);
        } else if (StringUtils.isNotBlank(sasToken)) {
            final String connectionStringWithSasToken = Utils.getConnectionStringForSas(sasToken, blobEndpoint, accountName);
            return Utils.getBlobContainer(connectionStringWithSasToken, containerName, blobRequestOptions);
        }
        final String connectionStringWithAccountKey = Utils.getConnectionString(accountName, accountKey, blobEndpoint);
        return Utils.getBlobContainer(connectionStringWithAccountKey, containerName, blobRequestOptions);
    }

    @NotNull
    private CloudBlobContainer getBlobContainerFromServicePrincipals(@Nullable BlobRequestOptions blobRequestOptions) throws DataStoreException {
        StorageCredentialsToken storageCredentialsToken = getStorageCredentials();
        try {
            CloudStorageAccount cloud = new CloudStorageAccount(storageCredentialsToken, true, DEFAULT_ENDPOINT_SUFFIX, accountName);
            CloudBlobClient cloudBlobClient = cloud.createCloudBlobClient();
            if (blobRequestOptions != null) {
                cloudBlobClient.setDefaultRequestOptions(blobRequestOptions);
            }
            return cloudBlobClient.getContainerReference(containerName);
        } catch (URISyntaxException | StorageException e) {
            throw new DataStoreException(e);
        }
    }

    @NotNull
    private StorageCredentialsToken getStorageCredentials() {
        ClientSecretCredential clientSecretCredential = new ClientSecretCredentialBuilder()
                .clientId(clientId)
                .clientSecret(clientSecret)
                .tenantId(tenantId)
                .build();
        String accessToken = clientSecretCredential.getTokenSync(new TokenRequestContext().addScopes(AZURE_DEFAULT_SCOPE)).getToken();
        return new StorageCredentialsToken(accountName, accessToken);
    }

    @NotNull
    public String generateSharedAccessSignature(BlobRequestOptions requestOptions,
                                                String key,
                                                EnumSet<SharedAccessBlobPermissions> permissions,
                                                int expirySeconds,
                                                SharedAccessBlobHeaders optionalHeaders) throws DataStoreException, URISyntaxException, StorageException, InvalidKeyException {
        SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy();
        Date expiry = Date.from(Instant.now().plusSeconds(expirySeconds));
        policy.setSharedAccessExpiryTime(expiry);
        policy.setPermissions(permissions);

        CloudBlockBlob blob = getBlobContainer(requestOptions).getBlockBlobReference(key);

        if (authenticateViaServicePrincipal()) {
            return generateUserDelegationKeySignedSas(blob, policy, optionalHeaders, expiry);
        }
        return generateSas(blob, policy, optionalHeaders);
    }

    @NotNull
    private String generateUserDelegationKeySignedSas(CloudBlockBlob blob,
                                                      SharedAccessBlobPolicy policy,
                                                      SharedAccessBlobHeaders optionalHeaders,
                                                      Date expiry) throws StorageException {
        fillEmptyHeaders(optionalHeaders);
        UserDelegationKey userDelegationKey = blob.getServiceClient().getUserDelegationKey(Date.from(Instant.now().minusSeconds(900)),
                expiry);
        return optionalHeaders == null ? blob.generateUserDelegationSharedAccessSignature(userDelegationKey, policy) :
                blob.generateUserDelegationSharedAccessSignature(userDelegationKey, policy, optionalHeaders, null, null);
    }

    /* set empty headers as blank string due to a bug in Azure SDK
     * Azure SDK considers null headers as 'null' string which corrupts the string to sign and generates an invalid
     * sas token
     * */
    private void fillEmptyHeaders(SharedAccessBlobHeaders sharedAccessBlobHeaders) {
        final String EMPTY_STRING = "";
        Optional.ofNullable(sharedAccessBlobHeaders)
                .ifPresent(headers -> {
                    if (StringUtils.isBlank(headers.getCacheControl())) {
                        headers.setCacheControl(EMPTY_STRING);
                    }
                    if (StringUtils.isBlank(headers.getContentDisposition())) {
                        headers.setContentDisposition(EMPTY_STRING);
                    }
                    if (StringUtils.isBlank(headers.getContentEncoding())) {
                        headers.setContentEncoding(EMPTY_STRING);
                    }
                    if (StringUtils.isBlank(headers.getContentLanguage())) {
                        headers.setContentLanguage(EMPTY_STRING);
                    }
                    if (StringUtils.isBlank(headers.getContentType())) {
                        headers.setContentType(EMPTY_STRING);
                    }
                });
    }

    @NotNull
    private String generateSas(CloudBlockBlob blob,
                               SharedAccessBlobPolicy policy,
                               SharedAccessBlobHeaders optionalHeaders) throws InvalidKeyException, StorageException {
        return optionalHeaders == null ? blob.generateSharedAccessSignature(policy, null) :
                blob.generateSharedAccessSignature(policy,
                        optionalHeaders, null, null, null, true);
    }

    private boolean authenticateViaServicePrincipal() {
        return StringUtils.isBlank(azureConnectionString) &&
                StringUtils.isNoneBlank(accountName, tenantId, clientId, clientSecret);
    }
}
