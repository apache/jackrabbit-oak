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

public class AzureDataStoreAccessManager {
    private static final String DEFAULT_ENDPOINT_SUFFIX = "core.windows.net";
    private static final String AZURE_DEFAULT_SCOPE = "https://storage.azure.com/.default";
    private String azureConnectionString;
    private String accountName;
    private String containerName;
    private String blobEndpoint;
    private String sasToken;
    private String accountKey;
    private String tenantId;
    private String clientId;
    private String clientSecret;

    public void setAzureConnectionString(String azureConnectionString) {
        this.azureConnectionString = azureConnectionString;
    }

    public void setAccountName(String accountName) {
        this.accountName = accountName;
    }

    public void setContainerName(String containerName) {
        this.containerName = containerName;
    }

    public void setBlobEndpoint(String blobEndpoint) {
        this.blobEndpoint = blobEndpoint;
    }

    public void setSasToken(String sasToken) {
        this.sasToken = sasToken;
    }

    public void setAccountKey(String accountKey) {
        this.accountKey = accountKey;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void setClientSecret(String clientSecret) {
        this.clientSecret = clientSecret;
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
        } else if (StringUtils.isNoneBlank(accountName, tenantId, clientId, clientSecret)) {
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
            return generateUserDelegationSignedSas(blob, policy, optionalHeaders, expiry);
        }
        return generateSas(blob, policy, optionalHeaders, expirySeconds);
    }

    @NotNull
    private String generateUserDelegationSignedSas(CloudBlockBlob blob,
                                                   SharedAccessBlobPolicy policy,
                                                   SharedAccessBlobHeaders optionalHeaders,
                                                   Date expiry) throws StorageException {


        UserDelegationKey userDelegationKey = blob.getServiceClient().getUserDelegationKey(Date.from(Instant.now()), expiry);
        return optionalHeaders == null ? blob.generateUserDelegationSharedAccessSignature(userDelegationKey, policy) :
                blob.generateUserDelegationSharedAccessSignature(userDelegationKey, policy, optionalHeaders, null, null);

    }

    @NotNull
    private String generateSas(CloudBlockBlob blob,
                               SharedAccessBlobPolicy policy,
                               SharedAccessBlobHeaders optionalHeaders,
                               int expirySeconds) throws InvalidKeyException, StorageException {
        return optionalHeaders == null ? blob.generateSharedAccessSignature(policy, null) :
                blob.generateSharedAccessSignature(policy,
                        optionalHeaders, null, null, null, true);
    }

    private boolean authenticateViaServicePrincipal() {
        return StringUtils.isBlank(azureConnectionString) &&
                StringUtils.isNoneBlank(accountName, tenantId, clientId, clientSecret);
    }
}
