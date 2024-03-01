package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentialsToken;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.net.URISyntaxException;

public class AzureDataStoreConfig {
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
            return createBlobContainerFromServicePrincipals(blobRequestOptions);
        } else if (StringUtils.isNotBlank(sasToken)) {
            final String connectionStringWithSasToken = Utils.getConnectionStringForSas(sasToken, blobEndpoint, accountName);
            return Utils.getBlobContainer(connectionStringWithSasToken, containerName, blobRequestOptions);
        }
        final String connectionStringWithAccountKey = Utils.getConnectionString(accountName, accountKey, blobEndpoint);
        return Utils.getBlobContainer(connectionStringWithAccountKey, containerName, blobRequestOptions);
    }

    @NotNull
    private CloudBlobContainer createBlobContainerFromServicePrincipals(@Nullable BlobRequestOptions blobRequestOptions) throws DataStoreException {
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
}
