/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.segment.azure;

import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobServiceAsyncClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.common.policy.RequestRetryOptions;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.oak.segment.azure.util.AzureRequestOptions;
import org.apache.jackrabbit.oak.segment.azure.util.Environment;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.AZURE_CLIENT_ID;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.AZURE_CLIENT_SECRET;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.AZURE_TENANT_ID;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.AZURE_SECRET_KEY;


public class AzurePersistenceManager {

    private static final Logger log = LoggerFactory.getLogger(AzurePersistenceManager.class);

    private AzurePersistenceManager() {
    }

    public static AzurePersistence createAzurePersistenceFrom(String accountName, String containerName, String rootPrefix, String sasToken) throws IOException {
        return createAzurePersistence(null, sasToken, accountName, containerName, rootPrefix, false, false);
    }

    public static AzurePersistence createAzurePersistenceFrom(String accountName, String containerName, String rootPrefix, Environment environment) throws IOException {
        final String clientId = environment.getVariable(AZURE_CLIENT_ID);
        final String clientSecret = environment.getVariable(AZURE_CLIENT_SECRET);
        final String tenantId = environment.getVariable(AZURE_TENANT_ID);

        if (StringUtils.isNoneBlank(clientId, clientSecret, tenantId)) {
            try {
                return createPersistenceFromServicePrincipalCredentials(accountName, containerName, rootPrefix, clientId, clientSecret, tenantId, false, true);
            } catch (IllegalArgumentException | StringIndexOutOfBoundsException e) {
                log.error("Error occurred while connecting to Azure Storage using service principals: ", e);
                throw new IllegalArgumentException(
                        "Could not connect to the Azure Storage. Please verify if AZURE_CLIENT_ID, AZURE_CLIENT_SECRET and AZURE_TENANT_ID environment variables are correctly set!");
            }
        }

        log.warn("AZURE_CLIENT_ID, AZURE_CLIENT_SECRET and AZURE_TENANT_ID environment variables empty or missing. Switching to authentication with AZURE_SECRET_KEY.");

        String key = environment.getVariable(AZURE_SECRET_KEY);
        try {
            return createPersistenceFromAccessKey(accountName, containerName, key, null, rootPrefix, false, false);
        } catch (IllegalArgumentException | StringIndexOutOfBoundsException e) {
            log.error("Error occurred while connecting to Azure Storage using secret key: ", e);
            throw new IllegalArgumentException(
                    "Could not connect to the Azure Storage. Please verify if AZURE_SECRET_KEY environment variable is correctly set!");
        }
    }

    public static AzurePersistence createAzurePersistenceFrom(Configuration configuration) throws IOException {
        if (!StringUtils.isBlank(configuration.connectionURL())) {
            return createPersistenceFromConnectionURL(configuration);
        }
        if (!StringUtils.isAnyBlank(configuration.clientId(), configuration.clientSecret(), configuration.tenantId())) {
            return createPersistenceFromServicePrincipalCredentials(configuration);
        }
        if (!StringUtils.isBlank(configuration.sharedAccessSignature())) {
            return createPersistenceFromSasUri(configuration);
        }
        return createPersistenceFromAccessKey(configuration);
    }

    public static AzurePersistence createAzurePersistenceFromFailover(Configuration configuration) throws IOException {
        if (!StringUtils.isAnyBlank(configuration.failoverClientId(), configuration.failoverClientSecret(), configuration.failoverTenantId())) {
            return createPersistenceFromServicePrincipalCredentials(configuration.failoverAccountName(), configuration.failoverContainerName(), configuration.rootPath(), configuration.failoverClientId(), configuration.failoverClientSecret(), configuration.failoverTenantId(), configuration.enableSecondaryLocation(), false);
        }

        return createPersistenceFromAccessKey(configuration.failoverAccountName(), configuration.failoverContainerName(), configuration.failoverAccessKey(), null, configuration.rootPath(), configuration.enableSecondaryLocation(), false);
    }

    private static AzurePersistence createPersistenceFromAccessKey(Configuration configuration) throws IOException {
        return createPersistenceFromAccessKey(configuration.accountName(), configuration.containerName(), configuration.accessKey(), configuration.blobEndpoint(), configuration.rootPath(), configuration.enableSecondaryLocation(), true);
    }

    private static AzurePersistence createPersistenceFromAccessKey(String accountName, String containerName, String accessKey, String blobEndpoint, String rootPrefix, boolean enableSecondaryLocation, boolean createContainer) throws IOException {
        checkIfEmpty(accessKey, "accessKey");
        StringBuilder connectionString = new StringBuilder();
        connectionString.append("DefaultEndpointsProtocol=https;");
        connectionString.append("AccountName=").append(accountName).append(';');
        connectionString.append("AccountKey=").append(accessKey).append(';');
        if (!StringUtils.isBlank(blobEndpoint)) {
            connectionString.append("BlobEndpoint=").append(blobEndpoint).append(';');
        }
        return createAzurePersistence(connectionString.toString(), null, accountName, containerName, rootPrefix, enableSecondaryLocation, createContainer);
    }

    @NotNull
    private static AzurePersistence createPersistenceFromConnectionURL(Configuration configuration) throws IOException {
        return createAzurePersistence(configuration.connectionURL(), configuration, true);
    }

    private static AzurePersistence createPersistenceFromSasUri(Configuration configuration) throws IOException {
        StringBuilder connectionString = new StringBuilder();
        connectionString.append("DefaultEndpointsProtocol=https;");
        connectionString.append("AccountName=").append(configuration.accountName()).append(';');
        connectionString.append("SharedAccessSignature=").append(configuration.sharedAccessSignature()).append(';');
        if (!StringUtils.isBlank(configuration.blobEndpoint())) {
            connectionString.append("BlobEndpoint=").append(configuration.blobEndpoint()).append(';');
        }
        return createAzurePersistence(connectionString.toString(), configuration, false);
    }


    @NotNull
    private static AzurePersistence createPersistenceFromServicePrincipalCredentials(Configuration configuration) {
        return createPersistenceFromServicePrincipalCredentials(configuration.accountName(), configuration.containerName(), configuration.rootPath(), configuration.clientId(), configuration.clientSecret(), configuration.tenantId(), configuration.enableSecondaryLocation(), true);
    }

    public static AzurePersistence createPersistenceFromServicePrincipalCredentials(String accountName, String containerName, String rootPrefix, String clientId, String clientSecret, String tenantId, boolean enableSecondaryLocation, boolean createContainer) {
        checkArguments(accountName, containerName, rootPrefix);
        AzureHttpRequestLoggingPolicy azureHttpRequestLoggingPolicy = new AzureHttpRequestLoggingPolicy();

        ClientSecretCredential clientSecretCredential = new ClientSecretCredentialBuilder()
                .clientId(clientId)
                .clientSecret(clientSecret)
                .tenantId(tenantId)
                .build();

        RequestRetryOptions retryOptions = readRequestRetryOptions(enableSecondaryLocation, accountName);
        BlobContainerClient blobContainerClient = new BlobContainerClientBuilder(accountName, containerName, azureHttpRequestLoggingPolicy).
                withRequestRetryOptions(retryOptions).withClientSecretCredential(clientSecretCredential).buildClient();

        RequestRetryOptions writeRetryOptions = AzureRequestOptions.getRetryOperationsOptimiseForWriteOperations();
        BlobContainerClient writeContainerClient = new BlobContainerClientBuilder(accountName, containerName, azureHttpRequestLoggingPolicy).
                withRequestRetryOptions(writeRetryOptions).withClientSecretCredential(clientSecretCredential).buildClient();

        BlobContainerClient noRetryBlobContainerClient = new BlobContainerClientBuilder(accountName, containerName, azureHttpRequestLoggingPolicy).
                withClientSecretCredential(clientSecretCredential).buildClient();

        return createAzurePersistence(blobContainerClient, writeContainerClient, noRetryBlobContainerClient, azureHttpRequestLoggingPolicy, rootPrefix, createContainer);
    }


    @NotNull
    private static AzurePersistence createAzurePersistence(String connectionString, Configuration configuration, boolean createContainer) throws IOException {
        return createAzurePersistence(connectionString, null, configuration.accountName(), configuration.containerName(), configuration.rootPath(), configuration.enableSecondaryLocation(), createContainer);
    }

    @NotNull
    public static AzurePersistence createAzurePersistence(String connectionString, String sasToken, String accountName, String containerName, String rootPrefix, boolean enableSecondaryLocation, boolean createContainer) throws IOException {
        if (StringUtils.isBlank(connectionString) && StringUtils.isBlank(sasToken)) {
            throw new IllegalArgumentException("Both connectionString and sasToken are not configured. Please configure one of them.");
        }
        checkArguments(accountName, containerName, rootPrefix);

        try {
            AzureHttpRequestLoggingPolicy azureHttpRequestLoggingPolicy = new AzureHttpRequestLoggingPolicy();

            RequestRetryOptions retryOptions = readRequestRetryOptions(enableSecondaryLocation, accountName);
            BlobContainerClient blobContainerClient = new BlobContainerClientBuilder(accountName, containerName, azureHttpRequestLoggingPolicy)
                    .withSasToken(sasToken).withConnectionString(connectionString).withRequestRetryOptions(retryOptions).buildClient();

            RequestRetryOptions writeRetryOptions = AzureRequestOptions.getRetryOperationsOptimiseForWriteOperations();
            BlobContainerClient writeBlobContainerClient = new BlobContainerClientBuilder(accountName, containerName, azureHttpRequestLoggingPolicy)
                    .withSasToken(sasToken).withConnectionString(connectionString).withRequestRetryOptions(writeRetryOptions).buildClient();


            BlobContainerClient noRetryBlobContainerClient = new BlobContainerClientBuilder(accountName, containerName, azureHttpRequestLoggingPolicy)
                    .withSasToken(sasToken).withConnectionString(connectionString).buildClient();

            return createAzurePersistence(blobContainerClient, writeBlobContainerClient, noRetryBlobContainerClient, azureHttpRequestLoggingPolicy, rootPrefix, createContainer);
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    private static AzurePersistence createAzurePersistence(BlobContainerClient blobContainerClient, BlobContainerClient writeContainerClient, BlobContainerClient noRetryBlobContainerClient, AzureHttpRequestLoggingPolicy azureHttpRequestLoggingPolicy, String rootPrefix, boolean createContainer) {
        if (createContainer && !blobContainerClient.exists()) {
            blobContainerClient.create();
        }

        final String rootPrefixNormalized = normalizePath(rootPrefix);

        return new AzurePersistence(blobContainerClient, writeContainerClient, noRetryBlobContainerClient, rootPrefixNormalized, azureHttpRequestLoggingPolicy, null);
    }

    private static RequestRetryOptions readRequestRetryOptions(boolean enableSecondaryLocation, String accountName) {
        RequestRetryOptions retryOptions = AzureRequestOptions.getRetryOptionsDefault();
        if (enableSecondaryLocation) {
            String endpointSecondaryRegion = String.format("https://%s-secondary.blob.core.windows.net", accountName);
            retryOptions = AzureRequestOptions.getRetryOptionsDefault(endpointSecondaryRegion);
        }
        return retryOptions;
    }

    @NotNull
    private static String normalizePath(@NotNull String rootPath) {
        if (!rootPath.isEmpty() && rootPath.charAt(0) == '/') {
            return rootPath.substring(1);
        }
        return rootPath;
    }

    private static void checkArguments(String accountName, String containerName, String rootPrefix) {
        checkIfEmpty(accountName, "Account name");
        checkIfEmpty(containerName, "Container name");
        checkIfEmpty(rootPrefix, "Root prefix");
    }

    private static void checkIfEmpty(String argument, String argumentName) {
        if (StringUtils.isEmpty(argument)) {
            throw new IllegalArgumentException(String.format("%s must not be empty argument", argumentName));
        }
    }

    private static class BlobContainerClientBuilder {
        private final String accountName;
        private final String containerName;
        private String sasToken;
        private String connectionString;
        private ClientSecretCredential clientSecretCredential;
        private RequestRetryOptions requestRetryOptions;
        private final AzureHttpRequestLoggingPolicy azureHttpRequestLoggingPolicy;

        public BlobContainerClientBuilder(String accountName, String containerName, AzureHttpRequestLoggingPolicy azureHttpRequestLoggingPolicy) {
            this.accountName = accountName;
            this.containerName = containerName;
            this.azureHttpRequestLoggingPolicy = azureHttpRequestLoggingPolicy;
        }

        public BlobContainerClientBuilder withSasToken(String sasToken) {
            this.sasToken = sasToken;
            return this;
        }

        public BlobContainerClientBuilder withConnectionString(String connectionString) {
            this.connectionString = connectionString;
            return this;
        }

        public BlobContainerClientBuilder withClientSecretCredential(ClientSecretCredential clientSecretCredential) {
            this.clientSecretCredential = clientSecretCredential;
            return this;
        }

        public BlobContainerClientBuilder withRequestRetryOptions(RequestRetryOptions requestRetryOptions) {
            this.requestRetryOptions = requestRetryOptions;
            return this;
        }

        public BlobContainerClient buildClient() {
            BlobServiceClient blobServiceClient = blobServiceClientBuilder().buildClient();
            return blobServiceClient.getBlobContainerClient(containerName);
        }

        public BlobContainerAsyncClient buildAsyncClient() {
            BlobServiceAsyncClient asyncBlobServiceClient = blobServiceClientBuilder().buildAsyncClient();
            return asyncBlobServiceClient.getBlobContainerAsyncClient(containerName);
        }

        private BlobServiceClientBuilder blobServiceClientBuilder() {
            validate(sasToken, clientSecretCredential, connectionString);

            if (sasToken == null) {
                sasToken = "";
            } else {
                sasToken = "?" + sasToken;
            }
            String endpoint = String.format("https://%s.blob.core.windows.net%s", accountName, sasToken);

            BlobServiceClientBuilder builder = new BlobServiceClientBuilder()
                    .endpoint(endpoint)
                    .addPolicy(azureHttpRequestLoggingPolicy);

            if (requestRetryOptions != null) {
                builder.retryOptions(requestRetryOptions);
            }

            if (clientSecretCredential != null) {
                builder.credential(clientSecretCredential);
            }
            if (connectionString != null) {
                builder.connectionString(connectionString);
            }

            return builder;
        }

        private void validate(String sasToken, ClientSecretCredential clientSecret, String connectionString) {
            if (StringUtils.isEmpty(sasToken) && clientSecret == null && StringUtils.isEmpty(connectionString)) {
                throw new IllegalArgumentException("Please configure one of sasToken, service principals or access key");
            }
        }
    }
}
