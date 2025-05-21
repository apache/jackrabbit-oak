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

import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.*;

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
        BlobContainerClient blobContainerClient = getBlobContainerClient(accountName, containerName, retryOptions, azureHttpRequestLoggingPolicy, clientSecretCredential);

        RequestRetryOptions writeRetryOptions = AzureRequestOptions.getRetryOperationsOptimiseForWriteOperations();
        BlobContainerClient writeContainerClient = getBlobContainerClient(accountName, containerName, writeRetryOptions, azureHttpRequestLoggingPolicy, clientSecretCredential);

        BlobContainerClient noRetryBlobContainerClient = getBlobContainerClient(accountName, containerName, null, azureHttpRequestLoggingPolicy, clientSecretCredential);

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
            BlobContainerClient blobContainerClient;
            if (sasToken != null) {
                blobContainerClient = getBlobContainerClientWithSas(accountName, containerName, retryOptions, azureHttpRequestLoggingPolicy, sasToken);
            } else {
                blobContainerClient = getBlobContainerClient(accountName, containerName, retryOptions, azureHttpRequestLoggingPolicy, connectionString);
            }

            RequestRetryOptions writeRetryOptions = AzureRequestOptions.getRetryOperationsOptimiseForWriteOperations();
            BlobContainerClient writeBlobContainerClient;
            if (sasToken != null) {
                writeBlobContainerClient = getBlobContainerClientWithSas(accountName, containerName, writeRetryOptions, azureHttpRequestLoggingPolicy, sasToken);
            } else {
                writeBlobContainerClient = getBlobContainerClient(accountName, containerName, writeRetryOptions, azureHttpRequestLoggingPolicy, connectionString);
            }

            BlobContainerClient noRetryBlobContainerClient;
            if (sasToken != null) {
                noRetryBlobContainerClient = getBlobContainerClientWithSas(accountName, containerName, null, azureHttpRequestLoggingPolicy, sasToken);
            } else {
                noRetryBlobContainerClient = getBlobContainerClient(accountName, containerName, null, azureHttpRequestLoggingPolicy, connectionString);
            }

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

        return new AzurePersistence(blobContainerClient, writeContainerClient, noRetryBlobContainerClient, rootPrefixNormalized, azureHttpRequestLoggingPolicy);
    }

    private static BlobContainerClient getBlobContainerClientWithSas(String accountName, String containerName, RequestRetryOptions requestRetryOptions, AzureHttpRequestLoggingPolicy azureHttpRequestLoggingPolicy, String sasToken) {
        BlobServiceClient blobServiceClient = blobServiceClientBuilder(accountName, requestRetryOptions, azureHttpRequestLoggingPolicy, sasToken)
                .buildClient();

        return blobServiceClient.getBlobContainerClient(containerName);
    }

    private static BlobContainerClient getBlobContainerClient(String accountName, String containerName, RequestRetryOptions requestRetryOptions, AzureHttpRequestLoggingPolicy azureHttpRequestLoggingPolicy, String connectionString) {
        BlobServiceClient blobServiceClient = blobServiceClientBuilder(accountName, requestRetryOptions, azureHttpRequestLoggingPolicy, null)
                .connectionString(connectionString)
                .buildClient();

        return blobServiceClient.getBlobContainerClient(containerName);
    }

    private static BlobContainerClient getBlobContainerClient(String accountName, String containerName, RequestRetryOptions requestRetryOptions, AzureHttpRequestLoggingPolicy azureHttpRequestLoggingPolicy, ClientSecretCredential clientSecretCredential) {
        BlobServiceClient blobServiceClient = blobServiceClientBuilder(accountName, requestRetryOptions, azureHttpRequestLoggingPolicy, null)
                .credential(clientSecretCredential)
                .buildClient();

        return blobServiceClient.getBlobContainerClient(containerName);
    }

    private static BlobServiceClientBuilder blobServiceClientBuilder(String accountName, RequestRetryOptions requestRetryOptions, AzureHttpRequestLoggingPolicy azureHttpRequestLoggingPolicy, String sasToken) {
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

        return builder;
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

    private static void checkArguments(String accountName, String containerName, String rootPrefix){
        checkIfEmpty(accountName, "Account name");
        checkIfEmpty(containerName, "Container name");
        checkIfEmpty(rootPrefix, "Root prefix");
    }

    private static void checkIfEmpty(String argument, String argumentName) {
        if (StringUtils.isEmpty(argument)) {
            throw new IllegalArgumentException(String.format("%s must not be empty argument", argumentName));
        }
    }
}
