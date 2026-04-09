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

package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v8;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.SocketAddress;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Properties;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.RetryExponentialRetry;
import com.microsoft.azure.storage.RetryNoRetry;
import com.microsoft.azure.storage.RetryPolicy;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.oak.spi.blob.data.DataStoreException;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class UtilsV8 {

    public static final String DEFAULT_CONFIG_FILE = "azure.properties";

    public static final String DASH = "-";

    /**
     * private constructor so that class cannot initialized from outside.
     */
    private UtilsV8() {
    }

    /**
     * Create CloudBlobClient from properties.
     *
     * @param connectionString connectionString to configure @link {@link CloudBlobClient}
     * @return {@link CloudBlobClient}
     */
    public static CloudBlobClient getBlobClient(@NotNull final String connectionString) throws URISyntaxException, InvalidKeyException {
        return getBlobClient(connectionString, null);
    }

    public static CloudBlobClient getBlobClient(@NotNull final String connectionString,
                                                @Nullable final BlobRequestOptions requestOptions) throws URISyntaxException, InvalidKeyException {
        CloudStorageAccount account = CloudStorageAccount.parse(connectionString);
        CloudBlobClient client = account.createCloudBlobClient();
        if (null != requestOptions) {
            client.setDefaultRequestOptions(requestOptions);
        }
        return client;
    }

    public static CloudBlobContainer getBlobContainer(@NotNull final String connectionString,
                                                      @NotNull final String containerName) throws DataStoreException {
        return getBlobContainer(connectionString, containerName, null);
    }

    public static CloudBlobContainer getBlobContainer(@NotNull final String connectionString,
                                                      @NotNull final String containerName,
                                                      @Nullable final BlobRequestOptions requestOptions) throws DataStoreException {
        try {
            CloudBlobClient client = (
                    (null == requestOptions)
                            ? UtilsV8.getBlobClient(connectionString)
                            : UtilsV8.getBlobClient(connectionString, requestOptions)
            );
            return client.getContainerReference(containerName);
        } catch (InvalidKeyException | URISyntaxException | StorageException e) {
            throw new DataStoreException(e);
        }
    }

    public static void setProxyIfNeeded(final Properties properties) {
        String proxyHost = properties.getProperty(AzureConstantsV8.PROXY_HOST);
        String proxyPort = properties.getProperty(AzureConstantsV8.PROXY_PORT);

        if (!StringUtils.isEmpty(proxyHost) &&
            !StringUtils.isEmpty(proxyPort)) {
            int port = Integer.parseInt(proxyPort);
            SocketAddress proxyAddr = new InetSocketAddress(proxyHost, port);
            Proxy proxy = new Proxy(Proxy.Type.HTTP, proxyAddr);
            OperationContext.setDefaultProxy(proxy);
        }
    }

    public static RetryPolicy getRetryPolicy(final String maxRequestRetry) {
        int retries = PropertiesUtil.toInteger(maxRequestRetry, -1);
        if (retries < 0) {
            return null;
        }
        if (retries == 0) {
            return new RetryNoRetry();
        }
        return new RetryExponentialRetry(RetryPolicy.DEFAULT_CLIENT_BACKOFF, retries);
    }


    public static String getConnectionStringFromProperties(Properties properties) {

        String sasUri = properties.getProperty(AzureConstantsV8.AZURE_SAS, "");
        String blobEndpoint = properties.getProperty(AzureConstantsV8.AZURE_BLOB_ENDPOINT, "");
        String connectionString = properties.getProperty(AzureConstantsV8.AZURE_CONNECTION_STRING, "");
        String accountName = properties.getProperty(AzureConstantsV8.AZURE_STORAGE_ACCOUNT_NAME, "");
        String accountKey = properties.getProperty(AzureConstantsV8.AZURE_STORAGE_ACCOUNT_KEY, "");

        if (!connectionString.isEmpty()) {
            return connectionString;
        }

        if (!sasUri.isEmpty()) {
            return getConnectionStringForSas(sasUri, blobEndpoint, accountName);
        }

        return getConnectionString(
                accountName,
                accountKey, 
                blobEndpoint);
    }

    public static String getConnectionStringForSas(String sasUri, String blobEndpoint, String accountName) {
        if (StringUtils.isEmpty(blobEndpoint)) {
            return String.format("AccountName=%s;SharedAccessSignature=%s", accountName, sasUri);
        } else {
            return String.format("BlobEndpoint=%s;SharedAccessSignature=%s", blobEndpoint, sasUri);
        }
    }

    public static String getConnectionString(final String accountName, final String accountKey) {
        return getConnectionString(accountName, accountKey, null);
    }
    
    public static String getConnectionString(final String accountName, final String accountKey, String blobEndpoint) {
        StringBuilder connString = new StringBuilder("DefaultEndpointsProtocol=https");
        connString.append(";AccountName=").append(accountName);
        connString.append(";AccountKey=").append(accountKey);
        
        if (!StringUtils.isEmpty(blobEndpoint)) {
            connString.append(";BlobEndpoint=").append(blobEndpoint);
        }
        return connString.toString();
    }

    /**
     * Check whether the given properties contain sufficient Azure configuration
     * for V8 SDK connectivity (account key, SAS, or AAD credentials).
     */
    public static boolean isConfigured(Properties props) {
        // Account key auth
        if (props.containsKey(AzureConstantsV8.AZURE_STORAGE_ACCOUNT_KEY)
                && props.containsKey(AzureConstantsV8.AZURE_STORAGE_ACCOUNT_NAME)
                && props.containsKey(AzureConstantsV8.AZURE_BLOB_CONTAINER_NAME)) {
            return true;
        }
        // SAS auth
        if (props.containsKey(AzureConstantsV8.AZURE_SAS)
                && props.containsKey(AzureConstantsV8.AZURE_BLOB_ENDPOINT)
                && props.containsKey(AzureConstantsV8.AZURE_BLOB_CONTAINER_NAME)) {
            return true;
        }
        // AAD client credentials
        return props.containsKey(AzureConstantsV8.AZURE_STORAGE_ACCOUNT_NAME)
                && props.containsKey(AzureConstantsV8.AZURE_TENANT_ID)
                && props.containsKey(AzureConstantsV8.AZURE_CLIENT_ID)
                && props.containsKey(AzureConstantsV8.AZURE_CLIENT_SECRET)
                && props.containsKey(AzureConstantsV8.AZURE_BLOB_CONTAINER_NAME);
    }

    /**
     * Read a configuration properties file.
     *
     * @param fileName the properties file name
     * @return the properties
     * @throws java.io.IOException if the file doesn't exist
     */
    public static Properties readConfig(String fileName) throws IOException {
        if (!new File(fileName).exists()) {
            throw new IOException("Config file not found. fileName=" + fileName);
        }
        Properties prop = new Properties();
        InputStream in = null;
        try {
            in = new FileInputStream(fileName);
            prop.load(in);
        } finally {
            if (in != null) {
                in.close();
            }
        }
        return prop;
    }
}
