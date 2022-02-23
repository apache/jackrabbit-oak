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

import com.google.common.base.Strings;
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
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class Utils {

    public static final String DEFAULT_CONFIG_FILE = "azure.properties";

    public static final String DASH = "-";

    /**
     * private constructor so that class cannot initialized from outside.
     */
    private Utils() {
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
                            ? Utils.getBlobClient(connectionString)
                            : Utils.getBlobClient(connectionString, requestOptions)
            );
            return client.getContainerReference(containerName);
        } catch (InvalidKeyException | URISyntaxException | StorageException e) {
            throw new DataStoreException(e);
        }
    }

    public static void setProxyIfNeeded(final Properties properties) {
        String proxyHost = properties.getProperty(AzureConstants.PROXY_HOST);
        String proxyPort = properties.getProperty(AzureConstants.PROXY_PORT);

        if (!Strings.isNullOrEmpty(proxyHost) &&
            Strings.isNullOrEmpty(proxyPort)) {
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

        String sasUri = properties.getProperty(AzureConstants.AZURE_SAS, "");
        String blobEndpoint = properties.getProperty(AzureConstants.AZURE_BLOB_ENDPOINT, "");
        String connectionString = properties.getProperty(AzureConstants.AZURE_CONNECTION_STRING, "");
        String accountName = properties.getProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, "");
        String accountKey = properties.getProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY, "");

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

    private static String getConnectionStringForSas(String sasUri, String blobEndpoint, String accountName) {
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
        
        if (!Strings.isNullOrEmpty(blobEndpoint)) {
            connString.append(";BlobEndpoint=").append(blobEndpoint);
        }
        return connString.toString();
    }

    /**
     * Read a configuration properties file. If the file name ends with ";burn",
     * the file is deleted after reading.
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
