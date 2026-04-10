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
import java.util.Objects;
import java.util.Properties;

import com.azure.core.http.HttpClient;
import com.azure.core.http.ProxyOptions;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class Utils {
    public static final String DASH = "-";
    public static final String DEFAULT_CONFIG_FILE = "azure.properties";

    private Utils() {}

    public static BlobContainerClient getBlobContainer(@NotNull final String connectionString,
                                                       @NotNull final String containerName,
                                                       @Nullable final RequestRetryOptions retryOptions,
                                                       final Properties properties) throws DataStoreException {
        try {
            AzureHttpRequestLoggingPolicy loggingPolicy = new AzureHttpRequestLoggingPolicy();

            BlobServiceClientBuilder builder = new BlobServiceClientBuilder()
                    .connectionString(connectionString)
                    .retryOptions(retryOptions)
                    .addPolicy(loggingPolicy);

                HttpClient httpClient = new NettyAsyncHttpClientBuilder()
                        .proxy(computeProxyOptions(properties))
                        .build();

                builder.httpClient(httpClient);

            BlobServiceClient blobServiceClient = builder.buildClient();
            return blobServiceClient.getBlobContainerClient(containerName);

        } catch (Exception e) {
            throw new DataStoreException(e);
        }
    }

    public static ProxyOptions computeProxyOptions(final Properties properties) {
        String proxyHost = properties.getProperty(AzureConstants.PROXY_HOST);
        String proxyPort = properties.getProperty(AzureConstants.PROXY_PORT);

        if(!(Objects.toString(proxyHost, "").isEmpty() || Objects.toString(proxyPort, "").isEmpty())) {
            return new ProxyOptions(ProxyOptions.Type.HTTP,
                    new InetSocketAddress(proxyHost, Integer.parseInt(proxyPort)));
        }
        return null;
    }

    public static RequestRetryOptions getRetryOptions(final String maxRequestRetryCount, Integer requestTimeout, String secondaryLocation) {
        int retries = PropertiesUtil.toInteger(maxRequestRetryCount, -1);
        if(retries < 0) {
            return null;
        }

        if (retries == 0) {
            return new RequestRetryOptions(RetryPolicyType.FIXED, 1,
                    requestTimeout, null, null,
                    secondaryLocation);
        }
        return new RequestRetryOptions(RetryPolicyType.EXPONENTIAL, retries,
                requestTimeout, null, null,
                secondaryLocation);
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

    public static String getConnectionStringForSas(String sasUri, String blobEndpoint, String accountName) {
        if (StringUtils.isEmpty(blobEndpoint)) {
            return String.format("AccountName=%s;SharedAccessSignature=%s", accountName, sasUri);
        } else {
            return String.format("BlobEndpoint=%s;SharedAccessSignature=%s", blobEndpoint, sasUri);
        }
    }

    public static String getConnectionString(final String accountName, final String accountKey, String blobEndpoint) {
        StringBuilder connString = new StringBuilder("DefaultEndpointsProtocol=https");
        connString.append(";AccountName=").append(accountName);
        connString.append(";AccountKey=").append(accountKey);
        if (!Objects.toString(blobEndpoint, "").isEmpty()) {
            connString.append(";BlobEndpoint=").append(blobEndpoint);
        }
        return connString.toString();
    }

    public static BlobContainerClient getBlobContainerFromConnectionString(final String azureConnectionString, final String containerName) {
        AzureHttpRequestLoggingPolicy loggingPolicy = new AzureHttpRequestLoggingPolicy();

        return new BlobContainerClientBuilder()
                .connectionString(azureConnectionString)
                .containerName(containerName)
                .addPolicy(loggingPolicy)
                .buildClient();
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
