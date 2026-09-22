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
package org.apache.jackrabbit.oak.fixture;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureBlobContainerProvider;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureDataStore;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3BackendHelper;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStore;
import org.apache.jackrabbit.oak.spi.blob.data.DataStore;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

/**
 * Extension to {@link DataStoreUtils} to enable S3 / AzureBlob extensions for cleaning and initialization.
 */
public class DataStoreUtils {

    private DataStoreUtils() {}

    private static final Logger log = LoggerFactory.getLogger(DataStoreUtils.class);

    private static Class S3 = S3DataStore.class;
    private static Class AZURE = AzureDataStore.class;

    public static boolean isS3DataStore(String dsName) {
        return (dsName != null) && (dsName.equals(S3.getName()));
    }

    public static boolean isAzureDataStore(String dsName) {
        return (dsName != null) &&
                (dsName.equals(AZURE.getName()));
    }

    public static DataStore configureIfCloudDataStore(String className, DataStore ds,
                                                      Map<String, ?> config, String bucket,
                                                      StatisticsProvider statisticsProvider) {
        // Add bucket info
        Properties props = new Properties();
        props.putAll(config);

        log.info("Using bucket [ {} ]", bucket);

        if (isS3DataStore(className)) {
            props.setProperty(S3Constants.S3_BUCKET, bucket);

            // Set the props object
            if (S3.getName().equals(className)) {
                ((S3DataStore) ds).setProperties(props);
                ((S3DataStore) ds).setStatisticsProvider(statisticsProvider);
            }
        } else if (isAzureDataStore(className)) {
            props.setProperty(AzureConstants.AZURE_BLOB_CONTAINER_NAME, bucket);
            ((AzureDataStore) ds).setProperties(props);
            ((AzureDataStore) ds).setStatisticsProvider(statisticsProvider);
        }


        return ds;
    }

    /**
     * Clean directory and if S3 bucket/Azure container is configured delete that.
     *
     * @param storeDir the local directory
     * @param config   the datastore config
     * @param bucket   the S3 bucket name / Azure container name
     * @throws Exception
     */
    public static void cleanup(File storeDir, Map<String, ?> config, String bucket) throws Exception {
        FileUtils.deleteQuietly(storeDir);
        if (config.containsKey(S3Constants.S3_BUCKET)) {
            if (!StringUtils.isEmpty(bucket)) {
                deleteBucket(bucket, config, new Date());
            }
        } else if (config.containsKey(AzureConstants.AZURE_BLOB_CONTAINER_NAME)
                || config.containsKey(AzureConstants.AZURE_CONNECTION_STRING)) {
            deleteAzureContainer(config, bucket);
        }
    }

    public static void deleteBucket(String bucket, Map<String, ?> map, Date date) {
        log.info("cleaning bucket [ {} ]", bucket);
        Properties props = new Properties();
        props.putAll(map);
        S3BackendHelper.deleteBucketAndAbortMultipartUploads(bucket, date, props);
    }

    public static void deleteAzureContainer(Map<String, ?> config, String containerName) throws Exception {
        if (config == null) {
            log.warn("config not provided, cannot delete blob container");
            return;
        }
        if (StringUtils.isEmpty(containerName)) {
            log.warn("container name is null or blank, cannot delete blob container");
            return;
        }

        final String azureConnectionString = (String) config.get(AzureConstants.AZURE_CONNECTION_STRING);
        final String clientId = (String) config.get(AzureConstants.AZURE_CLIENT_ID);
        final String clientSecret = (String) config.get(AzureConstants.AZURE_CLIENT_SECRET);
        final String tenantId = (String) config.get(AzureConstants.AZURE_TENANT_ID);
        final String accountName = (String) config.get(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME);
        final String accountKey = (String) config.get(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY);
        final String blobEndpoint = (String) config.get(AzureConstants.AZURE_BLOB_ENDPOINT);
        final String sasToken = (String) config.get(AzureConstants.AZURE_SAS);

        if (StringUtils.isAllBlank(azureConnectionString, clientId, clientSecret, tenantId, accountName, accountKey)) {
            log.warn("No valid config found for deleting blob container");
            return;
        }

        try (AzureBlobContainerProvider azureBlobContainerProvider = AzureBlobContainerProvider.Builder.builder(containerName)
                .withAzureConnectionString(azureConnectionString)
                .withAccountName(accountName)
                .withClientId(clientId)
                .withClientSecret(clientSecret)
                .withTenantId(tenantId)
                .withAccountKey(accountKey)
                .withSasToken(sasToken)
                .withBlobEndpoint(blobEndpoint)
                .build()) {
            log.info("deleting container [{}]", containerName);
            if (azureBlobContainerProvider.deleteContainerIfExists()) {
                log.info("container [{}] deleted", containerName);
            } else {
                log.info("container [{}] doesn't exists", containerName);
            }
        }
    }
}