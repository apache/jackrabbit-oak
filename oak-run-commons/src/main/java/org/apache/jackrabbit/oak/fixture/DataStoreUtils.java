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

import com.google.common.base.Strings;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureDataStore;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3BackendHelper;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3Constants;
import org.apache.jackrabbit.oak.blob.cloud.s3.S3DataStore;
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
     * @param config the datastore config
     * @param bucket the S3 bucket name / Azure container name
     * @throws Exception
     */
    public static void cleanup(File storeDir, Map<String, ?> config, String bucket) throws Exception {
        FileUtils.deleteQuietly(storeDir);
        if (config.containsKey(S3Constants.S3_BUCKET)) {
            if (!Strings.isNullOrEmpty(bucket)) {
                deleteBucket(bucket, config, new Date());
            }
        } else if (config.containsKey(AzureConstants.AZURE_BLOB_CONTAINER_NAME)) {
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
        String accountName = (String)config.get(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME);
        String accountKey = (String)config.get(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY);
        if (Strings.isNullOrEmpty(containerName) ||
                Strings.isNullOrEmpty(accountName) ||
                Strings.isNullOrEmpty(accountKey)) {
            return;
        }
        log.info("deleting container [" + containerName + "]");
        CloudBlobContainer container = org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.Utils
            .getBlobContainer(org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.Utils.getConnectionString(accountName, accountKey), containerName);
        if (container.deleteIfExists()) {
            log.info("container [ " + containerName + "] deleted");
        } else {
            log.info("container [" + containerName + "] doesn't exists");
        }
    }
}
