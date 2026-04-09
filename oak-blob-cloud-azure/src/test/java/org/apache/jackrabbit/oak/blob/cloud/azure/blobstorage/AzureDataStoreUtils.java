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
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.oak.spi.blob.data.DataStore;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v12.UtilsV12;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v8.UtilsV8;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.commons.collections.MapUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.DataStoreUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test utility for creating and managing Azure DataStore instances.
 * Used by oak-run, oak-it and other modules for integration testing with real Azure credentials.
 */
public class AzureDataStoreUtils extends DataStoreUtils {

    private static final Logger log = LoggerFactory.getLogger(AzureDataStoreUtils.class);

    private static final String DEFAULT_CONFIG_PATH = "./src/test/resources/azure.properties";
    private static final String DEFAULT_PROPERTY_FILE = "azure.properties";
    private static final String SYS_PROP_NAME = "azure.config";

    public static boolean isAzureConfigured() {
        Properties props = getAzureConfig();
        AzureSdkVersion version = AzureSdkVersion.resolve(props);
        if (version == AzureSdkVersion.V12) {
            return UtilsV12.isConfigured(props);
        }
        return UtilsV8.isConfigured(props);
    }

    public static Properties getAzureConfig() {
        String config = System.getProperty(SYS_PROP_NAME);
        if (StringUtils.isEmpty(config)) {
            File cfgFile = new File(System.getProperty("user.home"), DEFAULT_PROPERTY_FILE);
            if (cfgFile.exists()) {
                config = cfgFile.getAbsolutePath();
            }
        }
        if (StringUtils.isEmpty(config)) {
            config = DEFAULT_CONFIG_PATH;
        }

        Properties props = new Properties();
        if (new File(config).exists()) {
            InputStream is = null;
            try {
                is = new FileInputStream(config);
                props.load(is);
            } catch (Exception e) {
                log.warn("Error loading azure config", e);
            } finally {
                IOUtils.closeQuietly(is);
            }
            props.putAll(DataStoreUtils.getConfig());
            Map<String, String> filtered = MapUtils.filterEntries(MapUtils.fromProperties(props),
                    input -> !StringUtils.isEmpty(input.getValue()));
            props = new Properties();
            props.putAll(filtered);
        }

        return props;
    }

    public static DataStore getAzureDataStore(Properties props, String homeDir) throws Exception {
        AzureDataStore ds = new AzureDataStore();
        PropertiesUtil.populate(ds, MapUtils.fromProperties(props), false);
        ds.setProperties(props);
        ds.init(homeDir);
        return ds;
    }

    public static void deleteContainer(String containerName) throws Exception {
        if (StringUtils.isEmpty(containerName)) {
            log.warn("Cannot delete container with null or empty name. containerName={}", containerName);
            return;
        }
        log.info("Starting to delete container. containerName={}", containerName);
        Properties props = getAzureConfig();
        props.setProperty(AzureConstants.AZURE_BLOB_CONTAINER_NAME, containerName);

        boolean result = AzureBlobContainers.deleteIfExists(props);
        log.info("Container deleted. containerName={} existed={}", containerName, result);
    }
}
