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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;

import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v8.AzureBlobStoreBackendV8;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v12.AzureBlobStoreBackendV12;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;

public class AzureDataStoreVersionSelectionIT {

    private static final String AZURE_SDK_12_ENABLED = "blob.azure.v12.enabled";

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private final String originalProperty = System.getProperty(AZURE_SDK_12_ENABLED);

    private AzureDataStore dataStore;
    private AzureBlobContainer azuriteContainer;

    @After
    public void tearDown() throws Exception {
        restoreProperty();
        if (dataStore != null) {
            dataStore.close();
            dataStore = null;
        }
        if (azuriteContainer != null) {
            azuriteContainer.deleteIfExists();
            azuriteContainer = null;
        }
    }

    @Test
    public void logsSdk12WhenPropertyEnabled() throws Exception {
        assertStartupLog("true", null, "Starting blob store using Azure SDK 12", AzureBlobStoreBackendV12.class);
    }

    @Test
    public void logsSdk8WhenPropertyDisabled() throws Exception {
        assertStartupLog("false", null, "Starting blob store using Azure SDK 8", AzureBlobStoreBackendV8.class);
    }

    @Test
    public void logsSdk8WhenPropertyUnset() throws Exception {
        assertStartupLog(null, null, "Starting blob store using Azure SDK 8", AzureBlobStoreBackendV8.class);
    }

    @Test
    public void propertiesOverrideSystemPropertyWhenEnablingSdk12() throws Exception {
        assertStartupLog("false", "true", "Starting blob store using Azure SDK 12", AzureBlobStoreBackendV12.class);
    }

    @Test
    public void propertiesOverrideSystemPropertyWhenDisablingSdk12() throws Exception {
        assertStartupLog("true", "false", "Starting blob store using Azure SDK 8", AzureBlobStoreBackendV8.class);
    }

    private void assertStartupLog(String systemPropertyValue, String configuredValue, String expectedMessage, Class<?> backendType) throws Exception {
        if (systemPropertyValue == null) {
            System.clearProperty(AZURE_SDK_12_ENABLED);
        } else {
            System.setProperty(AZURE_SDK_12_ENABLED, systemPropertyValue);
        }

        ListAppender<ILoggingEvent> appender = subscribeAppender();
        try {
            String containerName = "it-" + UUID.randomUUID();
            Properties props = createAzuriteProperties(containerName, configuredValue);
            azuriteContainer = AzureBlobContainers.create(props);

            dataStore = new AzureDataStore();
            dataStore.setProperties(props);
            dataStore.setCacheSize(0);
            dataStore.init(folder.newFolder().getAbsolutePath());

            assertTrue(backendType.isInstance(dataStore.getBackend()));
            assertTrue(getMessages(appender).contains(expectedMessage));
        } finally {
            unsubscribe(appender);
        }
    }

    private static String getConnectionString() {
        return azurite.getConnectionString();
    }

    private Properties createAzuriteProperties(String containerName, String configuredValue) {
        Properties properties = new Properties();
        properties.setProperty(AzureConstants.AZURE_BLOB_CONTAINER_NAME, containerName);
        properties.setProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, AzuriteDockerRule.ACCOUNT_NAME);
        properties.setProperty(AzureConstants.AZURE_BLOB_ENDPOINT, azurite.getBlobEndpoint());
        properties.setProperty(AzureConstants.AZURE_CONNECTION_STRING, getConnectionString());
        if (configuredValue != null) {
            properties.setProperty(AzureConstants.AZURE_V12_ENABLED_PROPERTY, configuredValue);
        }
        properties.setProperty("azureCreateContainer", "true");
        properties.setProperty("refOnInit", "true");
        return properties;
    }

    private static List<String> getMessages(ListAppender<ILoggingEvent> appender) {
        return appender.list.stream().map(ILoggingEvent::getFormattedMessage).collect(Collectors.toList());
    }

    private static ListAppender<ILoggingEvent> subscribeAppender() {
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        ((Logger) LoggerFactory.getLogger(AzureDataStore.class)).addAppender(appender);
        return appender;
    }

    private static void unsubscribe(ListAppender<ILoggingEvent> appender) {
        ((Logger) LoggerFactory.getLogger(AzureDataStore.class)).detachAppender(appender);
        appender.stop();
    }

    private void restoreProperty() {
        if (originalProperty == null) {
            System.clearProperty(AZURE_SDK_12_ENABLED);
        } else {
            System.setProperty(AZURE_SDK_12_ENABLED, originalProperty);
        }
    }
}
