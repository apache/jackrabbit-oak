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

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.read.ListAppender;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureBlobContainerProvider;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzuriteDockerRule;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DataStoreUtilsTest {
    @ClassRule
    public static AzuriteDockerRule azuriteDockerRule = new AzuriteDockerRule();

    private static final String AZURE_ACCOUNT_NAME = "AZURE_ACCOUNT_NAME";
    private static final String AZURE_TENANT_ID = "AZURE_TENANT_ID";
    private static final String AZURE_CLIENT_ID = "AZURE_CLIENT_ID";
    private static final String AZURE_CLIENT_SECRET = "AZURE_CLIENT_SECRET";
    private static final String AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=%s";
    private static final String CONTAINER_NAME = "oak-blob-test";
    private static final String AUTHENTICATE_VIA_AZURE_CONNECTION_STRING_LOG = "connecting to azure blob storage via azureConnectionString";
    private static final String AUTHENTICATE_VIA_ACCESS_KEY_LOG = "connecting to azure blob storage via access key";
    private static final String AUTHENTICATE_VIA_SERVICE_PRINCIPALS_LOG = "connecting to azure blob storage via service principal credentials";
    private static final String AUTHENTICATE_VIA_SAS_TOKEN_LOG = "connecting to azure blob storage via sas token";
    private static final String REFRESH_TOKEN_EXECUTOR_SHUTDOWN_LOG = "Refresh token executor service shutdown completed";
    private static final String CONTAINER_DOES_NOT_EXIST_MESSAGE = "container [%s] doesn't exists";
    private static final String CONTAINER_DELETED_MESSAGE = "container [%s] deleted";

    private CloudBlobContainer container;

    @Before
    public void init() throws URISyntaxException, InvalidKeyException, StorageException {
        container = azuriteDockerRule.getContainer(CONTAINER_NAME);
        assertTrue(container.exists());
    }

    @After
    public void cleanup() throws StorageException {
        if (container != null) {
            container.deleteIfExists();
        }
    }

    @Test
    public void delete_non_existing_container_azure_connection_string() throws Exception {
        ListAppender<ILoggingEvent> logAppender = subscribeAppender();
        final String azureConnectionString = String.format(AZURE_CONNECTION_STRING, AzuriteDockerRule.ACCOUNT_NAME, AzuriteDockerRule.ACCOUNT_KEY, azuriteDockerRule.getBlobEndpoint());
        final String newContainerName = getNewContainerName();
        DataStoreUtils.deleteAzureContainer(getConfigMap(azureConnectionString, null, null, null, null, null, null, null),
                newContainerName);
        validate(Arrays.asList(AUTHENTICATE_VIA_AZURE_CONNECTION_STRING_LOG,
                        REFRESH_TOKEN_EXECUTOR_SHUTDOWN_LOG, String.format(CONTAINER_DOES_NOT_EXIST_MESSAGE, newContainerName)),
                Arrays.asList(AUTHENTICATE_VIA_SERVICE_PRINCIPALS_LOG, AUTHENTICATE_VIA_SAS_TOKEN_LOG, AUTHENTICATE_VIA_ACCESS_KEY_LOG),
                getLogMessages(logAppender));
        unsubscribe(logAppender);
    }

    @Test
    public void delete_existing_container_azure_connection_string() throws Exception {
        final String azureConnectionString = String.format(AZURE_CONNECTION_STRING, AzuriteDockerRule.ACCOUNT_NAME, AzuriteDockerRule.ACCOUNT_KEY, azuriteDockerRule.getBlobEndpoint());
        ListAppender<ILoggingEvent> logAppender = subscribeAppender();

        DataStoreUtils.deleteAzureContainer(getConfigMap(azureConnectionString, null, null, null, null, null, null, null),
                CONTAINER_NAME);
        validate(Arrays.asList(AUTHENTICATE_VIA_AZURE_CONNECTION_STRING_LOG, REFRESH_TOKEN_EXECUTOR_SHUTDOWN_LOG,
                        String.format(CONTAINER_DELETED_MESSAGE, CONTAINER_NAME)),
                Arrays.asList(AUTHENTICATE_VIA_SERVICE_PRINCIPALS_LOG, AUTHENTICATE_VIA_SAS_TOKEN_LOG, AUTHENTICATE_VIA_ACCESS_KEY_LOG),
                getLogMessages(logAppender));
        assertFalse(container.exists());
        unsubscribe(logAppender);
    }

    @Test
    public void delete_non_existing_container_service_principal() throws Exception {
        final String accountName = getEnvironmentVariable(AZURE_ACCOUNT_NAME);
        final String clientId = getEnvironmentVariable(AZURE_CLIENT_ID);
        final String clientSecret = getEnvironmentVariable(AZURE_CLIENT_SECRET);
        final String tenantId = getEnvironmentVariable(AZURE_TENANT_ID);

        Assume.assumeNotNull(accountName);
        Assume.assumeNotNull(clientId);
        Assume.assumeNotNull(clientSecret);
        Assume.assumeNotNull(tenantId);

        ListAppender<ILoggingEvent> logAppender = subscribeAppender();
        final String newContainerName = getNewContainerName();

        DataStoreUtils.deleteAzureContainer(getConfigMap(null, accountName, null, null, null, clientId, clientSecret, tenantId), newContainerName);
        validate(Arrays.asList(AUTHENTICATE_VIA_SERVICE_PRINCIPALS_LOG, REFRESH_TOKEN_EXECUTOR_SHUTDOWN_LOG, String.format(CONTAINER_DOES_NOT_EXIST_MESSAGE, newContainerName)),
                Arrays.asList(AUTHENTICATE_VIA_AZURE_CONNECTION_STRING_LOG, AUTHENTICATE_VIA_SAS_TOKEN_LOG, AUTHENTICATE_VIA_ACCESS_KEY_LOG),
                getLogMessages(logAppender));
        unsubscribe(logAppender);
    }


    @Test
    public void delete_container_service_principal() throws Exception {
        final String accountName = getEnvironmentVariable(AZURE_ACCOUNT_NAME);
        final String clientId = getEnvironmentVariable(AZURE_CLIENT_ID);
        final String clientSecret = getEnvironmentVariable(AZURE_CLIENT_SECRET);
        final String tenantId = getEnvironmentVariable(AZURE_TENANT_ID);

        Assume.assumeNotNull(accountName);
        Assume.assumeNotNull(clientId);
        Assume.assumeNotNull(clientSecret);
        Assume.assumeNotNull(tenantId);

        CloudBlobContainer container;
        try (AzureBlobContainerProvider azureBlobContainerProvider = AzureBlobContainerProvider.Builder.builder(CONTAINER_NAME)
                .withAccountName(accountName)
                .withClientId(clientId)
                .withClientSecret(clientSecret)
                .withTenantId(tenantId).build()) {
            container = azureBlobContainerProvider.getBlobContainer();
            container.createIfNotExists();
        }
        assertNotNull(container);
        assertTrue(container.exists());

        ListAppender<ILoggingEvent> logAppender = subscribeAppender();

        DataStoreUtils.deleteAzureContainer(getConfigMap(null, accountName, null, null, null, clientId, clientSecret, tenantId), CONTAINER_NAME);
        validate(Arrays.asList(AUTHENTICATE_VIA_SERVICE_PRINCIPALS_LOG, REFRESH_TOKEN_EXECUTOR_SHUTDOWN_LOG,
                        String.format(CONTAINER_DELETED_MESSAGE, CONTAINER_NAME)),
                Arrays.asList(AUTHENTICATE_VIA_AZURE_CONNECTION_STRING_LOG, AUTHENTICATE_VIA_SAS_TOKEN_LOG, AUTHENTICATE_VIA_ACCESS_KEY_LOG),
                getLogMessages(logAppender));
        assertFalse(container.exists());
        unsubscribe(logAppender);
    }

    @Test
    public void delete_non_existing_container_access_key() throws Exception {
        ListAppender<ILoggingEvent> logAppender = subscribeAppender();
        final String newContainerName = getNewContainerName();
        DataStoreUtils.deleteAzureContainer(getConfigMap(null, AzuriteDockerRule.ACCOUNT_NAME, AzuriteDockerRule.ACCOUNT_KEY, null, azuriteDockerRule.getBlobEndpoint(), null, null, null),
                newContainerName);

        validate(Arrays.asList(AUTHENTICATE_VIA_ACCESS_KEY_LOG, REFRESH_TOKEN_EXECUTOR_SHUTDOWN_LOG,
                        String.format(CONTAINER_DOES_NOT_EXIST_MESSAGE, newContainerName)),
                Arrays.asList(AUTHENTICATE_VIA_SERVICE_PRINCIPALS_LOG, AUTHENTICATE_VIA_SAS_TOKEN_LOG, AUTHENTICATE_VIA_SERVICE_PRINCIPALS_LOG),
                getLogMessages(logAppender));
        unsubscribe(logAppender);
    }

    @Test
    public void delete_existing_container_access_key() throws Exception {
        ListAppender<ILoggingEvent> logAppender = subscribeAppender();

        DataStoreUtils.deleteAzureContainer(getConfigMap(null, AzuriteDockerRule.ACCOUNT_NAME, AzuriteDockerRule.ACCOUNT_KEY, null, azuriteDockerRule.getBlobEndpoint(), null, null, null), CONTAINER_NAME);
        validate(Arrays.asList(AUTHENTICATE_VIA_ACCESS_KEY_LOG, REFRESH_TOKEN_EXECUTOR_SHUTDOWN_LOG,
                        String.format(CONTAINER_DELETED_MESSAGE, CONTAINER_NAME)),
                Arrays.asList(AUTHENTICATE_VIA_SERVICE_PRINCIPALS_LOG, AUTHENTICATE_VIA_SAS_TOKEN_LOG, AUTHENTICATE_VIA_SERVICE_PRINCIPALS_LOG),
                getLogMessages(logAppender));
        assertFalse(container.exists());
        unsubscribe(logAppender);
    }

    private void validate(List<String> includedLogs, List<String> excludedLogs, Set<String> allLogs) {
        includedLogs.forEach(log -> assertTrue(allLogs.contains(log)));
        excludedLogs.forEach(log -> assertFalse(allLogs.contains(log)));
    }

    private Set<String> getLogMessages(ListAppender<ILoggingEvent> logAppender) {
        return Optional.ofNullable(logAppender.list)
                .orElse(Collections.emptyList())
                .stream()
                .map(ILoggingEvent::getFormattedMessage)
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toSet());
    }

    @NotNull
    private String getNewContainerName() {
        return CONTAINER_NAME + "-" + UUID.randomUUID();
    }

    private String getEnvironmentVariable(String variableName) {
        return System.getenv(variableName);
    }

    private Map<String, ?> getConfigMap(String connectionString,
                                        String accountName,
                                        String accessKey,
                                        String sasToken,
                                        String blobEndpoint,
                                        String clientId,
                                        String clientSecret,
                                        String tenantId) {
        Map<String, String> config = new HashMap<>();
        config.put(AzureConstants.AZURE_CONNECTION_STRING, connectionString);
        config.put(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, accountName);
        config.put(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY, accessKey);
        config.put(AzureConstants.AZURE_BLOB_ENDPOINT, blobEndpoint);
        config.put(AzureConstants.AZURE_CLIENT_ID, clientId);
        config.put(AzureConstants.AZURE_CLIENT_SECRET, clientSecret);
        config.put(AzureConstants.AZURE_TENANT_ID, tenantId);
        config.put(AzureConstants.AZURE_SAS, sasToken);
        return config;
    }

    private ListAppender<ILoggingEvent> subscribeAppender() {
        ListAppender<ILoggingEvent> appender = new ListAppender<ILoggingEvent>();
        appender.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
        appender.setName("asynclogcollector");
        appender.start();
        ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger(
                ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME).addAppender(appender);
        ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger(
                ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME).setLevel(Level.DEBUG);
        return appender;
    }

    private void unsubscribe(@NotNull final Appender<ILoggingEvent> appender) {
        ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger(
                ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME).detachAppender(appender);
    }
}