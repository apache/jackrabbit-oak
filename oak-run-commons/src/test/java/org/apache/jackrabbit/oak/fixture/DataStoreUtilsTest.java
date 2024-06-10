package org.apache.jackrabbit.oak.fixture;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.read.ListAppender;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.Utils;
import org.jetbrains.annotations.NotNull;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DataStoreUtilsTest {
    private static final String AZURE_ACCOUNT_NAME = "AZURE_ACCOUNT_NAME";
    private static final String AZURE_SECRET_KEY = "AZURE_SECRET_KEY";
    private static final String AZURE_TENANT_ID = "AZURE_TENANT_ID";
    private static final String AZURE_CLIENT_ID = "AZURE_CLIENT_ID";
    private static final String AZURE_CLIENT_SECRET = "AZURE_CLIENT_SECRET";
    private static final String AZURE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s";

    private static final String CONTAINER_DOES_NOT_EXIST_MESSAGE = "container [%s] doesn't exists";
    private static final String CONTAINER_DELETED_MESSAGE = "container [%s] deleted";

    @Test
    public void delete_non_existing_container_azure_connection_string() throws Exception {
        final String accountName = getEnvironmentVariable(AZURE_ACCOUNT_NAME);
        final String accessKey = getEnvironmentVariable(AZURE_SECRET_KEY);

        Assume.assumeNotNull(accountName);
        Assume.assumeNotNull(accessKey);

        final String azureConnectionString = String.format(AZURE_CONNECTION_STRING, accountName, accessKey);
        final String containerName = "oak-test-" + UUID.randomUUID();
        ListAppender<ILoggingEvent> logAppender = subscribeAppender();

        DataStoreUtils.deleteAzureContainer(getConfigMap(azureConnectionString, null, null, null, null, null), containerName);

        Set<String> logMessages = getLogMessages(logAppender);
        assertTrue(logMessages.contains("Connecting to azure storage via azure connection string."));
        assertFalse(logMessages.contains("Connecting to azure storage via service principal credentials."));
        assertFalse(logMessages.contains("Connecting to azure storage via access key."));
        assertTrue(logMessages.contains(String.format(CONTAINER_DOES_NOT_EXIST_MESSAGE, containerName)));

        unsubscribe(logAppender);
    }

    @Test
    public void delete_existing_container_azure_connection_string() throws Exception {
        final String accountName = getEnvironmentVariable(AZURE_ACCOUNT_NAME);
        final String accessKey = getEnvironmentVariable(AZURE_SECRET_KEY);

        Assume.assumeNotNull(accountName);
        Assume.assumeNotNull(accessKey);

        final String azureConnectionString = String.format(AZURE_CONNECTION_STRING, accountName, accessKey);
        final String containerName = "oak-test-" + UUID.randomUUID();
        // create container before deleting
        CloudBlobContainer container = Utils.getBlobContainer(azureConnectionString, containerName);
        container.createIfNotExists();

        ListAppender<ILoggingEvent> logAppender = subscribeAppender();

        DataStoreUtils.deleteAzureContainer(getConfigMap(azureConnectionString, null, null, null, null, null), containerName);

        Set<String> logMessages = getLogMessages(logAppender);
        assertTrue(logMessages.contains("Connecting to azure storage via azure connection string."));
        assertFalse(logMessages.contains("Connecting to azure storage via service principal credentials."));
        assertFalse(logMessages.contains("Connecting to azure storage via access key."));
        assertTrue(logMessages.contains(String.format(CONTAINER_DELETED_MESSAGE, containerName)));

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
        final String containerName = "oak-test-" + UUID.randomUUID();
        ListAppender<ILoggingEvent> logAppender = subscribeAppender();

        DataStoreUtils.deleteAzureContainer(getConfigMap(null, accountName, null, clientId, clientSecret, tenantId), containerName);

        Set<String> logMessages = getLogMessages(logAppender);
        assertTrue(logMessages.contains("Connecting to azure storage via service principal credentials."));
        assertFalse(logMessages.contains("Connecting to azure storage via azure connection string."));
        assertFalse(logMessages.contains("Connecting to azure storage via access key."));
        assertTrue(logMessages.contains(String.format(CONTAINER_DOES_NOT_EXIST_MESSAGE, containerName)));

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
        final String containerName = "oak-test-" + UUID.randomUUID();

        // create container before deleting
        CloudBlobContainer container = Utils.getContainerFromServicePrincipalCredentials(null, accountName, containerName, clientId, clientSecret, tenantId);
        container.createIfNotExists();

        ListAppender<ILoggingEvent> logAppender = subscribeAppender();

        DataStoreUtils.deleteAzureContainer(getConfigMap(null, accountName, null, clientId, clientSecret, tenantId), containerName);

        Set<String> logMessages = getLogMessages(logAppender);
        assertTrue(logMessages.contains("Connecting to azure storage via service principal credentials."));
        assertFalse(logMessages.contains("Connecting to azure storage via azure connection string."));
        assertFalse(logMessages.contains("Connecting to azure storage via access key."));
        assertTrue(logMessages.contains(String.format(CONTAINER_DELETED_MESSAGE, containerName)));

        unsubscribe(logAppender);
    }

    @Test
    public void delete_non_existing_container_access_key() throws Exception {
        final String accountName = getEnvironmentVariable(AZURE_ACCOUNT_NAME);
        final String accessKey = getEnvironmentVariable(AZURE_SECRET_KEY);

        Assume.assumeNotNull(accountName);
        Assume.assumeNotNull(accessKey);
        final String containerName = "oak-test-" + UUID.randomUUID();

        ListAppender<ILoggingEvent> logAppender = subscribeAppender();

        DataStoreUtils.deleteAzureContainer(getConfigMap(null, accountName, accessKey, null, null, null), containerName);

        Set<String> logMessages = getLogMessages(logAppender);
        assertTrue(logMessages.contains("Connecting to azure storage via access key."));
        assertFalse(logMessages.contains("Connecting to azure storage via service principal credentials."));
        assertFalse(logMessages.contains("Connecting to azure storage via azure connection string."));
        assertTrue(logMessages.contains(String.format(CONTAINER_DOES_NOT_EXIST_MESSAGE, containerName)));

        unsubscribe(logAppender);
    }

    @Test
    public void delete_existing_container_access_key() throws Exception {
        final String accountName = getEnvironmentVariable(AZURE_ACCOUNT_NAME);
        final String accessKey = getEnvironmentVariable(AZURE_SECRET_KEY);

        Assume.assumeNotNull(accountName);
        Assume.assumeNotNull(accessKey);
        final String containerName = "oak-test-" + UUID.randomUUID();

        // create container before deleting
        String connectionString = org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.Utils.getConnectionString(accountName, accessKey);
        CloudBlobContainer container = org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.Utils.getBlobContainer(connectionString, containerName);
        container.createIfNotExists();

        ListAppender<ILoggingEvent> logAppender = subscribeAppender();

        DataStoreUtils.deleteAzureContainer(getConfigMap(null, accountName, accessKey, null, null, null), containerName);

        Set<String> logMessages = getLogMessages(logAppender);
        assertTrue(logMessages.contains("Connecting to azure storage via access key."));
        assertFalse(logMessages.contains("Connecting to azure storage via service principal credentials."));
        assertFalse(logMessages.contains("Connecting to azure storage via azure connection string."));
        assertTrue(logMessages.contains(String.format(CONTAINER_DELETED_MESSAGE, containerName)));

        unsubscribe(logAppender);
    }

    private Set<String> getLogMessages(ListAppender<ILoggingEvent> logAppender) {
        return Optional.ofNullable(logAppender.list)
                .orElse(Collections.emptyList())
                .stream()
                .map(ILoggingEvent::getFormattedMessage)
                .filter(StringUtils::isNotBlank)
                .collect(Collectors.toSet());
    }

    private String getEnvironmentVariable(String variableName) {
        return System.getenv(variableName);
    }

    private Map<String, ?> getConfigMap(String connectionString,
                                        String accountName,
                                        String accessKey,
                                        String clientId,
                                        String clientSecret,
                                        String tenantId) {
        Map<String, String> config = new HashMap<>();
        config.put(AzureConstants.AZURE_CONNECTION_STRING, connectionString);
        config.put(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, accountName);
        config.put(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY, accessKey);
        config.put(AzureConstants.AZURE_CLIENT_ID, clientId);
        config.put(AzureConstants.AZURE_CLIENT_SECRET, clientSecret);
        config.put(AzureConstants.AZURE_TENANT_ID, tenantId);
        return config;
    }

    private ListAppender<ILoggingEvent> subscribeAppender() {
        ListAppender<ILoggingEvent> appender = new ListAppender<ILoggingEvent>();
        appender.setContext((LoggerContext) LoggerFactory.getILoggerFactory());
        appender.setName("asynclogcollector");
        appender.start();
        ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger(
                ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME).addAppender(appender);
        return appender;
    }

    private void unsubscribe(@NotNull final Appender<ILoggingEvent> appender) {
        ((LoggerContext) LoggerFactory.getILoggerFactory()).getLogger(
                ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME).detachAppender(appender);
    }
}
