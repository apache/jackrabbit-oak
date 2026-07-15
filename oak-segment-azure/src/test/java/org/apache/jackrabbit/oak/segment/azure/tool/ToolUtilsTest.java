/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.jackrabbit.oak.segment.azure.tool;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.read.ListAppender;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzuriteDockerRule;
import org.apache.jackrabbit.oak.segment.azure.AzurePersistence;
import org.apache.jackrabbit.oak.segment.azure.util.Environment;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.AZURE_ACCOUNT_NAME;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.AZURE_CLIENT_ID;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.AZURE_CLIENT_SECRET;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.AZURE_SECRET_KEY;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.AZURE_TENANT_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeNotNull;

public class ToolUtilsTest {
    private static final Environment ENVIRONMENT = new Environment();

    private static final String CONTAINER_URL_FORMAT = "https://%s.blob.core.windows.net/%s";
    private static final String SEGMENT_STORE_PATH_FORMAT = CONTAINER_URL_FORMAT + "/%s";

    private static final String DEFAULT_ACCOUNT_NAME = "myaccount";
    private static final String DEFAULT_CONTAINER_NAME = "oak";
    private static final String DEFAULT_REPO_DIR = "repository";
    private static final String DEFAULT_SEGMENT_STORE_PATH = String.format(SEGMENT_STORE_PATH_FORMAT, DEFAULT_ACCOUNT_NAME, DEFAULT_CONTAINER_NAME, DEFAULT_REPO_DIR);
    public static final String AZURE_SECRET_KEY_WARNING = "AZURE_CLIENT_ID, AZURE_CLIENT_SECRET and AZURE_TENANT_ID environment variables empty or missing. Switching to authentication with AZURE_SECRET_KEY.";

    private final TestEnvironment environment = new TestEnvironment();

    @Test
    public void createAzurePersistenceWithAccessKey() {
        environment.setVariable(AZURE_SECRET_KEY, AzuriteDockerRule.ACCOUNT_KEY);

        final ListAppender<ILoggingEvent> logAppender = subscribeAppender();

        AzurePersistence azurePersistence = ToolUtils.createAzurePersistence(DEFAULT_SEGMENT_STORE_PATH, environment);

        assertTrue(checkLogContainsMessage(AZURE_SECRET_KEY_WARNING, logAppender.list.stream().map(ILoggingEvent::getFormattedMessage).collect(Collectors.toList())));
        assertEquals(Level.WARN, logAppender.list.get(0).getLevel());

        assertEquals(DEFAULT_ACCOUNT_NAME, azurePersistence.getReadBlobContainerClient().getAccountName());
        assertEquals(DEFAULT_CONTAINER_NAME, azurePersistence.getReadBlobContainerClient().getBlobContainerName());
        unsubscribe(logAppender);
    }

    @Test
    public void createAzurePersistenceFailsWhenAccessKeyNotPresent() {
        environment.setVariable(AZURE_SECRET_KEY, null);
        assertThrows(IllegalArgumentException.class, () ->
                ToolUtils.createAzurePersistence(DEFAULT_SEGMENT_STORE_PATH, environment)
        );
    }

    @Test
    public void createAzurePersistenceFailsWhenAccessKeyIsInvalid() {
        environment.setVariable(AZURE_SECRET_KEY, "invalid");
        AzurePersistence azurePersistence = ToolUtils.createAzurePersistence(DEFAULT_SEGMENT_STORE_PATH, environment);

        assertThrows(Exception.class, () ->
                azurePersistence.getReadBlobContainerClient().exists()
        );
    }

    @Test
    public void createAzurePersistenceWithSasUri() {
        String sasToken = "sig=qL%2Fi%2BP7J6S0sA8Ihc%2BKq75U5uJcnukpfktT2fm1ckXk%3D&se=2022-02-09T11%3A52%3A42Z&sv=2019-02-02&sp=rl&sr=c";

        AzurePersistence azurePersistence  = ToolUtils.createAzurePersistence(DEFAULT_SEGMENT_STORE_PATH + '?' + sasToken);

        assertEquals(DEFAULT_CONTAINER_NAME, azurePersistence.getReadBlobContainerClient().getBlobContainerName());
    }

    @Test
    public void createAzurePersistenceWithServicePrincipal() {
        assumeNotNull(ENVIRONMENT.getVariable(AZURE_ACCOUNT_NAME));
        assumeNotNull(ENVIRONMENT.getVariable(AZURE_TENANT_ID));
        assumeNotNull(ENVIRONMENT.getVariable(AZURE_CLIENT_ID));
        assumeNotNull(ENVIRONMENT.getVariable(AZURE_CLIENT_SECRET));

        String accountName = ENVIRONMENT.getVariable(AZURE_ACCOUNT_NAME);
        String containerName = "oak";
        String segmentStorePath = String.format(SEGMENT_STORE_PATH_FORMAT, accountName, containerName, DEFAULT_REPO_DIR);

        AzurePersistence azurePersistence = ToolUtils.createAzurePersistence(segmentStorePath, ENVIRONMENT);
        assertNotNull(azurePersistence);
        assertEquals(containerName, azurePersistence.getReadBlobContainerClient().getBlobContainerName());
    }

    private boolean checkLogContainsMessage(String toCheck, List<String> messages) {
        for (String message : messages) {
            if (message.equals(toCheck)) {
                return true;
            }
        }

        return false;
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

    static class TestEnvironment extends Environment {
        private final Map<String, String> envs = new HashMap<>();

        @Override
        public String getVariable(String name) {
            return envs.get(name);
        }

        public String setVariable(String name, String value) {
            return envs.put(name, value);
        }

        @Override
        public Map<String, String> getVariables() {
            return Collections.unmodifiableMap(envs);
        }
    }

}