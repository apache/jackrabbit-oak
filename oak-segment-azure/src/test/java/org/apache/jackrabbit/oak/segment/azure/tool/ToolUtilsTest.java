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

import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature;

import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzuriteDockerRule;
import org.apache.jackrabbit.oak.segment.azure.AzureUtilities;
import org.apache.jackrabbit.oak.segment.azure.util.Environment;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.AZURE_ACCOUNT_NAME;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.AZURE_CLIENT_ID;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.AZURE_CLIENT_SECRET;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.AZURE_SECRET_KEY;
import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.AZURE_TENANT_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeNotNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;

public class ToolUtilsTest {
    private static final Environment ENVIRONMENT = new Environment();

    private static final String CONTAINER_URL_FORMAT = "https://%s.blob.core.windows.net/%s";
    private static final String SEGMENT_STORE_PATH_FORMAT = CONTAINER_URL_FORMAT + "/%s";

    private static final String DEFAULT_ACCOUNT_NAME = "myaccount";
    private static final String DEFAULT_CONTAINER_NAME = "oak";
    private static final String DEFAULT_REPO_DIR = "repository";
    private static final String DEFAULT_CONTAINER_URL = String.format(CONTAINER_URL_FORMAT, DEFAULT_ACCOUNT_NAME, DEFAULT_CONTAINER_NAME);
    private static final String DEFAULT_SEGMENT_STORE_PATH = String.format(SEGMENT_STORE_PATH_FORMAT, DEFAULT_ACCOUNT_NAME, DEFAULT_CONTAINER_NAME, DEFAULT_REPO_DIR);

    private final TestEnvironment environment = new TestEnvironment();

    @Test
    public void createCloudBlobDirectoryWithAccessKey() {
        environment.setVariable(AZURE_SECRET_KEY, AzuriteDockerRule.ACCOUNT_KEY);

        StorageCredentialsAccountAndKey credentials = expectCredentials(
            StorageCredentialsAccountAndKey.class, 
            () -> ToolUtils.createCloudBlobDirectory(DEFAULT_SEGMENT_STORE_PATH, environment),
            DEFAULT_CONTAINER_URL
        );
        
        assertEquals(DEFAULT_ACCOUNT_NAME, credentials.getAccountName());
        assertEquals(AzuriteDockerRule.ACCOUNT_KEY, credentials.exportBase64EncodedKey());
    }

    @Test
    public void createCloudBlobDirectoryFailsWhenAccessKeyNotPresent() {
        environment.setVariable(AZURE_SECRET_KEY, null);
        assertThrows(IllegalArgumentException.class, () ->
            ToolUtils.createCloudBlobDirectory(DEFAULT_SEGMENT_STORE_PATH, environment)
        );
    }

    @Test
    public void createCloudBlobDirectoryFailsWhenAccessKeyIsInvalid() {
        environment.setVariable(AZURE_SECRET_KEY, "invalid");
        assertThrows(IllegalArgumentException.class, () ->
            ToolUtils.createCloudBlobDirectory(DEFAULT_SEGMENT_STORE_PATH, environment)
        );
    }

    @Test
    public void createCloudBlobDirectoryWithSasUri() {
        String sasToken = "sig=qL%2Fi%2BP7J6S0sA8Ihc%2BKq75U5uJcnukpfktT2fm1ckXk%3D&se=2022-02-09T11%3A52%3A42Z&sv=2019-02-02&sp=rl&sr=c";

        StorageCredentialsSharedAccessSignature credentials = expectCredentials(
            StorageCredentialsSharedAccessSignature.class, 
            () -> ToolUtils.createCloudBlobDirectory(DEFAULT_SEGMENT_STORE_PATH + '?' + sasToken),
            DEFAULT_CONTAINER_URL
        );

        assertEquals(sasToken, credentials.getToken());
        assertNull("AccountName should be null when SAS credentials are used", credentials.getAccountName());
    }

    @Test
    public void createCloudBlobDirectoryWithServicePrincipal() throws URISyntaxException, StorageException {
        assumeNotNull(ENVIRONMENT.getVariable(AZURE_ACCOUNT_NAME));
        assumeNotNull(ENVIRONMENT.getVariable(AZURE_TENANT_ID));
        assumeNotNull(ENVIRONMENT.getVariable(AZURE_CLIENT_ID));
        assumeNotNull(ENVIRONMENT.getVariable(AZURE_CLIENT_SECRET));

        String accountName = ENVIRONMENT.getVariable(AZURE_ACCOUNT_NAME);
        String containerName = "oak";
        String segmentStorePath = String.format(SEGMENT_STORE_PATH_FORMAT, accountName, containerName, DEFAULT_REPO_DIR);

        CloudBlobDirectory cloudBlobDirectory = ToolUtils.createCloudBlobDirectory(segmentStorePath, ENVIRONMENT);
        assertNotNull(cloudBlobDirectory);
        assertEquals(containerName, cloudBlobDirectory.getContainer().getName());
    }

    private static <T extends StorageCredentials> T expectCredentials(Class<T> clazz, Runnable body, String containerUrl) {
        ArgumentCaptor<T> credentialsCaptor = ArgumentCaptor.forClass(clazz);
        try (MockedStatic<AzureUtilities> mockedAzureUtilities = mockStatic(AzureUtilities.class)) {
            body.run();

            mockedAzureUtilities.verify(() -> AzureUtilities.cloudBlobDirectoryFrom(
                    credentialsCaptor.capture(),
                    eq(containerUrl),
                    eq(DEFAULT_REPO_DIR)
                )
            );
            return credentialsCaptor.getValue();
        }
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