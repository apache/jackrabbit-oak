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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.jackrabbit.oak.segment.azure.AzureUtilities;
import org.apache.jackrabbit.oak.segment.azure.AzuriteDockerRule;
import org.apache.jackrabbit.oak.segment.azure.util.Environment;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;

public class ToolUtilsTest {
    private static final String CONTAINER_URL = "https://myaccount.blob.core.windows.net/oak-test";
    private static final String REPO_DIR = "repository";
    private static final String SEGMENT_STORE_PATH = CONTAINER_URL + '/' + REPO_DIR;

    private final TestEnvironment environment = new TestEnvironment();

    @Test
    public void createCloudBlobDirectoryWithAccessKey() {
        environment.setVariable("AZURE_SECRET_KEY", AzuriteDockerRule.ACCOUNT_KEY);

        StorageCredentialsAccountAndKey credentials = expectCredentials(
            StorageCredentialsAccountAndKey.class, 
            () -> ToolUtils.createCloudBlobDirectory(SEGMENT_STORE_PATH, environment)
        );
        
        assertEquals("myaccount", credentials.getAccountName());
        assertEquals(AzuriteDockerRule.ACCOUNT_KEY, credentials.exportBase64EncodedKey());
    }

    @Test
    public void createCloudBlobDirectoryFailsWhenAccessKeyNotPresent() {
        environment.setVariable("AZURE_SECRET_KEY", null);
        assertThrows(IllegalArgumentException.class, () ->
            ToolUtils.createCloudBlobDirectory(SEGMENT_STORE_PATH)
        );
    }

    @Test
    public void createCloudBlobDirectoryFailsWhenAccessKeyIsInvalid() {
        environment.setVariable("AZURE_SECRET_KEY", "invalid");
        assertThrows(IllegalArgumentException.class, () ->
            ToolUtils.createCloudBlobDirectory(SEGMENT_STORE_PATH)
        );
    }

    @Test
    public void createCloudBlobDirectoryWithSasUri() {
        String sasToken = "sig=qL%2Fi%2BP7J6S0sA8Ihc%2BKq75U5uJcnukpfktT2fm1ckXk%3D&se=2022-02-09T11%3A52%3A42Z&sv=2019-02-02&sp=rl&sr=c";

        StorageCredentialsSharedAccessSignature credentials = expectCredentials(
            StorageCredentialsSharedAccessSignature.class, 
            () -> ToolUtils.createCloudBlobDirectory(SEGMENT_STORE_PATH + '?' + sasToken)
        );

        assertEquals(sasToken, credentials.getToken());
        assertNull("AccountName should be null when SAS credentials are used", credentials.getAccountName());
    }

    private static <T extends StorageCredentials> T expectCredentials(Class<T> clazz, Runnable body) {
        ArgumentCaptor<T> credentialsCaptor = ArgumentCaptor.forClass(clazz);
        try (MockedStatic<AzureUtilities> mockedAzureUtilities = mockStatic(AzureUtilities.class)) {
            body.run();

            mockedAzureUtilities.verify(() -> AzureUtilities.cloudBlobDirectoryFrom(
                    credentialsCaptor.capture(),
                    eq(CONTAINER_URL),
                    eq(REPO_DIR)
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