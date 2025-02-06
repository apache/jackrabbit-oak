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
package org.apache.jackrabbit.oak.segment.azure.tool;

import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import org.apache.jackrabbit.oak.segment.azure.v8.AzurePersistenceV8;
import org.apache.jackrabbit.oak.segment.azure.v8.AzureStorageCredentialManagerV8;
import org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils;
import org.apache.jackrabbit.oak.segment.azure.util.Environment;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.junit.Test;

import static org.apache.jackrabbit.oak.segment.azure.v8.AzureUtilitiesV8.AZURE_ACCOUNT_NAME;
import static org.apache.jackrabbit.oak.segment.azure.v8.AzureUtilitiesV8.AZURE_CLIENT_ID;
import static org.apache.jackrabbit.oak.segment.azure.v8.AzureUtilitiesV8.AZURE_CLIENT_SECRET;
import static org.apache.jackrabbit.oak.segment.azure.v8.AzureUtilitiesV8.AZURE_TENANT_ID;
import static org.junit.Assume.assumeNotNull;

public class SegmentCopyAzureServicePrincipalToTarTest extends SegmentCopyTestBase {
    private static final Environment ENVIRONMENT = new Environment();
    private static final String CONTAINER_NAME = "oak";
    private static final String DIR = "repository";
    private static final String SEGMENT_STORE_PATH_FORMAT = "https://%s.blob.core.windows.net/%s/%s";

    @Test
    @Override
    public void testSegmentCopy() throws Exception {
        assumeNotNull(ENVIRONMENT.getVariable(AZURE_ACCOUNT_NAME));
        assumeNotNull(ENVIRONMENT.getVariable(AZURE_TENANT_ID));
        assumeNotNull(ENVIRONMENT.getVariable(AZURE_CLIENT_ID));
        assumeNotNull(ENVIRONMENT.getVariable(AZURE_CLIENT_SECRET));

        super.testSegmentCopy();
    }

    @Override
    protected SegmentNodeStorePersistence getSrcPersistence() {
        String accountName = ENVIRONMENT.getVariable(AZURE_ACCOUNT_NAME);
        String path = String.format(SEGMENT_STORE_PATH_FORMAT, accountName, CONTAINER_NAME, DIR);
        CloudBlobDirectory cloudBlobDirectory;
        try (AzureStorageCredentialManagerV8 azureStorageCredentialManagerV8 = new AzureStorageCredentialManagerV8()) {
            cloudBlobDirectory = ToolUtils.createCloudBlobDirectory(path, ENVIRONMENT, azureStorageCredentialManagerV8);
        }
        return new AzurePersistenceV8(cloudBlobDirectory);
    }

    @Override
    protected SegmentNodeStorePersistence getDestPersistence() {
        return getTarPersistence();
    }

    @Override
    protected String getSrcPathOrUri() {
        String accountName = ENVIRONMENT.getVariable(AZURE_ACCOUNT_NAME);

        StringBuilder sb = new StringBuilder();
        sb.append("az:");
        sb.append(String.format(SEGMENT_STORE_PATH_FORMAT, accountName, CONTAINER_NAME, DIR));

        return sb.toString();
    }

    @Override
    protected String getDestPathOrUri() {
        return getTarPersistencePathOrUri();
    }
}
