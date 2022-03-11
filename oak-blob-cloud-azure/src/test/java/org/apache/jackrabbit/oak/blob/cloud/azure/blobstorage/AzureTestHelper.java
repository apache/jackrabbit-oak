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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.SharedAccessBlobPermissions;
import com.microsoft.azure.storage.blob.SharedAccessBlobPolicy;
import java.io.File;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.EnumSet;
import java.util.Properties;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.spi.blob.SharedBackend;
import org.jetbrains.annotations.NotNull;

public class AzureTestHelper {
    static final String CONTAINER_NAME = "blobstore";

    private AzureTestHelper() {
    }

    static CloudBlobContainer getContainer(AzuriteDockerRule azurite) throws Exception {
        return azurite.getContainer(AzureTestHelper.CONTAINER_NAME);
    }

    @NotNull
    static DataIdentifier writeBlob(SharedBackend sharedBackend) throws DataStoreException {
        return writeBlob(sharedBackend, "testABCD");
    }
    
    @NotNull
    static DataIdentifier writeBlob(SharedBackend sharedBackend, String id) throws DataStoreException {
        DataIdentifier identifier = new DataIdentifier(id);
        sharedBackend.write(identifier, new File("src/test/resources/blobs/test.txt"));
        return identifier;
    }

    static Properties getConfigurationWithSasToken(String sasToken, AzuriteDockerRule azurite) {
        Properties properties = getBasicConfiguration(azurite);
        properties.setProperty(AzureConstants.AZURE_SAS, sasToken);
        properties.setProperty(AzureConstants.AZURE_CREATE_CONTAINER, "false");
        return properties;
    }

    static Properties getConfigurationWithAccessKey(AzuriteDockerRule azurite) {
        Properties properties = getBasicConfiguration(azurite);
        properties.setProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY, AzuriteDockerRule.ACCOUNT_KEY);
        return properties;
    }

    @NotNull
    static Properties getConfigurationWithConnectionString(AzuriteDockerRule azurite) {
        Properties properties = getBasicConfiguration(azurite);
        properties.setProperty(AzureConstants.AZURE_CONNECTION_STRING, getConnectionString(azurite));
        return properties;
    }

    @NotNull
    static Properties getBasicConfiguration(AzuriteDockerRule azurite) {
        Properties properties = new Properties();
        properties.setProperty(AzureConstants.AZURE_BLOB_CONTAINER_NAME, CONTAINER_NAME);
        properties.setProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, AzuriteDockerRule.ACCOUNT_NAME);
        properties.setProperty(AzureConstants.AZURE_BLOB_ENDPOINT, azurite.getBlobEndpoint());
        properties.setProperty(AzureConstants.AZURE_CREATE_CONTAINER, "");
        properties.setProperty(AzureConstants.PRESIGNED_HTTP_DOWNLOAD_URI_EXPIRY_SECONDS, "60");
        properties.put("repository.home", "target");
        return properties;
    }

    private static String getConnectionString(AzuriteDockerRule azurite) {
        return Utils.getConnectionString(
            AzuriteDockerRule.ACCOUNT_NAME, 
            AzuriteDockerRule.ACCOUNT_KEY, 
            azurite.getBlobEndpoint()
        );
    }

    @NotNull
    static SharedAccessBlobPolicy policy(EnumSet<SharedAccessBlobPermissions> permissions, Instant expirationTime) {
        SharedAccessBlobPolicy sharedAccessBlobPolicy = new SharedAccessBlobPolicy();
        sharedAccessBlobPolicy.setPermissions(permissions);
        sharedAccessBlobPolicy.setSharedAccessExpiryTime(Date.from(expirationTime));
        return sharedAccessBlobPolicy;
    }

    @NotNull
    static SharedAccessBlobPolicy policy(EnumSet<SharedAccessBlobPermissions> permissions) {
        return policy(permissions, Instant.now().plus(Duration.ofDays(7)));
    }
}
