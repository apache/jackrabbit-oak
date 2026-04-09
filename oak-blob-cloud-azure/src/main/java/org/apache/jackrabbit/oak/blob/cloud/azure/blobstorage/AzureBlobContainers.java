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

import java.util.Optional;
import java.util.Properties;

import org.apache.jackrabbit.oak.spi.blob.data.DataStoreException;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v12.AzureBlobContainerProviderV12;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v12.AzureBlobContainerV12;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v8.AzureBlobContainerProviderV8;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v8.AzureBlobContainerV8;

public final class AzureBlobContainers {
    private AzureBlobContainers() {
    }

    public static boolean deleteIfExists(Properties properties) throws DataStoreException {
        try (AzureBlobContainer container = getReference(properties)) {
            return container.deleteIfExists();
        } catch (DataStoreException e) {
            throw e;
        } catch (Exception e) {
            throw new DataStoreException(e);
        }
    }

    public static AzureBlobContainer create(Properties properties) throws DataStoreException {
        try {
            AzureBlobContainer container = getReference(properties);
            container.createIfNotExists();
            return container;
        } catch (DataStoreException e) {
            throw e;
        } catch (Exception e) {
            throw new DataStoreException(e);
        }
    }

    public static Optional<AzureBlobContainer> get(Properties properties) throws DataStoreException {
        try {
            AzureBlobContainer container = getReference(properties);
            if (container.exists()) {
                return Optional.of(container);
            }
            container.close();
            return Optional.empty();
        } catch (DataStoreException e) {
            throw e;
        } catch (Exception e) {
            throw new DataStoreException(e);
        }
    }

    public static AzureBlobContainer getReference(Properties properties) throws DataStoreException {
        AzureSdkVersion version = AzureSdkVersion.resolve(properties);
        if (version == AzureSdkVersion.V12) {
            AzureBlobContainerProviderV12 provider = AzureBlobContainerProviderV12.Builder
                    .builder(properties.getProperty(AzureConstants.AZURE_BLOB_CONTAINER_NAME))
                    .initializeWithProperties(properties)
                    .build();
            return new AzureBlobContainerV12(provider.getBlobContainer(null, properties));
        }
        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(properties.getProperty(AzureConstants.AZURE_BLOB_CONTAINER_NAME))
                .initializeWithProperties(properties)
                .build();
        return new AzureBlobContainerV8(provider.getBlobContainer(), provider);
    }
}
