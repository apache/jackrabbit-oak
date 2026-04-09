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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v8;

import java.io.InputStream;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Objects;

import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.SharedAccessBlobPermissions;
import com.microsoft.azure.storage.blob.SharedAccessBlobPolicy;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureBlobContainer;

public class AzureBlobContainerV8 implements AzureBlobContainer {
    private final CloudBlobContainer container;
    private final AzureBlobContainerProviderV8 provider;

    public AzureBlobContainerV8(CloudBlobContainer container, AzureBlobContainerProviderV8 provider) {
        this.container = Objects.requireNonNull(container);
        this.provider = Objects.requireNonNull(provider);
    }

    @Override
    public void createIfNotExists() throws Exception {
        container.createIfNotExists();
    }

    @Override
    public void delete() throws Exception {
        container.delete();
    }

    @Override
    public boolean deleteIfExists() throws Exception {
        return container.deleteIfExists();
    }

    @Override
    public boolean exists() throws Exception {
        return container.exists();
    }

    @Override
    public String getName() {
        return container.getName();
    }

    @Override
    public String getContainerUri() {
        return container.getUri().toString();
    }

    @Override
    public void uploadBlockBlob(String name, InputStream input, long length) throws Exception {
        container.getBlockBlobReference(name).upload(input, length);
    }

    @Override
    public String generateSharedAccessSignature(Instant expiry) throws Exception {
        SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy();
        policy.setSharedAccessExpiryTime(java.util.Date.from(expiry));
        policy.setPermissions(EnumSet.of(SharedAccessBlobPermissions.READ, SharedAccessBlobPermissions.LIST));
        return container.generateSharedAccessSignature(policy, null);
    }

    @Override
    public void close() throws Exception {
        provider.close();
    }
}
