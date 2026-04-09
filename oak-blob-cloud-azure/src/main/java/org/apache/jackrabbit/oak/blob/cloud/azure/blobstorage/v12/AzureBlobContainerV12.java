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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v12;

import java.io.InputStream;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Objects;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.sas.BlobContainerSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureBlobContainer;

public class AzureBlobContainerV12 implements AzureBlobContainer {
    private final BlobContainerClient container;

    public AzureBlobContainerV12(BlobContainerClient container) {
        this.container = Objects.requireNonNull(container);
    }

    @Override
    public void createIfNotExists() {
        container.createIfNotExists();
    }

    @Override
    public void delete() {
        container.delete();
    }

    @Override
    public boolean deleteIfExists() {
        return container.deleteIfExists();
    }

    @Override
    public boolean exists() {
        return container.exists();
    }

    @Override
    public String getName() {
        return container.getBlobContainerName();
    }

    @Override
    public String getContainerUri() {
        return container.getBlobContainerUrl();
    }

    @Override
    public void uploadBlockBlob(String name, InputStream input, long length) {
        container.getBlobClient(name).upload(input, length, true);
    }

    @Override
    public String generateSharedAccessSignature(Instant expiry) {
        BlobContainerSasPermission permissions = new BlobContainerSasPermission().setReadPermission(true).setListPermission(true);
        BlobServiceSasSignatureValues values = new BlobServiceSasSignatureValues(OffsetDateTime.ofInstant(expiry, ZoneOffset.UTC), permissions);
        return container.generateSas(values);
    }
}
