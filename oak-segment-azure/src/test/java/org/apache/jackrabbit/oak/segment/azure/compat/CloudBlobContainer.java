/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
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

package org.apache.jackrabbit.oak.segment.azure.compat;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.jetbrains.annotations.NotNull;

import java.nio.file.Paths;

public class CloudBlobContainer {
    private final BlobContainerClient containerClient;
    private final String containerName;

    private CloudBlobContainer(@NotNull final BlobContainerClient containerClient,
                               @NotNull final String containerName) {
        this.containerClient = containerClient;
        this.containerName = containerName;
    }

    public static CloudBlobContainer withContainerClient(@NotNull final BlobContainerClient containerClient,
                                                         @NotNull final String containerName) {
        return new CloudBlobContainer(containerClient, containerName);
    }

    public String getName() {
        return containerName;
    }

    public boolean exists() {
        return containerClient.exists();
    }

    public void deleteIfExists() {
        if (containerClient.exists()) containerClient.delete();
    }

    public void create() {
        containerClient.create();
    }

    public void createIfNotExists() {
        if (! containerClient.exists()) containerClient.create();
    }

    public CloudBlobDirectory getDirectoryReference(@NotNull final String directoryName) {
        return new CloudBlobDirectory(containerClient, containerName, directoryName);
    }

    public BlobClient getBlobReference(@NotNull final String path) {
        String dirName = Paths.get(path).toFile().getParentFile().toString();
        String blobName = Paths.get(path).getFileName().toString();
        return null == dirName ?
                containerClient.getBlobClient(blobName) :
                getDirectoryReference(dirName).getBlobClient(blobName);
    }

    public BlockBlobClient getBlockBlobReference(@NotNull final String path) {
        return getBlobReference(path).getBlockBlobClient();
    }

    public AppendBlobClient getAppendBlobReference(@NotNull final String path) {
        return getBlobReference(path).getAppendBlobClient();
    }
}
