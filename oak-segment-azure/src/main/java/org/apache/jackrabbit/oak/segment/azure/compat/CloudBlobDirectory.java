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

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import org.apache.jackrabbit.oak.segment.azure.AzureStorageMonitorPolicy;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitor;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.time.Duration;

/**
 * Represents a virtual directory of blobs, designated by a delimiter character.
 * <p>
 * TODO OAK-8413: verify
 * <p>
 * Implements the same interface as the azure-storage-java v8 (https://azure.github.io/azure-storage-java/com/microsoft/azure/storage/blob/CloudBlobDirectory.html)
 */
public class CloudBlobDirectory {
    private static Logger LOG = LoggerFactory.getLogger(CloudBlobDirectory.class);

    private final BlobContainerClient containerClient;
    private final String containerName;
    private final String directory;

    private AzureStorageMonitorPolicy storageMonitorPolicy = null;

    public CloudBlobDirectory(@NotNull final BlobContainerClient containerClient,
                              @NotNull final String containerName,
                              @NotNull final String directory) {
        this.containerClient = containerClient;
        this.containerName = containerName;
        this.directory = directory;
    }

    public BlobContainerClient client() {
        return containerClient;
    }

    public String directory() {
        return directory;
    }

    public PagedIterable<BlobItem> listBlobsFlat() {
        return listBlobsFlat(new ListBlobsOptions().setPrefix(directory), null);
    }

    public PagedIterable<BlobItem> listBlobsFlat(ListBlobsOptions options, Duration timeout) {
        String prefix = Paths.get(directory, options.getPrefix()).toString();
        return containerClient.listBlobsByHierarchy("/",
                new ListBlobsOptions().setPrefix(prefix), timeout);
    }

    /**
     * @param filename filename without the directory prefix
     */
    public BlobClient getBlobClient(@NotNull final String filename) {
        return containerClient.getBlobClient(Paths.get(directory, filename).toString());
    }

    /**
     * @param blobItem a reference to a blob (contains the directory prefix)
     */
    public BlobClient getBlobClientAbsolute(@NotNull BlobItem blobItem) {
        return containerClient.getBlobClient(blobItem.getName());
    }

    public CloudBlobDirectory getDirectoryReference(@NotNull final String dirName) {
        return new CloudBlobDirectory(containerClient, containerName, Paths.get(directory, dirName).toString());
    }

    public void deleteBlobIfExists(BlobClient blob) {
        if (blob.exists()) blob.delete();
    }

    public URI getUri() {
        try {
            URL containerUrl = new URL(containerClient.getBlobContainerUrl());
            String path = Paths.get(containerUrl.getPath(), directory).toString();
            return new URI(containerUrl.getProtocol(),
                    containerUrl.getUserInfo(),
                    containerUrl.getHost(),
                    containerUrl.getPort(),
                    path,
                    containerUrl.getQuery(),
                    null);
        } catch (URISyntaxException | MalformedURLException e) {
            LOG.warn("Unable to format directory URI", e);
            return null;
        }
    }

    public String getContainerName() {
        return containerName;
    }

    // TODO OAK-8413: change missleading name
    public String getPrefix() {
        return directory;
    }

    public void setMonitorPolicy(@NotNull final AzureStorageMonitorPolicy monitorPolicy) {
        this.storageMonitorPolicy = monitorPolicy;
    }

    public void setRemoteStoreMonitor(@NotNull final RemoteStoreMonitor remoteStoreMonitor) {
        if (null != storageMonitorPolicy) {
            storageMonitorPolicy.setMonitor(remoteStoreMonitor);
        }
    }
}
