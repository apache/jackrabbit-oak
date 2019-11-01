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
import com.azure.storage.blob.ContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import org.apache.jackrabbit.oak.segment.azure.AzureStorageMonitorPolicy;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitor;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.time.Duration;

/**
 * Represents a virtual directory of blobs, designated by a delimiter character.
 *
 * TODO OAK-8413: verify
 *
 * Implements the same interface as the azure-storage-java v8 (https://azure.github.io/azure-storage-java/com/microsoft/azure/storage/blob/CloudBlobDirectory.html)
 */
public class CloudBlobDirectory {
    private static Logger LOG = LoggerFactory.getLogger(CloudBlobDirectory.class);

    private final ContainerClient client;
    private final String containerName;
    private final String directory;

    private AzureStorageMonitorPolicy storageMonitorPolicy = null;

    public CloudBlobDirectory(@NotNull final ContainerClient client,
                              @NotNull final String containerName,
                              @NotNull final String directory) {
        this.client = client;
        this.containerName = containerName;
        this.directory = directory;
    }

    public ContainerClient client() { return client; }
    public String directory() { return directory; }

    public PagedIterable<BlobItem> listBlobsFlat() {
        return listBlobsFlat(new ListBlobsOptions().prefix(directory), null);
    }

    public PagedIterable<BlobItem> listBlobsFlat(ListBlobsOptions options, Duration timeout) {
        return client.listBlobsHierarchy("/", new ListBlobsOptions().prefix(Paths.get(directory, options.prefix()).toString()), timeout);
    }

    /**
     * @param filename filename without the directory prefix
     * @return
     */
    public BlobClient getBlobClient(@NotNull final String filename) {
        return client.getBlobClient(Paths.get(directory, filename).toString());
    }

    public CloudBlobDirectory getDirectoryReference(@NotNull final String dirName) {
        return new CloudBlobDirectory(client, containerName, Paths.get(directory, dirName).toString());
    }

    public void deleteBlobIfExists(BlobClient blob) {
        if (blob.exists()) blob.delete();
    }

    public URI getUri() {
        URL containerUrl = client.getContainerUrl();
        String path = Paths.get(containerUrl.getPath(), directory).toString();
        try {
            return new URI(containerUrl.getProtocol(),
                    containerUrl.getUserInfo(),
                    containerUrl.getHost(),
                    containerUrl.getPort(),
                    path,
                    containerUrl.getQuery(),
                    null);
        }
        catch (URISyntaxException e) {
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
