/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.upgrade.cli.container;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzuriteDockerRule;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.azure.v8.AzurePersistenceV8;
import org.apache.jackrabbit.oak.segment.azure.v8.AzureUtilitiesV8;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import org.apache.jackrabbit.guava.common.io.Files;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

public class SegmentAzureNodeStoreContainer implements NodeStoreContainer {
    private static final String AZURE_ACCOUNT_NAME = "devstoreaccount1";
    private static final String AZURE_ACCOUNT_KEY = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

    private final String dir;

    private final BlobStoreContainer blob;

    private final CloudBlobContainer container;

    private final int mappedPort;

    private File tmpDir;

    private FileStore fs;

    public SegmentAzureNodeStoreContainer(AzuriteDockerRule azurite) throws IOException {
        this(azurite, null, null);
    }

    public SegmentAzureNodeStoreContainer(AzuriteDockerRule azurite, String dir) throws IOException {
        this(azurite, null, dir);
    }

    public SegmentAzureNodeStoreContainer(AzuriteDockerRule azurite, BlobStoreContainer blob) throws IOException {
        this(azurite, blob, null);
    }

    private SegmentAzureNodeStoreContainer(AzuriteDockerRule azurite, BlobStoreContainer blob, String dir)
            throws IOException {
        this.blob = blob;
        this.dir = dir == null ? "repository" : dir;
        try {
            this.container = azurite.getContainer("oak-test");
            this.mappedPort = azurite.getMappedPort();
        } catch (InvalidKeyException | URISyntaxException | StorageException e) {
            throw new IOException(e);
        }
    }

    @Override
    public NodeStore open() throws IOException {
        AzurePersistenceV8 azPersistence = null;
        try {
            azPersistence = new AzurePersistenceV8(container.getDirectoryReference(dir));
        } catch (URISyntaxException e) {
            throw new IllegalStateException(e);
        }

        tmpDir = Files.createTempDir();
        FileStoreBuilder builder = FileStoreBuilder.fileStoreBuilder(tmpDir)
                .withCustomPersistence(azPersistence).withMemoryMapping(false);

        if (blob != null) {
            builder.withBlobStore(blob.open());
        }

        try {
            fs = builder.build();
        } catch (InvalidFileStoreVersionException e) {
            throw new IllegalStateException(e);
        }
        return SegmentNodeStoreBuilders.builder(fs).build();
    }

    @Override
    public void close() {
        if (tmpDir != null) {
            tmpDir.delete();
        }
        if (fs != null) {
            fs.close();
            fs = null;
        }
    }

    @Override
    public void clean() throws IOException {
        try {
            AzureUtilitiesV8.deleteAllEntries(container.getDirectoryReference(dir));
        } catch (URISyntaxException e) {
            throw new IOException(e);
        }
    }

    @Override
    public String getDescription() {
        StringBuilder description = new StringBuilder("az:");
        description.append("DefaultEndpointsProtocol=https;");
        description.append("AccountName=").append(AZURE_ACCOUNT_NAME).append(';');
        description.append("AccountKey=").append(AZURE_ACCOUNT_KEY).append(';');
        description.append("BlobEndpoint=http://127.0.0.1:").append(mappedPort).append("/devstoreaccount1;");
        description.append("ContainerName=").append(container.getName()).append(";");
        description.append("Directory=").append(dir);

        return description.toString();
    }

}
