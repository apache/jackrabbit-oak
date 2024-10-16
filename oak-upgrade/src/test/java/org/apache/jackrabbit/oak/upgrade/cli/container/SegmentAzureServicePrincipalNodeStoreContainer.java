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

import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import org.apache.jackrabbit.guava.common.io.Files;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.azure.v8.AzurePersistenceV8;
import org.apache.jackrabbit.oak.segment.azure.v8.AzureStorageCredentialManagerV8;
import org.apache.jackrabbit.oak.segment.azure.v8.AzureUtilitiesV8;
import org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils;
import org.apache.jackrabbit.oak.segment.azure.util.Environment;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.cli.node.FileStoreUtils;

import java.io.File;
import java.io.IOException;

public class SegmentAzureServicePrincipalNodeStoreContainer implements NodeStoreContainer {
    private static final Environment ENVIRONMENT = new Environment();
    private static final String CONTAINER_NAME = "oak-migration-test";
    private static final String DIR = "repository";
    private static final String AZURE_SEGMENT_STORE_PATH = "https://%s.blob.core.windows.net/%s/%s";

    private final BlobStore blobStore;
    private FileStore fs;
    private File tmpDir;
    private AzurePersistenceV8 azurePersistenceV8;
    private final AzureStorageCredentialManagerV8 azureStorageCredentialManagerV8;

    public SegmentAzureServicePrincipalNodeStoreContainer() {
        this(null);
    }

    public SegmentAzureServicePrincipalNodeStoreContainer(BlobStore blobStore) {
        this.blobStore = blobStore;
        this.azureStorageCredentialManagerV8 = new AzureStorageCredentialManagerV8();
    }


    @Override
    public NodeStore open() throws IOException {
        try {
            azurePersistenceV8 = createAzurePersistence();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

        tmpDir = Files.createTempDir();
        FileStoreBuilder builder = FileStoreBuilder.fileStoreBuilder(tmpDir)
                .withCustomPersistence(azurePersistenceV8).withMemoryMapping(false);
        if (blobStore != null) {
            builder.withBlobStore(blobStore);
        }

        try {
            fs = builder.build();
        } catch (InvalidFileStoreVersionException e) {
            throw new IllegalStateException(e);
        }

        return new FileStoreUtils.NodeStoreWithFileStore(SegmentNodeStoreBuilders.builder(fs).build(), fs);
    }

    private AzurePersistenceV8 createAzurePersistence() {
        if (azurePersistenceV8 != null) {
            return azurePersistenceV8;
        }
        String path = String.format(AZURE_SEGMENT_STORE_PATH, ENVIRONMENT.getVariable(AzureUtilitiesV8.AZURE_ACCOUNT_NAME),
                CONTAINER_NAME, DIR);
        CloudBlobDirectory cloudBlobDirectory = ToolUtils.createCloudBlobDirectory(path, ENVIRONMENT, azureStorageCredentialManagerV8);
        return new AzurePersistenceV8(cloudBlobDirectory);
    }

    @Override
    public void close() {
        if (fs != null) {
            fs.close();
            fs = null;
        }
        if (tmpDir != null) {
            tmpDir.delete();
        }
        if (azureStorageCredentialManagerV8 != null) {
            azureStorageCredentialManagerV8.close();
        }
    }

    @Override
    public void clean() throws IOException {
        AzurePersistenceV8 azurePersistenceV8 = createAzurePersistence();
        try {
            AzureUtilitiesV8.deleteAllBlobs(azurePersistenceV8.getSegmentstoreDirectory());
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    public String getDescription() {
        return "az:" + String.format(AZURE_SEGMENT_STORE_PATH, ENVIRONMENT.getVariable(AzureUtilitiesV8.AZURE_ACCOUNT_NAME),
                CONTAINER_NAME, DIR);
    }
}