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

package org.apache.jackrabbit.oak.segment.azure.fixture;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
import com.google.common.io.Files;
import org.apache.jackrabbit.oak.fixture.NodeStoreFixture;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.azure.AzurePersistence;
import org.apache.jackrabbit.oak.segment.azure.compat.CloudBlobDirectory;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class SegmentAzureFixture extends NodeStoreFixture {

    private static final String AZURE_CONNECTION_STRING = System.getProperty("oak.segment.azure.connection", "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;");

    private static final String AZURE_CONTAINER = System.getProperty("oak.segment.azure.container", "oak");

    private static final String AZURE_ROOT_PATH = System.getProperty("oak.segment.azure.rootPath", "/oak");

    private Map<NodeStore, FileStore> fileStoreMap = new HashMap<>();

    private Map<NodeStore, BlobContainerClient> containerMap = new HashMap<>();

    @Override
    public NodeStore createNodeStore() {
        try {
            AzurePersistence persistence;
            BlobContainerClient container;
            BlobServiceClient blobClient = new BlobServiceClientBuilder().connectionString(AZURE_CONNECTION_STRING).buildClient();
            while (true) {
                String containerName = AZURE_CONTAINER + "-" + UUID.randomUUID().toString();
                container = blobClient.getBlobContainerClient(containerName);
                if (!container.exists()) {
                    container.create();
                    break;
                }
            }
            CloudBlobDirectory directory = new CloudBlobDirectory(container, AZURE_ROOT_PATH);
            persistence = new AzurePersistence(directory);


            FileStore fileStore = FileStoreBuilder.fileStoreBuilder(Files.createTempDir()).withCustomPersistence(persistence).build();
            NodeStore nodeStore = SegmentNodeStoreBuilders.builder(fileStore).build();
            fileStoreMap.put(nodeStore, fileStore);
            containerMap.put(nodeStore, container);
            return nodeStore;
        } catch (IOException | InvalidFileStoreVersionException e) {
            throw new RuntimeException(e);
        }
    }

    public void dispose(NodeStore nodeStore) {
        FileStore fs = fileStoreMap.remove(nodeStore);
        if (fs != null) {
            fs.close();
        }
        try {
            BlobContainerClient container = containerMap.remove(nodeStore);
            if (container != null && container.exists()) {
                container.delete();
            }
        } catch (BlobStorageException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "SegmentAzure";
    }
}
