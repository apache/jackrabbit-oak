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
package org.apache.jackrabbit.oak.upgrade.cli.node;

import static org.apache.jackrabbit.oak.upgrade.cli.node.FileStoreUtils.asCloseable;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.azure.AzurePersistence;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.cli.node.FileStoreUtils.NodeStoreWithFileStore;

import com.google.common.io.Closer;
import com.google.common.io.Files;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsAccountAndKey;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.StorageUri;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

public class SegmentAzureFactory implements NodeStoreFactory {
    private final String accountName;
    private final String uri;
    private final String connectionString;
    private final String containerName;
    private final String dir;
    private final boolean readOnly;

    public static class Builder {
        private final String dir;
        private final boolean readOnly;

        private String accountName;
        private String uri;
        private String connectionString;
        private String containerName;

        public Builder(String dir, boolean readOnly) {
            this.dir = dir;
            this.readOnly = readOnly;
        }

        public Builder accountName(String accountName) {
            this.accountName = accountName;
            return this;
        }

        public Builder uri(String uri) {
            this.uri = uri;
            return this;
        }

        public Builder connectionString(String connectionString) {
            this.connectionString = connectionString;
            return this;
        }

        public Builder containerName(String containerName) {
            this.containerName = containerName;
            return this;
        }

        public SegmentAzureFactory build() {
            return new SegmentAzureFactory(this);
        }
    }

    public SegmentAzureFactory(Builder builder) {
        this.accountName = builder.accountName;
        this.uri = builder.uri;
        this.connectionString = builder.connectionString;
        this.containerName = builder.containerName;
        this.dir = builder.dir;
        this.readOnly = builder.readOnly;
    }

    @Override
    public NodeStore create(BlobStore blobStore, Closer closer) throws IOException {
        AzurePersistence azPersistence = null;
        try {
            azPersistence = createAzurePersistence();
        } catch (StorageException | URISyntaxException | InvalidKeyException e) {
            throw new IllegalStateException(e);
        }

        FileStoreBuilder builder = FileStoreBuilder.fileStoreBuilder(Files.createTempDir())
                .withCustomPersistence(azPersistence).withMemoryMapping(false);

        if (blobStore != null) {
            builder.withBlobStore(blobStore);
        }

        try {
            if (readOnly) {
                final ReadOnlyFileStore fs;
                fs = builder.buildReadOnly();
                closer.register(asCloseable(fs));
                return SegmentNodeStoreBuilders.builder(fs).build();
            } else {
                final FileStore fs;
                fs = builder.build();
                closer.register(asCloseable(fs));
                return new NodeStoreWithFileStore(SegmentNodeStoreBuilders.builder(fs).build(), fs);
            }
        } catch (InvalidFileStoreVersionException e) {
            throw new IllegalStateException(e);
        }
    }

    private AzurePersistence createAzurePersistence() throws StorageException, URISyntaxException, InvalidKeyException {
        AzurePersistence azPersistence = null;

        if (accountName != null && uri != null) {
            String key = System.getenv("AZURE_SECRET_KEY");
            StorageCredentials credentials = new StorageCredentialsAccountAndKey(accountName, key);
            StorageUri storageUri = new StorageUri(new URI(uri));
            CloudBlobContainer cloudBlobContainer = new CloudBlobContainer(storageUri, credentials);

            azPersistence = new AzurePersistence(cloudBlobContainer.getDirectoryReference(dir));
        } else if (connectionString != null && containerName != null) {
            CloudStorageAccount cloud = CloudStorageAccount.parse(connectionString.toString());
            CloudBlobContainer container = cloud.createCloudBlobClient().getContainerReference(containerName);
            container.createIfNotExists();

            azPersistence = new AzurePersistence(container.getDirectoryReference(dir));
        }

        if (azPersistence == null) {
            throw new IllegalArgumentException("Could not connect to Azure storage. Too few connection parameters specified!");
        }

        return azPersistence;
    }

    @Override
    public boolean hasExternalBlobReferences() throws IOException {
        AzurePersistence azPersistence = null;
        try {
            azPersistence = createAzurePersistence();
        } catch (StorageException | URISyntaxException | InvalidKeyException e) {
            throw new IllegalStateException(e);
        }

        FileStoreBuilder builder = FileStoreBuilder.fileStoreBuilder(Files.createTempDir())
                .withCustomPersistence(azPersistence).withMemoryMapping(false);

        ReadOnlyFileStore fs;
        try {
            fs = builder.buildReadOnly();
        } catch (InvalidFileStoreVersionException e) {
            throw new IOException(e);
        }

        return FileStoreUtils.hasExternalBlobReferences(fs);
    }

    @Override
    public String toString() {
        return String.format("AzureSegmentNodeStore[%s]", dir);
    }
}
