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

import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageCredentialsSharedAccessSignature;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.guava.common.io.Closer;
import org.apache.jackrabbit.guava.common.io.Files;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.azure.v8.AzurePersistenceV8;
import org.apache.jackrabbit.oak.segment.azure.v8.AzureStorageCredentialManagerV8;
import org.apache.jackrabbit.oak.segment.azure.v8.AzureUtilitiesV8;
import org.apache.jackrabbit.oak.segment.azure.util.Environment;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.upgrade.cli.CliUtils;
import org.apache.jackrabbit.oak.upgrade.cli.node.FileStoreUtils.NodeStoreWithFileStore;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import static org.apache.jackrabbit.oak.segment.SegmentCache.DEFAULT_SEGMENT_CACHE_MB;
import static org.apache.jackrabbit.oak.upgrade.cli.node.FileStoreUtils.asCloseable;

public class SegmentAzureFactory implements NodeStoreFactory {
    private final String accountName;
    private final String sasToken;
    private final String uri;
    private final String connectionString;
    private final String containerName;
    private final String dir;

    private int segmentCacheSize;
    private final boolean readOnly;
    private static final Environment environment = new Environment();
    private AzureStorageCredentialManagerV8 azureStorageCredentialManagerV8;

    public static class Builder {
        private final String dir;
        private final int segmentCacheSize;
        private final boolean readOnly;

        private String accountName;
        private String sasToken;
        private String uri;
        private String connectionString;
        private String containerName;

        public Builder(String dir, int segmentCacheSize, boolean readOnly) {
            this.dir = dir;
            this.segmentCacheSize = segmentCacheSize;
            this.readOnly = readOnly;
        }

        public Builder accountName(String accountName) {
            this.accountName = accountName;
            return this;
        }

        public Builder sasToken(String sasToken) {
            this.sasToken = sasToken;
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
        this.sasToken = builder.sasToken;
        this.uri = builder.uri;
        this.connectionString = builder.connectionString;
        this.containerName = builder.containerName;
        this.dir = builder.dir;
        this.segmentCacheSize = builder.segmentCacheSize;
        this.readOnly = builder.readOnly;
    }

    @Override
    public NodeStore create(BlobStore blobStore, Closer closer) throws IOException {
        AzurePersistenceV8 azPersistence = null;
        try {
            azPersistence = createAzurePersistence(closer);
        } catch (StorageException | URISyntaxException | InvalidKeyException e) {
            throw new IllegalStateException(e);
        }

        File tmpDir = Files.createTempDir();
        closer.register(() -> tmpDir.delete());
        FileStoreBuilder builder = FileStoreBuilder.fileStoreBuilder(tmpDir)
                .withCustomPersistence(azPersistence).withMemoryMapping(false);

        if (blobStore != null) {
            builder.withBlobStore(blobStore);
        }

        try {
            if (readOnly) {
                final ReadOnlyFileStore fs;
                builder.withSegmentCacheSize(segmentCacheSize > 0 ? segmentCacheSize : DEFAULT_SEGMENT_CACHE_MB);
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

    private AzurePersistenceV8 createAzurePersistence(Closer closer) throws StorageException, URISyntaxException, InvalidKeyException {
        CloudBlobDirectory cloudBlobDirectory = null;

        // connection string will take precedence over accountkey / sas / service principal
        if (StringUtils.isNoneBlank(connectionString, containerName)) {
            cloudBlobDirectory = AzureUtilitiesV8.cloudBlobDirectoryFrom(connectionString, containerName, dir);
        } else if (StringUtils.isNoneBlank(accountName, uri)) {
            StorageCredentials credentials = null;
            if (StringUtils.isNotBlank(sasToken)) {
                credentials = new StorageCredentialsSharedAccessSignature(sasToken);
            } else {
                this.azureStorageCredentialManagerV8 = new AzureStorageCredentialManagerV8();
                credentials = azureStorageCredentialManagerV8.getStorageCredentialsFromEnvironment(accountName, environment);
                closer.register(azureStorageCredentialManagerV8);
            }
            cloudBlobDirectory = AzureUtilitiesV8.cloudBlobDirectoryFrom(credentials, uri, dir);
        }

        if (cloudBlobDirectory == null) {
            throw new IllegalArgumentException("Could not connect to Azure storage. Too few connection parameters specified!");
        }

        return new AzurePersistenceV8(cloudBlobDirectory);
    }

    @Override
    public boolean hasExternalBlobReferences() throws IOException {
        AzurePersistenceV8 azPersistence = null;
        Closer closer = Closer.create();
        CliUtils.handleSigInt(closer);
        try {
            azPersistence = createAzurePersistence(closer);
        } catch (StorageException | URISyntaxException | InvalidKeyException e) {
            closer.close();
            throw new IllegalStateException(e);
        }

        File tmpDir = Files.createTempDir();
        FileStoreBuilder builder = FileStoreBuilder.fileStoreBuilder(tmpDir)
                .withCustomPersistence(azPersistence).withMemoryMapping(false);

        ReadOnlyFileStore fs;
        try {
            fs = builder.buildReadOnly();
            return FileStoreUtils.hasExternalBlobReferences(fs);
        } catch (InvalidFileStoreVersionException e) {
            throw new IOException(e);
        } finally {
            tmpDir.delete();
            closer.close();
        }
    }

    @Override
    public String toString() {
        return String.format("AzureSegmentNodeStore[%s]", dir);
    }
}
