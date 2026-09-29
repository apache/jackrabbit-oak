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
package org.apache.jackrabbit.oak.segment.azure.tar;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import org.apache.jackrabbit.oak.segment.azure.AzureUtilities;
import org.apache.jackrabbit.oak.segment.azure.tar.io.AzureIOReadAccess;
import org.apache.jackrabbit.oak.segment.azure.tar.io.AzureIOWriteAccess;
import org.apache.jackrabbit.oak.segment.file.tar.IOReadAccess;
import org.apache.jackrabbit.oak.segment.file.tar.SegmentTarReader;
import org.apache.jackrabbit.oak.segment.file.tar.SegmentTarRecovery;
import org.apache.jackrabbit.oak.segment.file.tar.SegmentTarWriter;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * SegmentArchiveManager implementation for Azure Blob Storage using TAR files.
 * <p>
 * It reuses the {@link SegmentTarReader} and {@link SegmentTarWriter} implementations
 * for reading and writing TAR files.
 */
public class AzureTarArchiveManager implements SegmentArchiveManager {

    private final BlobContainerClient readBlobContainerClient;

    private final BlobContainerClient writeBlobContainerClient;

    private final String rootPrefix;

    private final IOMonitor ioMonitor;

    private final FileStoreMonitor fileStoreMonitor;

    private final WriteAccessController writeAccessController;

    public AzureTarArchiveManager(BlobContainerClient readBlobContainerClient, BlobContainerClient writeBlobContainerClient, String rootPrefix, IOMonitor ioMonitor, FileStoreMonitor fileStoreMonitor, WriteAccessController writeAccessController) {
        this.readBlobContainerClient = readBlobContainerClient;
        this.writeBlobContainerClient = writeBlobContainerClient;
        this.rootPrefix = AzureUtilities.asAzurePrefix(rootPrefix);
        this.ioMonitor = ioMonitor;
        this.fileStoreMonitor = fileStoreMonitor;
        this.writeAccessController = writeAccessController;
    }


    @Override
    public @NotNull List<String> listArchives() throws IOException {
        try {
            return listArchives(readBlobContainerClient.listBlobsByHierarchy(rootPrefix).stream().toList());
        } catch (BlobStorageException e) {
            throw new IOException("Error listing blobs in container " + readBlobContainerClient.getBlobContainerName() + " with prefix " + rootPrefix, e);
        }
    }

    public static @NotNull List<String> listArchives(@NotNull List<BlobItem> blobItems) throws IOException {
        return blobItems.stream()
                .filter(Predicate.not(BlobItem::isPrefix))
                .filter(b -> b.getProperties().getContentLength() > 0)
                .map(AzureUtilities::getName)
                .filter(blobName -> blobName.endsWith(".tar"))
                .collect(Collectors.toList());
    }

    @Override
    public @Nullable SegmentArchiveReader open(@NotNull String archiveName) throws IOException {
        BlobClient tarArchive = readBlobContainerClient.getBlobClient(rootPrefix + archiveName);
        return SegmentTarReader.create(new AzureIOReadAccess(archiveName, tarArchive), ioMonitor);
    }

    @Override
    public @Nullable SegmentArchiveReader forceOpen(String archiveName) throws IOException {
        return open(archiveName);
    }

    @Override
    public @NotNull SegmentArchiveWriter create(@NotNull String archiveName) throws IOException {
        WriteAccessController accessController = writeAccessController;
        // allow writing to read-only backup files to enable read-only recovery
        if (archiveName.endsWith(".ro.bak")) {
            accessController = new WriteAccessController();
            accessController.enableWriting();
        }
        return new SegmentTarWriter(
                new AzureIOWriteAccess(archiveName, getArchiveClient(archiveName), accessController),
                fileStoreMonitor,
                ioMonitor
        );
    }

    @Override
    public boolean delete(@NotNull String archiveName) {
        writeAccessController.checkWritingAllowed();
        return getArchiveClient(archiveName).deleteIfExists();
    }

    @Override
    public boolean renameTo(@NotNull String from, @NotNull String to) {
        // rename is not supported
        return false;
    }

    @Override
    public void copyFile(@NotNull String from, @NotNull String to) throws IOException {
        writeAccessController.checkWritingAllowed();
        BlobClient source = getArchiveClient(from);
        BlobClient target = getArchiveClient(to);
        target.beginCopy(source.getBlobUrl(), Duration.ofMillis(250))
                .waitForCompletion();
    }

    @Override
    public boolean exists(@NotNull String archiveName) {
        return getArchiveClient(archiveName).exists();
    }

    @Override
    public void recoverEntries(@NotNull String archiveName, @NotNull LinkedHashMap<UUID, byte[]> entries) throws IOException {
        try (IOReadAccess access = new AzureIOReadAccess(archiveName, getArchiveClient(archiveName))) {
            SegmentTarRecovery.recoverEntries(access, entries);
        }
    }

    @Override
    public void backup(@NotNull String archiveName, @NotNull String backupArchiveName, @NotNull Set<UUID> recoveredEntries) throws IOException {
        copyFile(archiveName, backupArchiveName);
        if (!delete(archiveName)) {
            throw new IOException(
                    "Could not remove broken tar file " + archiveName);
        }
    }

    private @NotNull BlobClient getArchiveClient(@NotNull String archiveName) {
        return writeBlobContainerClient.getBlobClient(rootPrefix + archiveName);
    }
}
