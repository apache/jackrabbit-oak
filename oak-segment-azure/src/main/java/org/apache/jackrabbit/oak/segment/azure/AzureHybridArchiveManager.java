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
package org.apache.jackrabbit.oak.segment.azure;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import org.apache.jackrabbit.oak.segment.azure.tar.AzureTarArchiveManager;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

public class AzureHybridArchiveManager implements SegmentArchiveManager {

    private final AzurePersistence.WriteMode writeMode;

    private final AzureArchiveManager folderArchiveManager;

    private final AzureTarArchiveManager tarArchiveManager;

    protected final BlobContainerClient readClient;

    protected final String rootPrefix;

    private final Map<String, SegmentArchiveManager> archiveToManagerCache = new ConcurrentSkipListMap<>();

    public AzureHybridArchiveManager(AzurePersistence.WriteMode writeMode, BlobContainerClient readClient, BlobContainerClient writeClient, String rootPrefix, IOMonitor ioMonitor, FileStoreMonitor fileStoreMonitor, WriteAccessController writeAccessController) {
        this.writeMode = writeMode;
        this.folderArchiveManager = new AzureArchiveManager(readClient, writeClient, rootPrefix, ioMonitor, fileStoreMonitor, writeAccessController);
        this.tarArchiveManager = new AzureTarArchiveManager(readClient, writeClient, rootPrefix, ioMonitor, fileStoreMonitor, writeAccessController);
        this.readClient = readClient;
        this.rootPrefix = AzureUtilities.asAzurePrefix(rootPrefix);
    }

    @Override
    public @NotNull List<String> listArchives() throws IOException {
        try {
            archiveToManagerCache.clear();
            List<BlobItem> blobItems = readClient.listBlobsByHierarchy(rootPrefix).stream().toList();
            folderArchiveManager.listArchives(blobItems).forEach(name -> archiveToManagerCache.put(name, folderArchiveManager));
            AzureTarArchiveManager.listArchives(blobItems).forEach(name -> archiveToManagerCache.put(name, tarArchiveManager));
            return new ArrayList<>(archiveToManagerCache.keySet());
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    @Override
    public SegmentArchiveReader open(@NotNull String archiveName) throws IOException {
        return getDelegate(archiveName).open(archiveName);
    }

    @Override
    public SegmentArchiveReader forceOpen(String archiveName) throws IOException {
        return getDelegate(archiveName).forceOpen(archiveName);
    }

    @Override
    public @NotNull SegmentArchiveWriter create(@NotNull String archiveName) throws IOException {
        switch (writeMode) {
            case TAR_FILE:
                archiveToManagerCache.put(archiveName, tarArchiveManager);
                return tarArchiveManager.create(archiveName);
            case FOLDER:
            default:
                archiveToManagerCache.put(archiveName, folderArchiveManager);
                return folderArchiveManager.create(archiveName);
        }
    }

    @Override
    public boolean delete(@NotNull String archiveName) {
        SegmentArchiveManager delegate = getDelegate(archiveName);
        archiveToManagerCache.remove(archiveName, delegate);
        return delegate.delete(archiveName);
    }

    @Override
    public boolean renameTo(@NotNull String from, @NotNull String to) {
        SegmentArchiveManager delegate = getDelegate(from);
        archiveToManagerCache.remove(from, delegate);
        archiveToManagerCache.put(to, delegate);
        return delegate.renameTo(from, to);
    }

    @Override
    public void copyFile(@NotNull String from, @NotNull String to) throws IOException {
        SegmentArchiveManager delegate = getDelegate(from);
        archiveToManagerCache.put(to, delegate);
        delegate.copyFile(from, to);
    }

    @Override
    public boolean exists(@NotNull String archiveName) {
        return getDelegate(archiveName).exists(archiveName);
    }

    @Override
    public void recoverEntries(@NotNull String archiveName, @NotNull LinkedHashMap<UUID, byte[]> entries) throws IOException {
        getDelegate(archiveName).recoverEntries(archiveName, entries);
    }

    @Override
    public void backup(@NotNull String archiveName, @NotNull String backupArchiveName, @NotNull Set<UUID> recoveredEntries) throws IOException {
            getDelegate(archiveName).backup(archiveName, backupArchiveName, recoveredEntries);
    }

    private SegmentArchiveManager getDelegate(String archiveName) {
        SegmentArchiveManager delegate = archiveToManagerCache.computeIfAbsent(archiveName, name ->
                readClient.listBlobsByHierarchy(rootPrefix + name).stream()
                        .filter(blobItem -> Objects.equals(AzureUtilities.getName(blobItem), name))
                        .findFirst()
                        .map(BlobItem::isPrefix)
                        .map(isFolderArchive -> isFolderArchive ? folderArchiveManager : tarArchiveManager)
                        .orElse(null));
        // apply default _after_ the map lookup, in order to avoid caching non-existing archives
        return Objects.requireNonNullElse(delegate, writeMode == AzurePersistence.WriteMode.FOLDER ? folderArchiveManager : tarArchiveManager);
    }
}
