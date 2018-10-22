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

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.CloudAppendBlob;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.jackrabbit.oak.segment.spi.persistence.GCJournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.ManifestFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.RepositoryLock;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.EnumSet;

public class AzurePersistence implements SegmentNodeStorePersistence {

    private static final Logger log = LoggerFactory.getLogger(AzurePersistence.class);

    private final CloudBlobDirectory segmentstoreDirectory;

    public AzurePersistence(CloudBlobDirectory segmentstoreDirectory) {
        this.segmentstoreDirectory = segmentstoreDirectory;
    }

    @Override
    public SegmentArchiveManager createArchiveManager(boolean mmap, boolean offHeapAccess, IOMonitor ioMonitor, FileStoreMonitor fileStoreMonitor) {
        return new AzureArchiveManager(segmentstoreDirectory, ioMonitor, fileStoreMonitor);
    }

    @Override
    public boolean segmentFilesExist() {
        try {
            for (ListBlobItem i : segmentstoreDirectory.listBlobs(null, false, EnumSet.noneOf(BlobListingDetails.class), null, null)) {
                if (i instanceof CloudBlobDirectory) {
                    CloudBlobDirectory dir = (CloudBlobDirectory) i;
                    String name = Paths.get(dir.getPrefix()).getFileName().toString();
                    if (name.endsWith(".tar")) {
                        return true;
                    }
                }
            }
            return false;
        } catch (StorageException | URISyntaxException e) {
            log.error("Can't check if the segment archives exists", e);
            return false;
        }
    }

    @Override
    public JournalFile getJournalFile() {
        return new AzureJournalFile(segmentstoreDirectory, "journal.log");
    }

    @Override
    public GCJournalFile getGCJournalFile() throws IOException {
        return new AzureGCJournalFile(getAppendBlob("gc.log"));
    }

    @Override
    public ManifestFile getManifestFile() throws IOException {
        return new AzureManifestFile(getBlockBlob("manifest"));
    }

    @Override
    public RepositoryLock lockRepository() throws IOException {
        return new AzureRepositoryLock(getBlockBlob("repo.lock"), () -> {
            log.warn("Lost connection to the Azure. The client will be closed.");
            // TODO close the connection
        }).lock();
    }

    private CloudBlockBlob getBlockBlob(String path) throws IOException {
        try {
            return segmentstoreDirectory.getBlockBlobReference(path);
        } catch (URISyntaxException | StorageException e) {
            throw new IOException(e);
        }
    }

    private CloudAppendBlob getAppendBlob(String path) throws IOException {
        try {
            return segmentstoreDirectory.getAppendBlobReference(path);
        } catch (URISyntaxException | StorageException e) {
            throw new IOException(e);
        }
    }

}
