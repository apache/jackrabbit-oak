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
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.remote.AbstractRemoteSegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.readBufferFully;

public class AzureSegmentArchiveReader extends AbstractRemoteSegmentArchiveReader {

    private final BlobContainerClient blobContainerClient;

    private final String archivePathPrefix;

    AzureSegmentArchiveReader(BlobContainerClient blobContainerClient, String rootPrefix, String archiveName, IOMonitor ioMonitor) {
        this(blobContainerClient, null, rootPrefix, archiveName, ioMonitor);
    }

    AzureSegmentArchiveReader(BlobContainerClient blobContainerClient, @Nullable BlobContainerClient secondaryBlobContainerClient,
                              String rootPrefix, String archiveName, IOMonitor ioMonitor) {
        super(ioMonitor, AzureUtilities.ensureNoTrailingSlash(archiveName),
                createEntryIterable(blobContainerClient, secondaryBlobContainerClient, AzureUtilities.asAzurePrefix(rootPrefix, archiveName)));
        this.blobContainerClient = blobContainerClient;
        this.archivePathPrefix = AzureUtilities.asAzurePrefix(rootPrefix, archiveName);
    }

    private static Iterable<ArchiveEntry> createEntryIterable(BlobContainerClient blobContainerClient,
                                                               @Nullable BlobContainerClient secondaryBlobContainerClient,
                                                               @NotNull String archivePathPrefix) {
        ListBlobsOptions listBlobsOptions = new ListBlobsOptions();
        listBlobsOptions.setPrefix(archivePathPrefix);

        // merge blobs from primary and secondary, primary wins on duplicates
        Map<String, BlobItem> mergedBlobs = new LinkedHashMap<>();
        if (secondaryBlobContainerClient != null) {
            for (BlobItem item : AzureUtilities.getBlobs(secondaryBlobContainerClient, listBlobsOptions)) {
                mergedBlobs.put(item.getName(), item);
            }
        }
        for (BlobItem item : AzureUtilities.getBlobs(blobContainerClient, listBlobsOptions)) {
            mergedBlobs.put(item.getName(), item);
        }

        return mergedBlobs.values().stream()
                .map(blobItem -> {
                    Map<String, String> metadata = blobItem.getMetadata();
                    int length = blobItem.getProperties().getContentLength().intValue();
                    if (AzureBlobMetadata.isSegment(metadata)) {
                        return new ArchiveEntry(AzureBlobMetadata.toIndexEntry(metadata, length));
                    } else {
                        return new ArchiveEntry(length);
                    }
                })
                ::iterator;
    }

    @Override
    protected void doReadSegmentToBuffer(String segmentFileName, Buffer buffer) throws IOException {
        readBufferFully(getBlobClient(segmentFileName), buffer);
    }

    @Override
    protected Buffer doReadDataFile(String extension) throws IOException {
        return readBlob(getName() + extension);
    }

    @Override
    protected File archivePathAsFile() {
        return new File(archivePathPrefix);
    }

    private BlockBlobClient getBlobClient(String name) throws IOException {
        try {
            String fullName = archivePathPrefix + name;
            return blobContainerClient.getBlobClient(fullName).getBlockBlobClient();
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    private Buffer readBlob(String name) throws IOException {
        try {
            BlockBlobClient blob = getBlobClient(name);
            if (!blob.exists()) {
                return null;
            }
            long length = blob.getProperties().getBlobSize();
            Buffer buffer = Buffer.allocate((int) length);
            AzureUtilities.readBufferFully(blob, buffer);
            return buffer;
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }
}
