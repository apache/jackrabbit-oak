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

import com.azure.core.util.BinaryData;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.apache.jackrabbit.guava.common.base.Stopwatch;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.azure.util.Retrier;
import org.apache.jackrabbit.oak.segment.remote.AbstractRemoteSegmentArchiveWriter;
import org.apache.jackrabbit.oak.segment.remote.RemoteSegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.readBufferFully;
import static org.apache.jackrabbit.oak.segment.remote.RemoteUtilities.OFF_HEAP;
import static org.apache.jackrabbit.oak.segment.remote.RemoteUtilities.getSegmentFileName;

public class AzureSegmentArchiveWriter extends AbstractRemoteSegmentArchiveWriter {

    private final BlobContainerClient blobContainerClient;

    private final String rootPrefix;

    private final String archiveName;

    private final Retrier retrier = Retrier.withParams(
            Integer.getInteger("azure.segment.archive.writer.retries.max", 16),
            Integer.getInteger("azure.segment.archive.writer.retries.intervalMs", 5000)
    );

    public AzureSegmentArchiveWriter(BlobContainerClient blobContainerClient, String rootPrefix, String archiveName, IOMonitor ioMonitor, FileStoreMonitor monitor, WriteAccessController writeAccessController) {
        super(ioMonitor, monitor);
        this.blobContainerClient = blobContainerClient;
        this.rootPrefix = rootPrefix;
        this.archiveName = archiveName;
        this.writeAccessController = writeAccessController;
    }

    @Override
    public String getName() {
        return archiveName;
    }

    @Override
    protected void doWriteArchiveEntry(RemoteSegmentArchiveEntry indexEntry, byte[] data, int offset, int size) throws IOException {

        writeAccessController.checkWritingAllowed();

        long msb = indexEntry.getMsb();
        long lsb = indexEntry.getLsb();
        String segmentName = getSegmentFileName(indexEntry);
        BlockBlobClient blob = getBlockBlobClient(segmentName);
        ioMonitor.beforeSegmentWrite(new File(blob.getBlobName()), msb, lsb, size);
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            blob.upload(BinaryData.fromBytes(Arrays.copyOfRange(data, offset, offset + size)), true);
            blob.setMetadata(AzureBlobMetadata.toSegmentMetadata(indexEntry));
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
        ioMonitor.afterSegmentWrite(new File(blob.getBlobName()), msb, lsb, size, stopwatch.elapsed(TimeUnit.NANOSECONDS));
    }

    @Override
    protected Buffer doReadArchiveEntry(RemoteSegmentArchiveEntry indexEntry)  throws IOException {
        Buffer buffer;
        if (OFF_HEAP) {
            buffer = Buffer.allocateDirect(indexEntry.getLength());
        } else {
            buffer = Buffer.allocate(indexEntry.getLength());
        }
        readBufferFully(getBlockBlobClient(getSegmentFileName(indexEntry)), buffer);
        return buffer;
    }

    @Override
    protected void doWriteDataFile(byte[] data, String extension) throws IOException {
        retrier.execute(() -> {
            try {
                writeAccessController.checkWritingAllowed();

                getBlockBlobClient(getName() + extension).upload(BinaryData.fromBytes(data), true);
            } catch (BlobStorageException e) {
                throw new IOException(e);
            }
        });
    }

    @Override
    protected void afterQueueClosed() throws IOException {
        retrier.execute(() -> {
            try {
                writeAccessController.checkWritingAllowed();

                getBlockBlobClient("closed").upload(BinaryData.fromBytes(new byte[0]), true);
            } catch (BlobStorageException e) {
                throw new IOException(e);
            }
        });
    }

    @Override
    protected void afterQueueFlushed() {
        // do nothing
    }

    private BlockBlobClient getBlockBlobClient(String name) throws IOException {
        String blobFullName = String.format("%s/%s/%s", rootPrefix, archiveName, name);
        try {
            return blobContainerClient.getBlobClient(blobFullName).getBlockBlobClient();
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }
}
