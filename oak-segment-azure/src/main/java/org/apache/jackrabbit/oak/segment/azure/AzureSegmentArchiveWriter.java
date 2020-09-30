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

import static org.apache.jackrabbit.oak.segment.azure.AzureUtilities.readBufferFully;
import static org.apache.jackrabbit.oak.segment.remote.RemoteUtilities.getSegmentFileName;
import static org.apache.jackrabbit.oak.segment.remote.RemoteUtilities.OFF_HEAP;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.remote.AbstractRemoteSegmentArchiveWriter;
import org.apache.jackrabbit.oak.segment.remote.RemoteSegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitor;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;

public class AzureSegmentArchiveWriter extends AbstractRemoteSegmentArchiveWriter {

    private final CloudBlobDirectory archiveDirectory;

    public AzureSegmentArchiveWriter(CloudBlobDirectory archiveDirectory, IOMonitor ioMonitor, FileStoreMonitor monitor) {
        super(ioMonitor, monitor);
        this.archiveDirectory = archiveDirectory;
    }

    @Override
    public String getName() {
        return AzureUtilities.getName(archiveDirectory);
    }

    @Override
    protected void doWriteArchiveEntry(RemoteSegmentArchiveEntry indexEntry, byte[] data, int offset, int size) throws IOException {
        long msb = indexEntry.getMsb();
        long lsb = indexEntry.getLsb();
        String segmentName = getSegmentFileName(indexEntry);
        CloudBlockBlob blob = getBlob(segmentName);
        ioMonitor.beforeSegmentWrite(new File(blob.getName()), msb, lsb, size);
        Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            blob.setMetadata(AzureBlobMetadata.toSegmentMetadata(indexEntry));
            blob.uploadFromByteArray(data, offset, size);
            blob.uploadMetadata();
        } catch (StorageException e) {
            throw new IOException(e);
        }
        ioMonitor.afterSegmentWrite(new File(blob.getName()), msb, lsb, size, stopwatch.elapsed(TimeUnit.NANOSECONDS));
    }

    @Override
    protected Buffer doReadArchiveEntry(RemoteSegmentArchiveEntry indexEntry)  throws IOException {
        Buffer buffer;
        if (OFF_HEAP) {
            buffer = Buffer.allocateDirect(indexEntry.getLength());
        } else {
            buffer = Buffer.allocate(indexEntry.getLength());
        }
        readBufferFully(getBlob(getSegmentFileName(indexEntry)), buffer);
        return buffer;
    }

    @Override
    protected void doWriteDataFile(byte[] data, String extension) throws IOException {
        try {
            getBlob(getName() + extension).uploadFromByteArray(data, 0, data.length);
        } catch (StorageException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void afterQueueClosed() throws IOException {
        try {
            getBlob("closed").uploadFromByteArray(new byte[0], 0, 0);
        } catch (StorageException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void afterQueueFlushed() {
        // do nothing
    }

    private CloudBlockBlob getBlob(String name) throws IOException {
        try {
            return archiveDirectory.getBlockBlobReference(name);
        } catch (URISyntaxException | StorageException e) {
            throw new IOException(e);
        }
    }
}
