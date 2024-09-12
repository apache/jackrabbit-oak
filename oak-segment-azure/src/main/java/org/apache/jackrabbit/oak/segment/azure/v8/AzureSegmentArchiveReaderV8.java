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
package org.apache.jackrabbit.oak.segment.azure.v8;

import static org.apache.jackrabbit.oak.segment.azure.v8.AzureUtilitiesV8.readBufferFully;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.UUID;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.azure.AzureBlobMetadata;
import org.apache.jackrabbit.oak.segment.remote.AbstractRemoteSegmentArchiveReader;
import org.apache.jackrabbit.oak.segment.remote.RemoteSegmentArchiveEntry;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitor;

public class AzureSegmentArchiveReaderV8 extends AbstractRemoteSegmentArchiveReader {

    private final CloudBlobDirectory archiveDirectory;

    private final long length;

    protected AzureSegmentArchiveReaderV8(CloudBlobDirectory archiveDirectory, IOMonitor ioMonitor) throws IOException {
        super(ioMonitor);
        this.archiveDirectory = archiveDirectory;
        this.length = computeArchiveIndexAndLength();
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public String getName() {
        return AzureUtilitiesV8.getName(archiveDirectory);
    }

    @Override
    protected long computeArchiveIndexAndLength() throws IOException {
        long length = 0;
        for (CloudBlob blob : AzureUtilitiesV8.getBlobs(archiveDirectory)) {
            Map<String, String> metadata = blob.getMetadata();
            if (AzureBlobMetadata.isSegment(metadata)) {
                RemoteSegmentArchiveEntry indexEntry = AzureBlobMetadata.toIndexEntry(metadata, (int) blob.getProperties().getLength());
                index.put(new UUID(indexEntry.getMsb(), indexEntry.getLsb()), indexEntry);
            }
            length += blob.getProperties().getLength();
        }

        return length;
    }

    @Override
    protected void doReadSegmentToBuffer(String segmentFileName, Buffer buffer) throws IOException {
        readBufferFully(getBlob(segmentFileName), buffer);
    }

    @Override
    protected Buffer doReadDataFile(String extension) throws IOException {
        return readBlob(getName() + extension);
    }

    @Override
    protected File archivePathAsFile() {
        return new File(archiveDirectory.getUri().getPath());
    }

    private CloudBlockBlob getBlob(String name) throws IOException {
        try {
            return archiveDirectory.getBlockBlobReference(name);
        } catch (URISyntaxException | StorageException e) {
            throw new IOException(e);
        }
    }

    private Buffer readBlob(String name) throws IOException {
        try {
            CloudBlockBlob blob = getBlob(name);
            if (!blob.exists()) {
                return null;
            }
            long length = blob.getProperties().getLength();
            Buffer buffer = Buffer.allocate((int) length);
            AzureUtilitiesV8.readBufferFully(blob, buffer);
            return buffer;
        } catch (StorageException e) {
            throw new IOException(e);
        }
    }
}
