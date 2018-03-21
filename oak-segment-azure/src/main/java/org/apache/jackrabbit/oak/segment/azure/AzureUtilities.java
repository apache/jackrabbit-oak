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
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class AzureUtilities {

    public static String SEGMENT_FILE_NAME_PATTERN = "^([0-9a-f]{4})\\.([0-9a-f-]+)$";

    private AzureUtilities() {
    }

    public static String getSegmentFileName(AzureSegmentArchiveEntry indexEntry) {
        return getSegmentFileName(indexEntry.getPosition(), indexEntry.getMsb(), indexEntry.getLsb());
    }

    public static String getSegmentFileName(long offset, long msb, long lsb) {
        return String.format("%04x.%s", offset, new UUID(msb, lsb).toString());
    }

    public static String getName(CloudBlob blob) {
        return Paths.get(blob.getName()).getFileName().toString();
    }

    public static String getName(CloudBlobDirectory directory) {
        return Paths.get(directory.getUri().getPath()).getFileName().toString();
    }

    public static Stream<CloudBlob> getBlobs(CloudBlobDirectory directory) throws IOException {
        try {
            return StreamSupport.stream(directory.listBlobs().spliterator(), false)
                    .filter(i -> i instanceof CloudBlob)
                    .map(i -> (CloudBlob) i);
        } catch (StorageException | URISyntaxException e) {
            throw new IOException(e);
        }
    }

    public static long getDirectorySize(CloudBlobDirectory directory) throws IOException {
        long size = 0;
        for (CloudBlob b : getBlobs(directory).collect(Collectors.toList())) {
            try {
                b.downloadAttributes();
            } catch (StorageException e) {
                throw new IOException(e);
            }
            size += b.getProperties().getLength();
        }
        return size;
    }

    public static void readBufferFully(CloudBlob blob, ByteBuffer buffer) throws IOException {
        try {
            buffer.rewind();
            long readBytes = blob.downloadToByteArray(buffer.array(), 0);
            if (buffer.limit() != readBytes) {
                throw new IOException("Buffer size: " + buffer.limit() + ", read bytes: " + readBytes);
            }
        } catch (StorageException e) {
            throw new IOException(e);
        }
    }
}
