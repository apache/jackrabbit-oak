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

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlobClientBase;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.azure.compat.CloudBlobDirectory;
import org.apache.jackrabbit.oak.segment.spi.RepositoryNotReachableException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public final class AzureUtilities {

    public static String SEGMENT_FILE_NAME_PATTERN = "^([0-9a-f]{4})\\.([0-9a-f-]+)$";

    private static final Logger log = LoggerFactory.getLogger(AzureUtilities.class);

    private AzureUtilities() {
    }

    public static String getSegmentFileName(AzureSegmentArchiveEntry indexEntry) {
        return getSegmentFileName(indexEntry.getPosition(), indexEntry.getMsb(), indexEntry.getLsb());
    }

    public static String getSegmentFileName(long offset, long msb, long lsb) {
        return String.format("%04x.%s", offset, new UUID(msb, lsb).toString());
    }

    public static String getFilename(BlobClientBase blob) {
        return Paths.get(blob.getBlobName()).getFileName().toString();
    }

    public static List<BlobClient> getBlobs(CloudBlobDirectory directory) throws IOException {
        try {
            return directory.listBlobs()
                    .stream()
                    .map(directory::getBlobClientAbsolute)
                    .collect(Collectors.toList());
        } catch (BlobStorageException e) {
            throw new IOException(e);
        }
    }

    public static void readBufferFully(BlobClientBase blob, Buffer buffer) throws IOException {
        try (ByteBufferOutputStream stream = new ByteBufferOutputStream(buffer)) {
            blob.download(stream);
            buffer.flip();
        } catch (BlobStorageException e) {
            throw new RepositoryNotReachableException(e);
        }
    }

    public static void deleteAllEntries(CloudBlobDirectory directory) throws IOException {
        getBlobs(directory).forEach(blob -> {
            try {
                directory.deleteBlobIfExists(blob);
            } catch (BlobStorageException e) {
                log.error("Can't delete blob {}", AzureUtilities.getBlobPath(blob), e);
            }
        });
    }

    public static CloudBlobDirectory cloudBlobDirectoryFrom(StorageSharedKeyCredential credential,
                                                            String uriString, String dir) throws URISyntaxException, BlobStorageException {
        URI uri = new URI(uriString);
        String containerName = extractContainerName(uri);

        BlobContainerClient container = AzurePersistence.createBlobContainerClient(credential,  uri, containerName);
        return new CloudBlobDirectory(container, dir);
    }

    public static String extractContainerName(URI uri) {
        return Paths.get(uri.getPath()).subpath(0, 1).toString();
    }

    public static CloudBlobDirectory cloudBlobDirectoryFrom(String connection, String containerName,
                                                            String dir) throws BlobStorageException {
        BlobContainerClient containerClient = getContainerClient(connection, containerName);
        return new CloudBlobDirectory(containerClient, dir);
    }

    @NotNull
    public static BlobContainerClient getContainerClient(String connection, String containerName) {

        BlobContainerClient containerClient;
        try {
            containerClient = AzurePersistence.createBlobContainerClient(connection, containerName);
        } catch (RuntimeException cause) {
            throw new IllegalArgumentException(String.format("Invalid connection string - could not parse '%s'", connection), cause);
        }

        if (!containerClient.exists()) {
            containerClient.create();
        }
        return containerClient;
    }

    /**
     * @param blobClient blob
     * @return the absolute path of a blob
     */
    public static String getBlobPath(BlobClient blobClient) {
        try {
            return new URI(blobClient.getBlobUrl()).getPath();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static class ByteBufferOutputStream extends OutputStream {

        @NotNull
        private final Buffer buffer;

        public ByteBufferOutputStream(@NotNull Buffer buffer) {
            this.buffer = buffer;
        }

        @Override
        public void write(int b) {
            buffer.put((byte) b);
        }

        @Override
        public void write(@NotNull byte[] bytes, int offset, int length) {
            buffer.put(bytes, offset, length);
        }
    }
}


