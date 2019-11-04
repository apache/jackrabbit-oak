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
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlobClientBase;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.azure.compat.CloudBlobDirectory;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
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

    public static String getName(BlobClientBase blob) {
        Path blobPath = null;
        try {
            blobPath = Paths.get(new URL(blob.getBlobUrl()).getPath());
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("invalid blob url: " + blob.getBlobUrl(), e);
        }

        // TODO: OAK-8413: simplify or describe and remove sout:
        System.out.println("TODO: getName(): " + blob.getBlobUrl());

        int nElements = blobPath.getNameCount();
        return nElements > 1 ? blobPath.subpath(1, nElements - 1).toString() : blobPath.toString();
    }

    /**
     * @param directory
     * @return the name of the directory *only*
     */
    public static String getName(CloudBlobDirectory directory) {
        return Paths.get(directory.getUri().getPath()).getFileName().toString();
    }

    public static List<BlobClient> getBlobs(CloudBlobDirectory directory) {
        return directory.listBlobs()
                .stream()
                .map(directory::getBlobClientAbsolute)
                .collect(Collectors.toList());
    }

    public static void readBufferFully(BlobClientBase blob, Buffer buffer) throws IOException {
        try (ByteBufferOutputStream stream = new ByteBufferOutputStream(buffer)) {
            blob.download(stream);
            buffer.flip();
        }
    }

    public static void deleteAllEntries(CloudBlobDirectory directory) {
        getBlobs(directory).forEach(blob -> directory.deleteBlobIfExists(blob));
    }

    public static CloudBlobDirectory cloudBlobDirectoryFrom(StorageSharedKeyCredential credential,
                                                            String uriString, String dir) throws URISyntaxException, BlobStorageException {
        URI uri = new URI(uriString);
        String host = uri.getHost();
        String containerName = extractContainerName(uri);
        BlobContainerClient client = new BlobServiceClientBuilder()
                .credential(credential)
                .endpoint(String.format("https://%s", host))
                .buildClient()
                .getBlobContainerClient(containerName);
        return new CloudBlobDirectory(client, containerName, dir);
    }

    public static String extractContainerName(URI uri) {
        return Paths.get(uri.getPath()).subpath(0, 1).toString();
    }

    public static CloudBlobDirectory cloudBlobDirectoryFrom(String connection, String containerName,
                                                            String dir) throws BlobStorageException {
        BlobContainerClient containerClient = getContainerClient(connection, containerName);
        return new CloudBlobDirectory(containerClient, containerName, dir);
    }

    @NotNull
    public static BlobContainerClient getContainerClient(String connection, String containerName) {
        BlobContainerClient containerClient;
        try {
            containerClient = new BlobServiceClientBuilder()
                    .connectionString(connection)
                    .buildClient()
                    .createBlobContainer(containerName);
        } catch (RuntimeException cause) {
            throw new IllegalArgumentException(String.format("Invalid connection string - could not parse '%s'", connection), cause);
        }

        if (!containerClient.exists()) {
            containerClient.create();
        }
        return containerClient;
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


