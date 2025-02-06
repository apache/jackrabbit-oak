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
import com.azure.storage.blob.models.BlobListDetails;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.specialized.AppendBlobClient;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.spi.RepositoryNotReachableException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

public final class AzureUtilities {

    public static final String AZURE_ACCOUNT_NAME = "AZURE_ACCOUNT_NAME";
    public static final String AZURE_SECRET_KEY = "AZURE_SECRET_KEY";
    public static final String AZURE_TENANT_ID = "AZURE_TENANT_ID";
    public static final String AZURE_CLIENT_ID = "AZURE_CLIENT_ID";
    public static final String AZURE_CLIENT_SECRET = "AZURE_CLIENT_SECRET";

    private static final Logger log = LoggerFactory.getLogger(AzureUtilities.class);

    private AzureUtilities() {
    }

    public static String getName(BlobItem blob) {
        return Paths.get(blob.getName()).getFileName().toString();
    }

    public static String getName(AppendBlobClient blob) {
        return Paths.get(blob.getBlobName()).getFileName().toString();
    }



    public static List<BlobItem> getBlobs(BlobContainerClient blobContainerClient, ListBlobsOptions listOptions) {
        listOptions.setDetails(new BlobListDetails().setRetrieveMetadata(true));
        return blobContainerClient.listBlobs(listOptions, null).stream().collect(Collectors.toList());
    }

    public static void readBufferFully(BlockBlobClient blob, Buffer buffer) throws IOException {
        try {
            blob.downloadStream(new ByteBufferOutputStream(buffer));
            buffer.flip();
        } catch (BlobStorageException e) {
            if (e.getStatusCode() == 404) {
                log.error("Blob not found in the remote repository: {}", blob.getBlobName());
                throw new FileNotFoundException("Blob not found in the remote repository: " + blob.getBlobName());
            }
            throw new RepositoryNotReachableException(e);
        }
    }

    public static void deleteAllEntries(BlobContainerClient blobContainerClient, ListBlobsOptions listBlobsOptions) {
        getBlobs(blobContainerClient, listBlobsOptions).forEach(b -> {
            try {
                blobContainerClient.getBlobClient(b.getName()).deleteIfExists();
            } catch (BlobStorageException e) {
                log.error("Can't delete blob {}", b.getName(), e);
            }
        });
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


