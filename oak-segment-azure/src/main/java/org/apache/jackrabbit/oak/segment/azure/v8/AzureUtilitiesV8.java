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

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.ResultContinuation;
import com.microsoft.azure.storage.ResultSegment;
import com.microsoft.azure.storage.StorageCredentials;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.StorageUri;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.LeaseStatus;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.spi.RepositoryNotReachableException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public final class AzureUtilitiesV8 {

    public static final String AZURE_ACCOUNT_NAME = "AZURE_ACCOUNT_NAME";
    public static final String AZURE_SECRET_KEY = "AZURE_SECRET_KEY";
    public static final String AZURE_TENANT_ID = "AZURE_TENANT_ID";
    public static final String AZURE_CLIENT_ID = "AZURE_CLIENT_ID";
    public static final String AZURE_CLIENT_SECRET = "AZURE_CLIENT_SECRET";

    private static final Logger log = LoggerFactory.getLogger(AzureUtilitiesV8.class);

    private AzureUtilitiesV8() {
    }

    public static String getName(CloudBlob blob) {
        return Paths.get(blob.getName()).getFileName().toString();
    }

    public static String getName(CloudBlobDirectory directory) {
        return Paths.get(directory.getUri().getPath()).getFileName().toString();
    }

    public static List<CloudBlob> getBlobs(CloudBlobDirectory directory) throws IOException {
        List<CloudBlob> blobList = new ArrayList<>();
        ResultContinuation token = null;
        do {
            ResultSegment<ListBlobItem> result = listBlobsInSegments(directory, token); //get the blobs in pages of 5000
            for (ListBlobItem b : result.getResults()) {                                //add resultant blobs to list
                if (b instanceof CloudBlob) {
                    CloudBlob cloudBlob = (CloudBlob) b;
                    blobList.add(cloudBlob);
                }
            }
            token = result.getContinuationToken();
        } while (token != null);
        return blobList;
    }

    public static void readBufferFully(CloudBlob blob, Buffer buffer) throws IOException {
        try {
            blob.download(new ByteBufferOutputStream(buffer));
            buffer.flip();
        } catch (StorageException e) {
            if (e.getHttpStatusCode() == 404) {
                log.error("Blob not found in the remote repository: {}", blob.getName());
                throw new FileNotFoundException("Blob not found in the remote repository: " + blob.getName());
            }
            throw new RepositoryNotReachableException(e);
        }
    }

    public static void deleteAllEntries(CloudBlobDirectory directory) throws IOException {
        getBlobs(directory).forEach(b -> {
            try {
                b.deleteIfExists();
            } catch (StorageException e) {
                log.error("Can't delete blob {}", b.getUri().getPath(), e);
            }
        });
    }

    public static CloudBlobDirectory cloudBlobDirectoryFrom(StorageCredentials credentials,
                                                            String uri, String dir) throws URISyntaxException, StorageException {
        StorageUri storageUri = new StorageUri(new URI(uri));
        CloudBlobContainer container = new CloudBlobContainer(storageUri, credentials);

        container.createIfNotExists();

        return container.getDirectoryReference(dir);
    }

    public static CloudBlobDirectory cloudBlobDirectoryFrom(String connection, String containerName,
                                                            String dir) throws InvalidKeyException, URISyntaxException, StorageException {
        CloudStorageAccount cloud = CloudStorageAccount.parse(connection);
        CloudBlobContainer container = cloud.createCloudBlobClient().getContainerReference(containerName);
        container.createIfNotExists();

        return container.getDirectoryReference(dir);
    }

    private static ResultSegment<ListBlobItem> listBlobsInSegments(CloudBlobDirectory directory,
                                                                   ResultContinuation token) throws IOException {
        ResultSegment<ListBlobItem> result = null;
        IOException lastException = null;
        for (int sleep = 10; sleep <= 10000; sleep *= 10) {  //increment the sleep time in steps.
            try {
                result = directory.listBlobsSegmented(
                        null,
                        false,
                        EnumSet.of(BlobListingDetails.METADATA),
                        5000,
                        token,
                        null,
                        null);
                break;  //we have the results, no need to retry
            } catch (StorageException | URISyntaxException e) {
                lastException = new IOException(e);
                try {
                    Thread.sleep(sleep); //Sleep and retry
                } catch (InterruptedException ex) {
                    log.warn("Interrupted", e);
                }
            }
        }

        if (result == null) {
            throw lastException;
        } else {
            return result;
        }
    }

    public static void deleteAllBlobs(@NotNull CloudBlobDirectory directory) throws URISyntaxException, StorageException, InterruptedException {
        for (ListBlobItem blobItem : directory.listBlobs()) {
            if (blobItem instanceof CloudBlob) {
                CloudBlob cloudBlob = (CloudBlob) blobItem;
                if (cloudBlob.getProperties().getLeaseStatus() == LeaseStatus.LOCKED) {
                    cloudBlob.breakLease(0);
                }
                cloudBlob.deleteIfExists();
            } else if (blobItem instanceof CloudBlobDirectory) {
                CloudBlobDirectory cloudBlobDirectory = (CloudBlobDirectory) blobItem;
                deleteAllBlobs(cloudBlobDirectory);
            }
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


