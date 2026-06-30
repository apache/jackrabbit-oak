/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v12;

import com.azure.storage.blob.BlobContainerClient;
import org.apache.jackrabbit.oak.spi.blob.data.DataStoreException;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * Unit tests for AzureBlobStoreBackendV12.
 */
public class AzureBlobStoreBackendV12Test {

    /**
     * getAllMetadataRecords must throw on storage error, not return empty — an empty result tells GC no records exist and causes it to delete all blobs.
     */
    @Test(expected = RuntimeException.class)
    public void getAllMetadataRecords_storageException_propagatesInsteadOfReturningEmpty() {
        new FailingContainerBackend().getAllMetadataRecords("prefix");
    }

    /**
     * deleteAllMetadataRecords must throw on storage error, not silently succeed — a silent no-op leaves stale metadata that misleads the next GC mark phase.
     */
    @Test(expected = RuntimeException.class)
    public void deleteAllMetadataRecords_storageException_propagatesInsteadOfSilentReturn() {
        new FailingContainerBackend().deleteAllMetadataRecords("prefix");
    }

    /**
     * IllegalArgumentException from Azure SDK validation inside uploadBlob must be caught and surfaced as DataStoreException, not escape unchecked (which would silently leave the blob unwritten).
     */
    @Test(expected = DataStoreException.class)
    public void uploadBlob_illegalArgumentFromSdk_wrappedAsDataStoreException() throws Exception {
        FailingUploadBackend backend = new FailingUploadBackend();
        java.io.File tempFile = java.io.File.createTempFile("safety-test", ".bin");
        tempFile.deleteOnExit();
        backend.write(new org.apache.jackrabbit.oak.spi.blob.data.DataIdentifier("test1234567890abcdef"), tempFile);
    }

    /**
     * Concurrent cold-start must write exactly one key — a second write invalidates upload tokens signed against the first, orphaning staged blocks.
     */
    @Test
    public void getOrCreateReferenceKey_concurrentColdStart_writesOnce() throws Exception {
        CountDownLatch writeStarted = new CountDownLatch(1);
        CountDownLatch letWriteProceed = new CountDownLatch(1);
        AtomicInteger writeCount = new AtomicInteger(0);

        // Backend that: returns null until a key is stored, and blocks in addMetadataRecord
        // so Thread 2 can observe null before Thread 1 finishes writing.
        AzureBlobStoreBackendV12 backend = new AzureBlobStoreBackendV12() {
            volatile byte[] stored = null;

            @Override
            protected byte[] readMetadataBytes(String name) {
                return stored;
            }

            @Override
            public void addMetadataRecord(InputStream input, String name) throws DataStoreException {
                try {
                    writeCount.incrementAndGet();
                    writeStarted.countDown();
                    letWriteProceed.await();
                    ByteArrayOutputStream buf = new ByteArrayOutputStream();
                    byte[] chunk = new byte[256];
                    int n;
                    while ((n = input.read(chunk)) != -1) buf.write(chunk, 0, n);
                    stored = buf.toByteArray();
                } catch (IOException | InterruptedException e) {
                    throw new DataStoreException(e.getMessage());
                }
            }
        };

        ExecutorService exec = Executors.newFixedThreadPool(2);
        // Thread 1: starts initialization, enters addMetadataRecord and blocks there
        Future<byte[]> f1 = exec.submit(backend::getOrCreateReferenceKey);
        writeStarted.await(5, TimeUnit.SECONDS);

        // Thread 2: starts while Thread 1 is blocked mid-write
        Future<byte[]> f2 = exec.submit(backend::getOrCreateReferenceKey);
        Thread.sleep(50); // NOSONAR: intentional timing gap so Thread 2 races into getOrCreateReferenceKey while Thread 1 is mid-write

        letWriteProceed.countDown(); // let Thread 1 finish writing

        byte[] key1 = f1.get(5, TimeUnit.SECONDS);
        byte[] key2 = f2.get(5, TimeUnit.SECONDS);
        exec.shutdown();

        assertEquals("Concurrent cold-start must write exactly one key; a second write invalidates upload tokens from the first", 1, writeCount.get());
        assertArrayEquals("Both concurrent callers must return the same reference key", key1, key2);
    }

    /**
     * getDefaultBlobStorageDomain() provides the host embedded in SAS presigned download URIs.
     * Wrong value makes client fetches fail against non-standard (Azurite, private cloud) endpoints.
     */
    @Test
    public void getDefaultBlobStorageDomain_customEndpoint_returnsHostFromEndpoint() {
        AzureBlobStoreBackendV12 backend = new AzureBlobStoreBackendV12();
        Properties props = new Properties();
        props.setProperty(AzureConstantsV12.AZURE_BLOB_ENDPOINT, "https://myaccount.blob.core.some.custom.endpoint.com");
        props.setProperty(AzureConstantsV12.AZURE_STORAGE_ACCOUNT_NAME, "myaccount");
        backend.setProperties(props);

        assertEquals("myaccount.blob.core.some.custom.endpoint.com", backend.getDefaultBlobStorageDomain());
    }

    @Test
    public void getDefaultBlobStorageDomain_noCustomEndpoint_returnsDefaultWindowsNet() {
        AzureBlobStoreBackendV12 backend = new AzureBlobStoreBackendV12();
        Properties props = new Properties();
        props.setProperty(AzureConstantsV12.AZURE_STORAGE_ACCOUNT_NAME, "myaccount");
        backend.setProperties(props);

        assertEquals("myaccount.blob.core.windows.net", backend.getDefaultBlobStorageDomain());
    }

    @Test
    public void getDefaultBlobStorageDomain_malformedEndpoint_fallsBackToAccountName() {
        AzureBlobStoreBackendV12 backend = new AzureBlobStoreBackendV12();
        Properties props = new Properties();
        props.setProperty(AzureConstantsV12.AZURE_BLOB_ENDPOINT, "not a valid uri ://@@");
        props.setProperty(AzureConstantsV12.AZURE_STORAGE_ACCOUNT_NAME, "myaccount");
        backend.setProperties(props);

        assertEquals("myaccount.blob.core.windows.net", backend.getDefaultBlobStorageDomain());
    }

    @Test
    public void getDefaultBlobStorageDomain_noAccountAndNoEndpoint_returnsNull() {
        AzureBlobStoreBackendV12 backend = new AzureBlobStoreBackendV12();
        backend.setProperties(new Properties());

        assertNull(backend.getDefaultBlobStorageDomain());
    }

    static class FailingContainerBackend extends AzureBlobStoreBackendV12 {
        @Override
        protected BlobContainerClient getAzureContainer() throws DataStoreException {
            throw new DataStoreException("simulated Azure connectivity failure");
        }
    }

    static class FailingUploadBackend extends AzureBlobStoreBackendV12 {
        @Override
        protected BlobContainerClient getAzureContainer() throws DataStoreException {
            com.azure.storage.blob.BlobContainerClient mock =
                    org.mockito.Mockito.mock(com.azure.storage.blob.BlobContainerClient.class);
            com.azure.storage.blob.BlobClient blobClient =
                    org.mockito.Mockito.mock(com.azure.storage.blob.BlobClient.class);
            com.azure.storage.blob.specialized.BlockBlobClient blockBlobClient =
                    org.mockito.Mockito.mock(com.azure.storage.blob.specialized.BlockBlobClient.class);
            org.mockito.Mockito.when(mock.getBlobClient(org.mockito.ArgumentMatchers.anyString()))
                    .thenReturn(blobClient);
            org.mockito.Mockito.when(blobClient.getBlockBlobClient()).thenReturn(blockBlobClient);
            org.mockito.Mockito.when(blockBlobClient.exists()).thenReturn(false);
            org.mockito.Mockito.when(blobClient.uploadFromFileWithResponse(
                            org.mockito.ArgumentMatchers.any(),
                            org.mockito.ArgumentMatchers.any(),
                            org.mockito.ArgumentMatchers.any()))
                    .thenThrow(new IllegalArgumentException("blockSize must be <= 4000 MiB"));
            org.mockito.Mockito.when(blockBlobClient.getContainerClient()).thenReturn(mock);
            return mock;
        }
    }
}
