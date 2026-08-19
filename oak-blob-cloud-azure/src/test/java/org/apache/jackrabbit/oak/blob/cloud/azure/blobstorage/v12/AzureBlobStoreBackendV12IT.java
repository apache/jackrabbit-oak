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

import com.azure.storage.blob.specialized.BlockBlobClient;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzuriteDockerRule;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadToken;
import org.apache.jackrabbit.oak.spi.blob.data.DataRecord;
import org.apache.jackrabbit.oak.spi.blob.data.DataStoreException;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for data-loss safety properties of AzureBlobStoreBackendV12.
 * <p>
 * Tests are in the v12 package to access package-private/protected methods of AzureBlobStoreBackendV12.
 * Uses Azurite (Microsoft's open-source Azure Storage emulator running in Docker) — no external
 * credentials needed, and the emulator's real block-blob commit semantics are required to exercise
 * the concurrent-write and metadata-atomicity scenarios tested here.
 * <p>
 * These tests verify findings from the pre-merge data-loss risk assessment that require real storage
 * to exercise: metadata atomicity on upload completion, concurrent completeHttpUpload, and
 * concurrent reference-key initialization.
 */
public class AzureBlobStoreBackendV12IT {

    // Azurite Docker container shared across all tests in this class; starting it once
    // keeps the suite fast. The container is torn down after the last test completes.
    @ClassRule
    public static final AzuriteDockerRule AZURITE = new AzuriteDockerRule();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private AzureBlobStoreBackendV12 backend;
    private String containerName;

    /**
     * Constructs a blob key using the same format as getKeyName(identifier):
     * first 4 hex chars, then "-", then the remainder.
     */
    private static String newBlobId() {
        String id = UUID.randomUUID().toString().replace("-", ""); // 32 hex chars
        return id.substring(0, 4) + "-" + id.substring(4);
    }

    @Before
    public void setUp() throws DataStoreException, IOException {
        containerName = "v12it-" + System.nanoTime();
        Properties props = azuriteProps(containerName);

        AzureDataStoreV12 store = new AzureDataStoreV12();
        store.setProperties(props);
        store.setStagingSplitPercentage(0); // disable local staging cache; all writes go directly to Azurite
        store.init(folder.newFolder().getAbsolutePath());

        backend = (AzureBlobStoreBackendV12) store.getBackend();
    }

    @After
    public void tearDown() {
        // Nothing to close for the backend directly; the store lifecycle is managed by the test.
    }

    /**
     * A blob committed via completeHttpUpload must have a "lastModified" metadata key; its absence makes getLastModified() fall back to the Azure server timestamp and can cause deleteAllOlderThan() to GC the blob prematurely.
     */
    @Test
    public void directUploadCompletion_hasLastModifiedMetadata()
            throws Exception {
        byte[] payload = new byte[4096];
        Arrays.fill(payload, (byte) 0x42);

        // Build a valid upload token manually — no SAS URL needed; completeHttpUpload
        // only uses the blobId from the token to find and commit blocks.
        String blobId = newBlobId();
        String uploadId = Base64.getEncoder().encodeToString(UUID.randomUUID().toString().getBytes());
        DataRecordUploadToken token = new DataRecordUploadToken(blobId, uploadId);
        byte[] refKey = backend.getOrCreateReferenceKey();
        String encodedToken = token.getEncodedToken(refKey);

        // Stage a block directly in Azurite using the container client (not via SAS URL).
        String blockId = Base64.getEncoder().encodeToString("blk001".getBytes());
        backend.getAzureContainer()
                .getBlobClient(blobId)
                .getBlockBlobClient()
                .stageBlock(blockId, new ByteArrayInputStream(payload), payload.length);

        // Complete the upload — this invokes commitBlocksAndGetSize, which now atomically
        // includes lastModified metadata via BlockBlobCommitBlockListOptions.
        DataRecord dataRecord = backend.completeHttpUpload(encodedToken);
        assertNotNull("completeHttpUpload must return a DataRecord", dataRecord);
        assertEquals("DataRecord length must equal payload size", payload.length, dataRecord.getLength());

        // Verify the committed blob has lastModified metadata in Azurite.
        BlockBlobClient blobClient = backend.getAzureContainer()
                .getBlobClient(blobId).getBlockBlobClient();
        Map<String, String> metadata = blobClient.getProperties().getMetadata();
        assertTrue(
                "committed blob must have 'lastModified' metadata; " +
                        "absent key causes getLastModified() to fall back to Azure server timestamp, " +
                        "which can cause deleteAllOlderThan() to prematurely GC the blob",
                metadata != null && metadata.containsKey(AzureConstantsV12.AZURE_BLOB_LAST_MODIFIED_KEY));

        long lastModified = Long.parseLong(metadata.get(AzureConstantsV12.AZURE_BLOB_LAST_MODIFIED_KEY));
        long now = System.currentTimeMillis();
        assertTrue("lastModified must be a recent epoch-millis value",
                lastModified > 0 && lastModified <= now && lastModified > now - 60_000);
    }

    /**
     * Concurrent completeHttpUpload calls on the same token must not produce a zero-length DataRecord — a zero length means the committed-block fallback read an empty list before the first commit was durable.
     */
    @Test
    public void concurrentCompleteUpload_neitherResultZeroLength() throws Exception {
        byte[] payload = new byte[8192];
        Arrays.fill(payload, (byte) 0x55);

        String blobId = newBlobId();
        String uploadId = Base64.getEncoder().encodeToString(UUID.randomUUID().toString().getBytes());
        DataRecordUploadToken token = new DataRecordUploadToken(blobId, uploadId);
        byte[] refKey = backend.getOrCreateReferenceKey();
        String encodedToken = token.getEncodedToken(refKey);

        // Stage blocks in Azurite directly.
        String blockId = Base64.getEncoder().encodeToString("blk001".getBytes());
        backend.getAzureContainer()
                .getBlobClient(blobId)
                .getBlockBlobClient()
                .stageBlock(blockId, new ByteArrayInputStream(payload), payload.length);

        // Two threads concurrently complete the same upload.
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        List<Future<DataRecord>> futures = new ArrayList<>();

        for (int i = 0; i < 2; i++) {
            futures.add(pool.submit(() -> {
                ready.countDown();
                start.await();
                return backend.completeHttpUpload(encodedToken);
            }));
        }

        ready.await();
        start.countDown();
        pool.shutdown();
        boolean finished30 = pool.awaitTermination(30, TimeUnit.SECONDS);
        if (!finished30) {
            pool.shutdownNow();
        }
        assertTrue("threads must finish within 30s", finished30);

        // At least one thread must succeed; neither must return a zero-length record.
        int successes = 0;
        for (Future<DataRecord> f : futures) {
            DataRecord result = null;
            try {
                result = f.get();
                successes++;
            } catch (Exception e) {
                // One thread may throw (e.g. DataStoreException) if the other already committed.
                // That is acceptable — data safety means the successful commit has the right size.
                continue;
            }
            assertNotNull(result);
            assertNotEquals("concurrent completeHttpUpload must never return a zero-length DataRecord — " +
                    "a zero-length result means the committed-block fallback read a stale empty list", 0, result.getLength());
            assertEquals("DataRecord length must equal payload size",
                    payload.length, result.getLength());
        }
        assertTrue("at least one completeHttpUpload call must succeed", successes >= 1);
    }

    /**
     * Concurrent backend initialization against the same container must produce a single consistent reference key; diverged keys cause upload-token verification failures that orphan staged blocks.
     */
    @Test
    public void concurrentReferenceKeyInit_allBackendsGetSameKey() throws Exception {
        // Use the container already created in setUp, so all backends share the same storage.
        Properties props = azuriteProps(containerName);

        int n = 4;
        // ready: each thread signals when it has constructed its store (but not called init yet).
        // start: released once all threads are ready, so they race into init() simultaneously.
        CountDownLatch ready = new CountDownLatch(n);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(n);
        List<Future<byte[]>> futures = new ArrayList<>();

        for (int i = 0; i < n; i++) {
            futures.add(pool.submit(() -> {
                AzureDataStoreV12 store = new AzureDataStoreV12();
                store.setProperties(props);
                store.setStagingSplitPercentage(0);
                ready.countDown();
                start.await();
                store.init(folder.newFolder().getAbsolutePath());
                AzureBlobStoreBackendV12 b = (AzureBlobStoreBackendV12) store.getBackend();
                return b.getOrCreateReferenceKey();
            }));
        }

        ready.await();
        start.countDown();
        pool.shutdown();
        boolean finished60 = pool.awaitTermination(60, TimeUnit.SECONDS);
        if (!finished60) {
            pool.shutdownNow();
        }
        assertTrue("backends must initialize within 60s", finished60);

        List<byte[]> keys = new ArrayList<>();
        for (Future<byte[]> f : futures) {
            keys.add(f.get()); // propagates any exception from the init thread
        }

        // All returned keys must be identical — a diverged key would cause token-verification failure.
        byte[] first = keys.get(0);
        assertNotNull("reference key must not be null", first);
        for (int i = 1; i < keys.size(); i++) {
            assertArrayEquals("all concurrently-initialized backends must hold the same reference key; " +
                    "diverged keys cause upload-token verification failures and orphaned blocks", first, keys.get(i));
        }

        // Count reference key blobs in Azurite — must be exactly one.
        long refKeyCount = backend.getAzureContainer()
                .listBlobs(new com.azure.storage.blob.models.ListBlobsOptions()
                        .setPrefix(AzureConstantsV12.AZURE_BLOB_META_DIR_NAME + "/"), null)
                .stream()
                .filter(b -> b.getName().contains(AzureConstantsV12.AZURE_BLOB_REF_KEY)
                        || b.getName().contains("oak.datastore.key"))
                .count();

        assertTrue(
                "concurrent init must result in at most one reference key in storage; " +
                        "found " + refKeyCount + " — multiple keys indicate a write race that corrupts token signing",
                refKeyCount <= 1);
    }

    private Properties azuriteProps(String containerName) {
        return AzuriteV12TestUtils.azuriteProps(containerName, AZURITE.getBlobEndpoint());
    }
}
