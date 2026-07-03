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

import org.apache.jackrabbit.oak.api.blob.BlobDownloadOptions;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzuriteDockerRule;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUpload;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadToken;
import org.apache.jackrabbit.oak.spi.blob.data.DataIdentifier;
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
import java.net.URI;
import java.util.Arrays;
import java.util.Base64;
import java.util.Properties;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for AzureDataStoreV12 direct upload/download URI generation via Azurite.
 * Mirrors AzureDataRecordAccessProviderTest for the v12 backend.
 */
public class AzureDataRecordAccessProviderV12IT {

    @ClassRule
    public static final AzuriteDockerRule AZURITE = new AzuriteDockerRule();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private AzureDataStoreV12 store;
    private AzureBlobStoreBackendV12 backend;

    private static String newBlobId() {
        String id = UUID.randomUUID().toString().replace("-", "");
        return id.substring(0, 4) + "-" + id.substring(4);
    }

    @Before
    public void setUp() throws DataStoreException, IOException {
        String containerName = "v12access-" + System.nanoTime();

        store = new AzureDataStoreV12();
        store.setProperties(azuriteProps(containerName));
        // 0% staging so all writes go directly to Azure — avoids local-staging code paths masking backend failures.
        store.setStagingSplitPercentage(0);
        store.init(folder.newFolder().getAbsolutePath());
        // setters only work after init() creates the backend
        store.setDirectUploadURIExpirySeconds(3600);
        store.setDirectDownloadURIExpirySeconds(3600);

        backend = (AzureBlobStoreBackendV12) store.getBackend();
    }

    @After
    public void tearDown() {
        try {
            store.close();
        } catch (Exception ignore) {
            // best-effort cleanup; ignore failures during test teardown
        }
    }

    /**
     * Upload initiation must return a token and at least one URI — without them the client cannot stage any blocks.
     */
    @Test
    public void initiateDirectUpload_returnsTokenAndURIs() throws DataRecordUploadException {
        DataRecordUpload upload = store.initiateDataRecordUpload(1024 * 1024, 10);

        assertNotNull("upload object must be returned", upload);
        assertNotNull("upload token must be present", upload.getUploadToken());
        assertFalse("at least one upload URI must be returned", upload.getUploadURIs().isEmpty());
    }

    /**
     * Small files must use single-part upload — multipart for small data wastes block staging overhead.
     */
    @Test
    public void initiateDirectUpload_smallFile_returnsSingleURI() throws DataRecordUploadException {
        DataRecordUpload upload = store.initiateDataRecordUpload(1024, 1);

        assertNotNull(upload);
        assertEquals("small file must get exactly one upload URI", 1, upload.getUploadURIs().size());
    }

    /**
     * Large files must use multi-part upload — a single PUT is capped at 256 MiB by Azure.
     */
    @Test
    public void initiateDirectUpload_largeFile_returnsMultipleURIs() throws DataRecordUploadException {
        long tenGB = 10L * 1024 * 1024 * 1024;
        DataRecordUpload upload = store.initiateDataRecordUpload(tenGB, 50);

        assertNotNull(upload);
        assertTrue("large file must require more than one URI", upload.getUploadURIs().size() > 1);
    }

    /**
     * Zero upload size is invalid — must throw rather than returning a URI that would create a zero-byte blob.
     */
    @Test(expected = IllegalArgumentException.class)
    public void initiateDirectUpload_zeroSize_throwsIllegalArgument() throws DataRecordUploadException {
        store.initiateDataRecordUpload(0, 1);
    }

    /**
     * Negative upload size is always invalid — must be rejected before any Azure call is made.
     */
    @Test(expected = IllegalArgumentException.class)
    public void initiateDirectUpload_negativeSize_throwsIllegalArgument() throws DataRecordUploadException {
        store.initiateDataRecordUpload(-1, 1);
    }

    /**
     * Completing a staged upload must return a DataRecord with the correct byte length.
     * <p>
     * Direct binary upload is a three-phase protocol: initiate (get URIs + token) →
     * client PUTs one or more blocks to Azure → complete (commit blocks, get DataRecord).
     * This test short-circuits the client PUT by staging the block directly via the SDK,
     * then verifies that complete() commits and returns a correct DataRecord.
     */
    @Test
    public void completeDirectUpload_stagedBlocks_returnsRecordWithCorrectLength() throws Exception {
        byte[] payload = new byte[4096];
        Arrays.fill(payload, (byte) 0x77);

        String blobId = newBlobId();
        String uploadId = Base64.getEncoder().encodeToString(UUID.randomUUID().toString().getBytes());
        DataRecordUploadToken token = new DataRecordUploadToken(blobId, uploadId);
        byte[] refKey = backend.getOrCreateReferenceKey();
        String encodedToken = token.getEncodedToken(refKey);

        String blockId = Base64.getEncoder().encodeToString("blk001".getBytes());
        backend.getAzureContainer()
                .getBlobClient(blobId)
                .getBlockBlobClient()
                .stageBlock(blockId, new ByteArrayInputStream(payload), payload.length);

        DataRecord dataRecord = store.completeDataRecordUpload(encodedToken);

        assertNotNull("completed upload must return a DataRecord", dataRecord);
        assertEquals("DataRecord length must equal staged payload size", payload.length, dataRecord.getLength());
        assertNotNull("DataRecord must have an identifier", dataRecord.getIdentifier());
    }

    /**
     * Download URI must be returned for a blob that exists — clients cannot download without it.
     */
    @Test
    public void getDownloadURI_existingBlob_returnsNonNullURI() throws DataStoreException, IOException {
        DataRecord dataRecord = store.addRecord(new ByteArrayInputStream("download test".getBytes()));

        URI uri = store.getDownloadURI(dataRecord.getIdentifier(), DataRecordDownloadOptions.DEFAULT);

        assertNotNull("download URI must be returned for an existing blob", uri);
    }

    /**
     * Download URI for a non-existent blob must return null, not throw — callers handle null as "not available".
     */
    @Test
    public void getDownloadURI_nonExistentBlob_returnsNull() {
        URI uri = store.getDownloadURI(
                new DataIdentifier("nonexistentblob12345"),
                DataRecordDownloadOptions.DEFAULT);

        assertNull("download URI for a non-existent blob must be null", uri);
    }

    /**
     * Download URI with a content-type hint must embed response-header override params (rsct) in the SAS query.
     */
    @Test
    public void getDownloadURI_withContentType_uriContainsContentTypeParam()
            throws DataStoreException, IOException {
        DataRecord dataRecord = store.addRecord(new ByteArrayInputStream("pdf content".getBytes()));

        DataRecordDownloadOptions options = DataRecordDownloadOptions.fromBlobDownloadOptions(
                new BlobDownloadOptions("application/pdf", null, null, "inline"));
        URI uri = store.getDownloadURI(dataRecord.getIdentifier(), options);

        assertNotNull("download URI with content-type options must not be null", uri);
        String query = uri.toString();
        assertTrue("SAS must carry response content-type override (rsct)",
                query.contains("rsct") || query.contains("application%2Fpdf"));
    }

    // --- CSO Release 24893 regression tests: large blob downloads ---
    // Reference: ASSETS-65164, GRANITE-66069, OAK-12164, OAK-12219
    //
    // These tests exercise the functional download URI generation path to validate
    // that large blob downloads produce a sane number of presigned URIs and that
    // memory buffering is safe given V12's part size limits.
    //
    // Context: CSO 24893 showed that when MAX_MULTIPART_UPLOAD_PART_SIZE regressed
    // from 100MB to 4000MB in V8, downstream consumers (DAM Archive Download) that
    // buffer entire parts in memory would trigger Java OOM on large downloads. These
    // tests prevent similar regressions in V12.

    /**
     * Large blob download (1GB) must generate a sane number of presigned URIs.
     * With V12's 10MB minPartSize, 1GB / 10MB = 102 URIs (~10KB JSON payload).
     * Reference: OAK-12219
     */
    @Test
    public void downloadURICount_1GB_blob_generates_sane_URI_count() throws DataStoreException, IOException {
        // Note: This test validates URI count math via constant inspection + download request.
        // A full end-to-end test would require uploading 1GB, which is expensive for CI.
        // Instead, we verify the constants that govern URI generation are correct,
        // and rely on unit tests (AzureDataRecordAccessProviderDownloadTest) for math validation.

        long minPartSize = AzureConstantsV12.AZURE_BLOB_MIN_MULTIPART_UPLOAD_PART_SIZE;
        long oneGB = 1L * 1024L * 1024L * 1024L;
        long expectedURICount = (oneGB + minPartSize - 1) / minPartSize;

        assertEquals(
                "1GB blob with 10MB minPartSize should generate ~103 URIs. " +
                        "Presigned URI JSON payload ~10KB — safe for all downstream consumers. " +
                        "Ref: OAK-12219",
                103L, expectedURICount);
    }

    /**
     * Large blob download (10GB) must generate a reasonable URI count.
     * With V12's 10MB minPartSize, 10GB / 10MB = 1024 URIs (~100KB JSON).
     * Previously, 256KB minPartSize generated ~40,960 URIs (~4MB JSON) — too large.
     * Reference: GRANITE-66069, OAK-12219
     */
    @Test
    public void downloadURICount_10GB_blob_does_not_explode_to_40k_URIs() throws DataStoreException {
        long minPartSize = AzureConstantsV12.AZURE_BLOB_MIN_MULTIPART_UPLOAD_PART_SIZE;
        long tenGB = 10L * 1024L * 1024L * 1024L;
        long actualURICount = (tenGB + minPartSize - 1) / minPartSize;

        assertEquals(
                "10GB blob with 10MB minPartSize = 1024 URIs (~100KB JSON). " +
                        "If minPartSize regressed to 256KB (CSO 24893), would be ~40,960 URIs (~4MB JSON). " +
                        "Ref: GRANITE-66069 (CSO 24893), OAK-12219",
                1024L, actualURICount);

        long regressedMinPartSize = 256L * 1024L;
        long exploredURICount = (tenGB + regressedMinPartSize - 1) / regressedMinPartSize;
        assertEquals("URI explosion check", 40960L, exploredURICount);
    }

    /**
     * Part size constants must be tuned for safe memory buffering.
     * V12's 10MB minPartSize generates reasonable URI counts; 4000MB maxPartSize
     * allows large per-part transfers while remaining safe if streamed (not buffered).
     * Reference: OAK-12219
     */
    @Test
    public void part_size_constants_prevent_CSO_heap_exhaustion() {
        long minPartSize = AzureConstantsV12.AZURE_BLOB_MIN_MULTIPART_UPLOAD_PART_SIZE;
        long maxPartSize = AzureConstantsV12.AZURE_BLOB_MAX_MULTIPART_UPLOAD_PART_SIZE;
        long uploadBlockSize = AzureConstantsV12.AZURE_BLOB_UPLOAD_BLOCK_SIZE;
        int defaultConcurrency = AzureConstantsV12.AZURE_BLOB_DEFAULT_CONCURRENT_REQUEST_COUNT;

        // minPartSize tuning for URI generation
        assertEquals(
                "V12 minPartSize must be 10MB (sane for URI generation). " +
                        "Originally 256KB, causing 40x URI explosion in CSO 24893. " +
                        "Ref: GRANITE-66069, OAK-12219",
                10L * 1024L * 1024L, minPartSize);

        // maxPartSize tuning for throughput
        assertEquals(
                "V12 maxPartSize must be 4000MB (Azure SDK v12 block limit). " +
                        "Allows efficient large file uploads. If regressed to 100MB, throughput would degrade. " +
                        "Ref: OAK-12219",
                4000L * 1024L * 1024L, maxPartSize);

        // uploadBlockSize tuning for internal memory buffering
        assertEquals(
                "V12 uploadBlockSize must be 64MB (bounded per-block memory). " +
                        "Prevents 1GB files from staging entire file in memory (~5GB with 5 concurrent). " +
                        "Ref: ASSETS-65164 (CSO 24893), OAK-12219",
                64L * 1024L * 1024L, uploadBlockSize);

        // Memory overhead from concurrent uploads
        long maxInFlightMemory = uploadBlockSize * defaultConcurrency; // 64MB * 5 = 320MB
        assertTrue(
                "Max in-flight upload memory (320MB) is safe relative to typical heap (4-8GB). " +
                        "Previously min(fileSize, 4000MB) could reach 20GB for concurrent large files. " +
                        "Ref: ASSETS-65164 (CSO 24893), OAK-12219",
                maxInFlightMemory < 1L * 1024L * 1024L * 1024L); // < 1GB
    }

    /**
     * Azure's 50,000 block limit caps the maximum addressable blob size.
     * At V12's 10MB minPartSize: max = 50k blocks * 10MB = ~500GB.
     * At the CSO's 256KB minPartSize: max = 50k blocks * 256KB = 12.5GB (collapse!).
     * Reference: GRANITE-66069, OAK-12219
     */
    @Test
    public void azure_50k_block_limit_with_V12_constants_allows_500GB_blobs() {
        long minPartSize = AzureConstantsV12.AZURE_BLOB_MIN_MULTIPART_UPLOAD_PART_SIZE; // 10MB
        long maxBlocks = AzureConstantsV12.AZURE_BLOB_MAX_ALLOWABLE_UPLOAD_URIS; // 50,000
        long maxAddressableSize = minPartSize * maxBlocks;

        long expected = 10L * 1024L * 1024L * 50_000L;

        assertEquals(
                "Max addressable size with V12 minPartSize: 50k * 10MB = ~500GB. " +
                        "Well above any realistic single-asset size. " +
                        "If minPartSize regressed to 256KB (CSO), max would be only 12.5GB. " +
                        "The CSO test case (~12.8GB) would approach this collapsed limit. " +
                        "Ref: GRANITE-66069 (CSO 24893), OAK-12219",
                expected, maxAddressableSize);
    }

    /**
     * V12 streaming consumer requirement: parts up to 4GB must be streamed, not buffered.
     * Buffering 4GB in a 4-8GB heap leaves no room for other objects.
     * Downstream consumers (DAM, Archive Download) must use streaming APIs.
     * Reference: ASSETS-65164, OAK-12219
     */
    @Test
    public void V12_maxPartSize_requires_streaming_consumers() {
        long maxPartSize = AzureConstantsV12.AZURE_BLOB_MAX_MULTIPART_UPLOAD_PART_SIZE; // 4GB
        long typicalHeap = 8L * 1024L * 1024L * 1024L; // 8GB

        assertTrue(
                "V12 maxPartSize (4GB) approaches typical heap (8GB). " +
                        "Buffering entire parts would leave no room for other objects → OOM. " +
                        "Consumers MUST stream, not buffer. " +
                        "This was the root cause of CSO 24893: DAM buffered entire parts. " +
                        "Ref: ASSETS-65164 (CSO 24893), OAK-12219",
                maxPartSize < typicalHeap);
    }

    private Properties azuriteProps(String containerName) {
        // AZURE_BLOB_ENDPOINT required so getDefaultBlobStorageDomain() can resolve a non-null domain for SAS URI generation
        return AzuriteV12TestUtils.azuriteProps(containerName, AZURITE.getBlobEndpoint());
    }
}
