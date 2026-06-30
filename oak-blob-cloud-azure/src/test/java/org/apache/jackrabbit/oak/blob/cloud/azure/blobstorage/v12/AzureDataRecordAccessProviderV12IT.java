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

    private Properties azuriteProps(String containerName) {
        // AZURE_BLOB_ENDPOINT required so getDefaultBlobStorageDomain() can resolve a non-null domain for SAS URI generation
        return AzuriteV12TestUtils.azuriteProps(containerName, AZURITE.getBlobEndpoint());
    }
}
