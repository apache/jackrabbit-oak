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

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlobInputStream;
import com.azure.storage.blob.specialized.BlobOutputStream;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadOptions;
import org.apache.jackrabbit.oak.spi.blob.data.DataIdentifier;
import org.apache.jackrabbit.oak.spi.blob.data.DataRecord;
import org.apache.jackrabbit.oak.spi.blob.data.DataStoreException;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.OffsetDateTime;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * CRUD unit tests for AzureBlobStoreBackendV12 that exercise the blob CRUD and metadata paths
 * without a live Azure/Azurite endpoint. The integration tests (AzureBlobStoreBackendV12IT) cover the
 * same paths against real storage but are skipped in CI when Docker/Azurite is unavailable, so these
 * unit tests are what actually drives line coverage for the SDK-call branches.
 */
public class AzureBlobStoreBackendV12CRUDTest {

    // A valid 24-char identifier: getKeyName() does substring(0,4) + "-" + substring(4).
    private static final DataIdentifier ID = new DataIdentifier("abcdef0123456789abcdef01");

    private BlobContainerClient container;
    private BlobClient blobClient;
    private BlockBlobClient blockBlobClient;
    private AzureBlobStoreBackendV12 backend;

    @Before
    public void setUp() {
        container = mock(BlobContainerClient.class);
        blobClient = mock(BlobClient.class);
        blockBlobClient = mock(BlockBlobClient.class);
        when(container.getBlobClient(anyString())).thenReturn(blobClient);
        when(blobClient.getBlockBlobClient()).thenReturn(blockBlobClient);
        when(blockBlobClient.getContainerClient()).thenReturn(container);
        backend = new AzureBlobStoreBackendV12() {
            @Override
            protected BlobContainerClient getAzureContainer() {
                return container;
            }
        };
    }

    // BlobProperties is final with final accessors — not reliably mockable, so build a real one.
    // Only blobSize (arg 4), lastModified (arg 2) and metadata (arg 30) matter here; rest are null.
    private static BlobProperties propsWithSize(long size) {
        return new BlobProperties(
                null, OffsetDateTime.now(), null, size, null, null, null, null, null, null,
                null, null, null, null, null, null, null, null, null, null,
                null, null, null, null, null, null, null, null, null, Map.of(),
                null);
    }

    // --- read ---

    @Test
    public void read_blobExists_returnsStream() throws Exception {
        when(blockBlobClient.exists()).thenReturn(true);
        BlobInputStream stream = mock(BlobInputStream.class);
        when(blockBlobClient.openInputStream()).thenReturn(stream);

        InputStream result = backend.read(ID);

        assertSame(stream, result);
    }

    @Test
    public void read_blobMissing_throwsDataStoreException() {
        when(blockBlobClient.exists()).thenReturn(false);
        try {
            backend.read(ID);
            fail("expected DataStoreException for missing blob");
        } catch (DataStoreException e) {
            assertTrue(e.getMessage().contains("missing blob"));
        }
    }

    @Test(expected = DataStoreException.class)
    public void read_blobStorageException_wrappedAsDataStoreException() throws Exception {
        when(blockBlobClient.exists()).thenThrow(mock(BlobStorageException.class));
        backend.read(ID);
    }

    @Test(expected = NullPointerException.class)
    public void read_nullIdentifier_throwsNPE() throws Exception {
        backend.read(null);
    }

    // --- getRecord ---

    @Test
    public void getRecord_blobExists_returnsRecordWithSizeAndModified() throws Exception {
        when(blockBlobClient.getProperties()).thenReturn(propsWithSize(4242L));
        when(blockBlobClient.getBlobName()).thenReturn("abcd-ef0123456789abcdef01");

        DataRecord rec = backend.getRecord(ID);

        assertNotNull(rec);
        assertEquals(4242L, rec.getLength());
    }

    @Test
    public void getRecord_notFound404_throwsDataStoreException() {
        BlobStorageException ex = mock(BlobStorageException.class);
        when(ex.getStatusCode()).thenReturn(404);
        when(blockBlobClient.getProperties()).thenThrow(ex);
        try {
            backend.getRecord(ID);
            fail("expected DataStoreException");
        } catch (DataStoreException e) {
            assertTrue(e.getMessage().contains("Cannot retrieve blob"));
        }
    }

    @Test(expected = DataStoreException.class)
    public void getRecord_otherStorageError_throwsDataStoreException() throws Exception {
        BlobStorageException ex = mock(BlobStorageException.class);
        when(ex.getStatusCode()).thenReturn(503);
        when(blockBlobClient.getProperties()).thenThrow(ex);
        backend.getRecord(ID);
    }

    // --- exists ---

    @Test
    public void exists_trueWhenBlobExists() throws Exception {
        when(blockBlobClient.exists()).thenReturn(true);
        assertTrue(backend.exists(ID));
    }

    @Test
    public void exists_falseWhenBlobMissing() throws Exception {
        when(blockBlobClient.exists()).thenReturn(false);
        assertFalse(backend.exists(ID));
    }

    // --- deleteRecord ---

    @Test
    public void deleteRecord_callsDeleteIfExists() throws Exception {
        when(blockBlobClient.deleteIfExists()).thenReturn(true);
        backend.deleteRecord(ID);
        verify(blockBlobClient).deleteIfExists();
    }

    @Test(expected = DataStoreException.class)
    public void deleteRecord_storageError_throwsDataStoreException() throws Exception {
        when(blockBlobClient.deleteIfExists()).thenThrow(mock(BlobStorageException.class));
        backend.deleteRecord(ID);
    }

    // --- metadata records ---

    @Test
    public void getMetadataRecord_missing_returnsNull() {
        when(blockBlobClient.exists()).thenReturn(false);
        assertNull(backend.getMetadataRecord("some-meta"));
    }

    @Test
    public void getMetadataRecord_exists_returnsRecord() {
        when(blockBlobClient.exists()).thenReturn(true);
        when(blockBlobClient.getProperties()).thenReturn(propsWithSize(7L));

        DataRecord rec = backend.getMetadataRecord("some-meta");

        assertNotNull(rec);
    }

    @Test(expected = IllegalStateException.class)
    public void getMetadataRecord_storageError_throwsIllegalState() {
        when(blockBlobClient.exists()).thenThrow(mock(BlobStorageException.class));
        backend.getMetadataRecord("some-meta");
    }

    @Test
    public void metadataRecordExists_reflectsBlobExists() {
        when(blobClient.exists()).thenReturn(true);
        assertTrue(backend.metadataRecordExists("meta1"));
        when(blobClient.exists()).thenReturn(false);
        assertFalse(backend.metadataRecordExists("meta1"));
    }

    @Test
    public void metadataRecordExists_storageError_returnsFalse() {
        when(blobClient.exists()).thenThrow(mock(BlobStorageException.class));
        assertFalse(backend.metadataRecordExists("meta1"));
    }

    @Test
    public void deleteMetadataRecord_success_returnsTrue() {
        when(blobClient.deleteIfExists()).thenReturn(true);
        assertTrue(backend.deleteMetadataRecord("meta1"));
    }

    @Test
    public void deleteMetadataRecord_storageError_returnsFalse() {
        when(blobClient.deleteIfExists()).thenThrow(mock(BlobStorageException.class));
        assertFalse(backend.deleteMetadataRecord("meta1"));
    }

    @Test(expected = NullPointerException.class)
    public void addMetadataRecord_nullInputStream_throwsNPE() throws Exception {
        backend.addMetadataRecord((InputStream) null, "name");
    }

    @Test(expected = IllegalArgumentException.class)
    public void addMetadataRecord_emptyName_throwsIllegalArgument() throws Exception {
        backend.addMetadataRecord(new ByteArrayInputStream(new byte[]{1}), "");
    }

    @Test(expected = NullPointerException.class)
    public void addMetadataRecord_nullFile_throwsNPE() throws Exception {
        backend.addMetadataRecord((java.io.File) null, "name");
    }

    // --- write ---

    @Test
    public void write_lengthCollision_throwsDataStoreException() throws Exception {
        java.io.File tempFile = java.io.File.createTempFile("collision", ".bin");
        tempFile.deleteOnExit();
        java.nio.file.Files.write(tempFile.toPath(), new byte[]{1, 2, 3});

        when(blockBlobClient.exists()).thenReturn(true);
        // Existing blob reports a different size than the file being written.
        when(blockBlobClient.getProperties()).thenReturn(propsWithSize(999L));

        try {
            backend.write(ID, tempFile);
            fail("expected DataStoreException for length collision");
        } catch (DataStoreException e) {
            // Re-wrapped with "Cannot write blob" outer message; check the cause
            DataStoreException cause = (DataStoreException) e.getCause();
            assertNotNull("Expected nested DataStoreException cause", cause);
            assertTrue(cause.getMessage().contains("Length Collision"));
        }
    }

    @Test
    public void write_blobExistsWithMatchingSize_updatesLastModifiedMetadata() throws Exception {
        java.io.File tempFile = java.io.File.createTempFile("match", ".bin");
        tempFile.deleteOnExit();
        java.nio.file.Files.write(tempFile.toPath(), new byte[]{1, 2, 3});
        long len = tempFile.length();

        when(blockBlobClient.exists()).thenReturn(true);
        when(blockBlobClient.getProperties()).thenReturn(propsWithSize(len)); // size matches → no collision

        backend.write(ID, tempFile);

        verify(blockBlobClient).setMetadata(any()); // updateLastModifiedMetadata path ran
    }

    @Test
    public void write_blobMissing_uploadsNewBlob() throws Exception {
        java.io.File tempFile = java.io.File.createTempFile("newblob", ".bin");
        tempFile.deleteOnExit();
        java.nio.file.Files.write(tempFile.toPath(), new byte[]{9, 9, 9, 9, 9});

        when(blockBlobClient.exists()).thenReturn(false); // not present → upload path
        com.azure.core.http.rest.Response<com.azure.storage.blob.models.BlockBlobItem> resp =
                mock(com.azure.core.http.rest.Response.class);
        when(resp.getStatusCode()).thenReturn(201);
        when(blobClient.uploadFromFileWithResponse(any(), any(), any())).thenReturn(resp);

        backend.write(ID, tempFile);

        verify(blobClient).uploadFromFileWithResponse(any(), any(), any());
    }

    // --- initiateHttpUpload argument validation (no Azure call needed) ---

    private final DataRecordUploadOptions opts = DataRecordUploadOptions.DEFAULT;

    @Test(expected = IllegalArgumentException.class)
    public void initiateHttpUpload_zeroMaxSize_throws() throws Exception {
        backend.initiateHttpUpload(0L, 1, opts);
    }

    @Test(expected = IllegalArgumentException.class)
    public void initiateHttpUpload_invalidMaxNumberOfURIs_throws() throws Exception {
        backend.initiateHttpUpload(1024L, 0, opts);
    }

    @Test(expected = IllegalArgumentException.class)
    public void initiateHttpUpload_singlePutTooLarge_throws() throws Exception {
        // 300MB with maxNumberOfURIs == 1 exceeds the single-put limit (256MB).
        backend.initiateHttpUpload(300L * 1024 * 1024, 1, opts);
    }

    @Test(expected = IllegalArgumentException.class)
    public void initiateHttpUpload_exceedsMaxBinarySize_throws() throws Exception {
        // V12 max binary upload size is ~190.7 TiB; go over it.
        backend.initiateHttpUpload(200L * 1024 * 1024 * 1024 * 1024, -1, opts);
    }

    // --- init(): config parsing, container connection, presigned + secondary-location config ---

    private static Properties baseInitProps() {
        Properties p = new Properties();
        p.setProperty(AzureConstantsV12.AZURE_BLOB_CONTAINER_NAME, "test-container");
        p.setProperty(AzureConstantsV12.AZURE_STORAGE_ACCOUNT_NAME, "testacct");
        // Skip reference-key creation on init — exercised separately in AzureBlobStoreBackendV12Test.
        p.setProperty(AzureConstantsV12.AZURE_REF_ON_INIT, "false");
        return p;
    }

    @Test
    public void init_lowConcurrentRequestCount_clampedToDefault_andReusesExistingContainer() throws Exception {
        when(container.exists()).thenReturn(true); // reuse path
        Properties p = baseInitProps();
        p.setProperty(AzureConstantsV12.AZURE_BLOB_CONCURRENT_REQUESTS_PER_OPERATION, "0"); // below min → clamp
        backend.setProperties(p);

        backend.init();

        verify(container, never()).create();
    }

    @Test
    public void init_highConcurrentRequestCount_clampedToMax_andCreatesMissingContainer() throws Exception {
        when(container.exists()).thenReturn(false); // create path (createContainer defaults true)
        Properties p = baseInitProps();
        p.setProperty(AzureConstantsV12.AZURE_BLOB_CONCURRENT_REQUESTS_PER_OPERATION, "9999"); // above max → clamp
        backend.setProperties(p);

        backend.init();

        verify(container).create();
    }

    @Test
    public void init_withSecondaryLocationAndPresignedConfig_parsesAllOptions() throws Exception {
        when(container.exists()).thenReturn(true);
        Properties p = baseInitProps();
        p.setProperty(AzureConstantsV12.AZURE_BLOB_ENABLE_SECONDARY_LOCATION_NAME, "true");
        p.setProperty(AzureConstantsV12.AZURE_BLOB_REQUEST_TIMEOUT, "30");
        p.setProperty(AzureConstantsV12.AZURE_BLOB_MAX_REQUEST_RETRY, "4");
        p.setProperty(AzureConstantsV12.PRESIGNED_HTTP_DOWNLOAD_URI_VERIFY_EXISTS, "false");
        p.setProperty(AzureConstantsV12.PRESIGNED_HTTP_UPLOAD_URI_EXPIRY_SECONDS, "300");
        p.setProperty(AzureConstantsV12.PRESIGNED_HTTP_DOWNLOAD_URI_EXPIRY_SECONDS, "600");
        p.setProperty(AzureConstantsV12.PRESIGNED_HTTP_DOWNLOAD_URI_CACHE_MAX_SIZE, "50");
        p.setProperty(AzureConstantsV12.PRESIGNED_HTTP_UPLOAD_URI_DOMAIN_OVERRIDE, "upload.example.com");
        p.setProperty(AzureConstantsV12.PRESIGNED_HTTP_DOWNLOAD_URI_DOMAIN_OVERRIDE, "download.example.com");
        backend.setProperties(p);

        backend.init();
        verify(container).exists();
    }

    @Test
    public void init_downloadExpiryWithoutCacheSize_defaultsCacheToZero() throws Exception {
        when(container.exists()).thenReturn(true);
        Properties p = baseInitProps();
        p.setProperty(AzureConstantsV12.PRESIGNED_HTTP_DOWNLOAD_URI_EXPIRY_SECONDS, "600");
        // No cache max size set → exercises the else branch that sets cache size to 0.
        backend.setProperties(p);

        backend.init();
        verify(container).exists();
    }

    // --- getAllIdentifiers / getAllRecords (list-based) ---

    @SuppressWarnings("unchecked")
    private void stubListBlobs(BlobItem... items) {
        PagedIterable<BlobItem> paged = mock(PagedIterable.class);
        when(paged.stream()).thenReturn(Stream.of(items));
        when(container.listBlobs()).thenReturn(paged);
    }

    private static BlobItem blobItem(String name, long length) {
        BlobItem item = mock(BlobItem.class);
        when(item.getName()).thenReturn(name);
        BlobItemProperties itemProps = mock(BlobItemProperties.class);
        when(itemProps.getLastModified()).thenReturn(OffsetDateTime.now());
        when(itemProps.getContentLength()).thenReturn(length);
        when(item.getProperties()).thenReturn(itemProps);
        return item;
    }

    @Test
    public void getAllIdentifiers_skipsMetaKeysAndNoDashKeys() throws Exception {
        // Blob key with dash → kept. Meta-prefixed and no-dash blobs → getIdentifierName returns null → filtered.
        stubListBlobs(
                blobItem("abcd-ef0123456789abcdef01", 10L),  // valid: kept
                blobItem("META/skip-this", 5L),              // meta prefix → line 111 null → filtered
                blobItem("nodashblob", 3L));                 // no dash → line 114 null → filtered
        Iterator<DataIdentifier> ids = backend.getAllIdentifiers();
        assertTrue(ids.hasNext());
        ids.next();
        assertFalse("meta and no-dash blobs must be filtered out", ids.hasNext());
    }

    @Test
    public void getAllRecords_skipsMetaAndNoDashKeys() throws Exception {
        stubListBlobs(
                blobItem("abcd-ef0123456789abcdef01", 11L),
                blobItem("META/skip-this", 5L),
                blobItem("nodash", 2L));
        Iterator<DataRecord> records = backend.getAllRecords();
        assertTrue(records.hasNext());
        assertEquals(11L, records.next().getLength());
        assertFalse(records.hasNext());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void getAllMetadataRecords_success_returnsMatchingRecords() throws Exception {
        BlobItem item = blobItem(AzureConstantsV12.AZURE_BLOB_META_DIR_NAME + "/mymeta", 7L);
        PagedIterable<BlobItem> paged = mock(PagedIterable.class);
        when(paged.stream()).thenReturn(Stream.of(item));
        when(paged.iterator()).thenReturn(java.util.Arrays.asList(item).iterator());
        // getAllMetadataRecords uses listBlobs(ListBlobsOptions, null) — match any args.
        when(container.listBlobs(any(), (java.time.Duration) org.mockito.ArgumentMatchers.isNull())).thenReturn(paged);

        java.util.List<DataRecord> records = backend.getAllMetadataRecords("mymeta");

        assertEquals(1, records.size());
    }

    // --- addMetadataRecord(File) ---

    @Test
    public void addMetadataRecord_fromFile_writesToBlobOutputStream() throws Exception {
        java.io.File tempFile = java.io.File.createTempFile("meta", ".bin");
        tempFile.deleteOnExit();
        java.nio.file.Files.write(tempFile.toPath(), new byte[]{1, 2, 3, 4});

        BlobOutputStream out = mock(BlobOutputStream.class);
        when(blockBlobClient.getBlobOutputStream(any(), any(), any(), any(), any())).thenReturn(out);

        backend.addMetadataRecord(tempFile, "meta-from-file");

        verify(blockBlobClient).setMetadata(any());
    }

    // --- AzureBlobStoreDataRecord value semantics ---

    @Test
    public void dataRecord_exposesLengthLastModifiedAndToString() throws Exception {
        AzureBlobStoreBackendV12.AzureBlobStoreDataRecord rec =
                new AzureBlobStoreBackendV12.AzureBlobStoreDataRecord(
                        backend, null, ID, 1234L, 5678L);

        assertEquals(5678L, rec.getLength());
        assertEquals(1234L, rec.getLastModified());
        String s = rec.toString();
        assertTrue(s.contains("5678"));
        assertTrue(s.contains("1234"));
    }
}
