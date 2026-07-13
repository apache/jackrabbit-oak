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
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.sas.BlobContainerSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzuriteDockerRule;
import org.apache.jackrabbit.oak.spi.blob.data.DataIdentifier;
import org.apache.jackrabbit.oak.spi.blob.data.DataStoreException;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.OffsetDateTime;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration tests for AzureBlobStoreBackendV12 authentication paths.
 * Verifies that connection string, SAS token (read-write, read-only, expired) auth
 * behave correctly — mirrors AzureBlobStoreBackendTest for the v12 backend.
 * <p>
 * Uses Azurite (Microsoft's open-source Azure Storage emulator) running in Docker
 * instead of real Azure, so the tests need no external credentials and run offline.
 */
public class AzureBlobStoreBackendV12AuthIT {

    // Azurite Docker container shared across all tests in this class; starting it once
    // keeps the suite fast. The container is torn down after the last test completes.
    @ClassRule
    public static final AzuriteDockerRule AZURITE = new AzuriteDockerRule();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private BlobContainerClient adminContainer;
    private String containerName;

    private static BlobContainerSasPermission readWritePermissions() {
        return new BlobContainerSasPermission()
                .setReadPermission(true)
                .setWritePermission(true)
                .setCreatePermission(true)
                .setDeletePermission(true)
                .setListPermission(true)
                .setAddPermission(true);
    }

    private static BlobContainerSasPermission readOnlyPermissions() {
        return new BlobContainerSasPermission()
                .setReadPermission(true)
                .setListPermission(true);
    }

    @Before
    public void setUp() {
        // Unique per test so auth failures in one test can't bleed into another.
        containerName = "auth-" + System.nanoTime();
        adminContainer = new BlobContainerClientBuilder()
                .connectionString(azuriteConnectionString())
                .containerName(containerName)
                .buildClient();
        adminContainer.create();
    }

    @After
    public void tearDown() {
        if (adminContainer != null) {
            try {
                adminContainer.deleteIfExists();
            } catch (Exception ignore) {
                // best-effort cleanup; ignore failures during test teardown
            }
        }
    }

    /**
     * Connection string is the primary auth path — backend must init, write, and persist a reference key.
     */
    @Test
    public void initWithConnectionString_writesAndPersistsReferenceKey() throws DataStoreException {
        AzureBlobStoreBackendV12 backend = new AzureBlobStoreBackendV12();
        backend.setProperties(connectionStringProps());
        backend.init();

        byte[] key1 = backend.getOrCreateReferenceKey();
        byte[] key2 = backend.getOrCreateReferenceKey();

        assertNotNull("connection string auth must produce a reference key", key1);
        assertTrue("reference key must be non-empty", key1.length > 0);
        assertArrayEquals("reference key must be stable across calls", key1, key2);
    }

    /**
     * Connection string auth must allow writes — write a blob and verify it's retrievable.
     */
    @Test
    public void initWithConnectionString_writeAndReadBlob() throws DataStoreException, IOException {
        AzureBlobStoreBackendV12 backend = new AzureBlobStoreBackendV12();
        backend.setProperties(connectionStringProps());
        backend.init();

        File file = writeTempFile("connection string write test");
        backend.write(new DataIdentifier("conntest1"), file);

        // getKeyName("conntest1") produces "conn-test1" (4-char prefix + dash + remainder)
        assertTrue("written blob must exist in storage",
                adminContainer.listBlobs().stream()
                        .anyMatch(b -> b.getName().contains("conn-test1")));
    }

    /**
     * Read-write SAS must allow init and write — the backend needs to create the reference key on first run.
     */
    @Test
    public void initWithSasToken_readWrite_canInitAndWrite() throws DataStoreException {
        String sasToken = generateContainerSas(readWritePermissions(), OffsetDateTime.now().plusHours(1));

        AzureBlobStoreBackendV12 backend = new AzureBlobStoreBackendV12();
        backend.setProperties(sasProps(sasToken));
        backend.init();

        byte[] key = backend.getOrCreateReferenceKey();
        assertNotNull("read-write SAS must allow reference key creation", key);
        assertTrue("reference key must be non-empty", key.length > 0);
    }

    /**
     * Read-only SAS must reject writes — a write attempt must throw DataStoreException, not silently succeed.
     */
    @Test
    public void initWithSasToken_readOnly_writeThrowsDataStoreException()
            throws DataStoreException, IOException {
        // Pre-populate the reference key so that init() can read it without needing write access.
        AzureBlobStoreBackendV12 adminBackend = new AzureBlobStoreBackendV12();
        adminBackend.setProperties(connectionStringProps());
        adminBackend.init();

        String sasToken = generateContainerSas(readOnlyPermissions(), OffsetDateTime.now().plusHours(1));

        AzureBlobStoreBackendV12 backend = new AzureBlobStoreBackendV12();
        backend.setProperties(sasProps(sasToken));
        backend.init();

        File file = writeTempFile("read only test");
        try {
            backend.write(new DataIdentifier("readonly1"), file);
            fail("write with read-only SAS must throw DataStoreException");
        } catch (DataStoreException expected) {
            // correct: SAS does not grant write permission
        }
    }

    /**
     * Expired SAS must reject all access — init must throw rather than silently succeeding with a stale token.
     * BlobStorageException (RuntimeException) propagates unwrapped through withAzureSDKContext.
     */
    @Test
    public void initWithExpiredSasToken_initThrowsDataStoreException() throws DataStoreException {
        String sasToken = generateContainerSas(readWritePermissions(), OffsetDateTime.now().minusSeconds(60));

        AzureBlobStoreBackendV12 backend = new AzureBlobStoreBackendV12();
        backend.setProperties(sasProps(sasToken));
        try {
            backend.init();
            fail("init with expired SAS must throw");
        } catch (DataStoreException | RuntimeException ignored) {
            // expected: expired SAS causes auth failure
        }
    }

    /**
     * Reference key must be the same on repeated init — a diverged key would corrupt upload-token signing.
     */
    @Test
    public void initSecret_referenceKeyStableAcrossReinit() throws DataStoreException {
        AzureBlobStoreBackendV12 b1 = new AzureBlobStoreBackendV12();
        b1.setProperties(connectionStringProps());
        b1.init();
        byte[] key1 = b1.getOrCreateReferenceKey();

        AzureBlobStoreBackendV12 b2 = new AzureBlobStoreBackendV12();
        b2.setProperties(connectionStringProps());
        b2.init();
        byte[] key2 = b2.getOrCreateReferenceKey();

        assertArrayEquals("reference key must be identical across backend re-inits", key1, key2);
    }

    private String azuriteConnectionString() {
        return "DefaultEndpointsProtocol=http" +
                ";AccountName=" + AzuriteDockerRule.ACCOUNT_NAME +
                ";AccountKey=" + AzuriteDockerRule.ACCOUNT_KEY +
                ";BlobEndpoint=" + AZURITE.getBlobEndpoint();
    }

    private Properties connectionStringProps() {
        Properties p = new Properties();
        p.setProperty(AzureConstantsV12.AZURE_CONNECTION_STRING, azuriteConnectionString());
        p.setProperty(AzureConstantsV12.AZURE_BLOB_CONTAINER_NAME, containerName);
        return p;
    }

    private Properties sasProps(String sasToken) {
        Properties p = new Properties();
        p.setProperty(AzureConstantsV12.AZURE_SAS, sasToken);
        p.setProperty(AzureConstantsV12.AZURE_BLOB_ENDPOINT, AZURITE.getBlobEndpoint());
        p.setProperty(AzureConstantsV12.AZURE_STORAGE_ACCOUNT_NAME, AzuriteDockerRule.ACCOUNT_NAME);
        p.setProperty(AzureConstantsV12.AZURE_BLOB_CONTAINER_NAME, containerName);
        // container already exists from @Before; skip the exists()/create() check that Azurite
        // doesn't permit under container-scoped SAS tokens
        p.setProperty(AzureConstantsV12.AZURE_CREATE_CONTAINER, "false");
        return p;
    }

    private String generateContainerSas(BlobContainerSasPermission permissions, OffsetDateTime expiry) {
        BlobServiceSasSignatureValues values = new BlobServiceSasSignatureValues(expiry, permissions);
        return adminContainer.generateSas(values);
    }

    private File writeTempFile(String content) throws IOException {
        File f = folder.newFile();
        Files.write(f.toPath(), content.getBytes());
        return f;
    }
}
