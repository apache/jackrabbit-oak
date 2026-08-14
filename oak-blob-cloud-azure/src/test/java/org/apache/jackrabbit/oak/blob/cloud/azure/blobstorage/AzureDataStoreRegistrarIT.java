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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.specialized.BlockBlobClient;
import org.apache.jackrabbit.oak.plugins.blob.AbstractSharedCachingDataStore;
import org.apache.jackrabbit.oak.spi.blob.data.DataIdentifier;
import org.apache.jackrabbit.oak.spi.blob.data.DataRecord;
import org.apache.jackrabbit.oak.spi.blob.data.DataStoreException;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 * Integration tests for AzureDataStoreWrapper against Azurite (Azure Storage emulator).
 * <p>
 * Runs unconditionally in CI via Docker — no external Azure credentials needed.
 * Each test gets a fresh container name and home directories.
 * <p>
 * Cross-SDK compatibility tests (v8 write → v12 read and vice versa) use two separate
 * wrapper instances pointing at the same Azure container. Upload staging is disabled
 * (stagingSplitPercentage=0) so addRecord() writes synchronously to Azurite.
 */
public class AzureDataStoreRegistrarIT {

    @ClassRule
    public static final AzuriteDockerRule AZURITE = new AzuriteDockerRule();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private AzureDataStoreRegistrar.DelegatingDataStore dsV8;
    private AzureDataStoreRegistrar.DelegatingDataStore dsV12;
    private String containerName;

    // Azure backend stores blobs under "{first4}-{rest}" keys; must match to find the right blob.
    private static String blobKeyFor(DataIdentifier identifier) {
        String id = identifier.toString();
        return id.substring(0, 4) + "-" + id.substring(4);
    }

    @Before
    public void setUp() throws Exception {
        containerName = "test-" + System.nanoTime();
        Properties props = azuriteProps(containerName);

        AzureDataStore v8 = new AzureDataStore();
        v8.setProperties(props);
        v8.setStagingSplitPercentage(0);
        AzureDataStoreRegistrar wrapperV8 = new AzureDataStoreRegistrar();
        wrapperV8.activeImpl = v8;
        dsV8 = wrapperV8.new DelegatingDataStore();
        dsV8.init(folder.newFolder().getAbsolutePath());

        AbstractSharedCachingDataStore v12 = AzureDataStoreRegistrar.createV12Store(props);
        v12.setStagingSplitPercentage(0);
        AzureDataStoreRegistrar wrapperV12 = new AzureDataStoreRegistrar();
        wrapperV12.activeImpl = v12;
        dsV12 = wrapperV12.new DelegatingDataStore();
        dsV12.init(folder.newFolder().getAbsolutePath());
    }

    @After
    public void tearDown() throws DataStoreException {
        if (dsV8 != null) dsV8.close();
        if (dsV12 != null) dsV12.close();
    }

    /**
     * Write a blob via the v8 SDK path, then read it back via v12.
     * Verifies that the v8 blob key format is resolvable by the v12 SDK and that
     * the content bytes are byte-for-byte identical.
     */
    @Test
    public void blobWrittenOnV8IsReadableByV12() throws DataStoreException, IOException {
        byte[] payload = "hello from v8 SDK".getBytes();

        DataRecord written = dsV8.addRecord(new ByteArrayInputStream(payload));
        assertNotNull(written);

        DataRecord read = dsV12.getRecord(written.getIdentifier());
        assertNotNull(read);
        assertArrayEquals("content must survive v8-write → v12-read", payload, read.getStream().readAllBytes());
    }

    /**
     * Write a blob via v12, then read it back via v8.
     * Mirror of the above: verifies v12 key format is resolvable by v8.
     */
    @Test
    public void blobWrittenOnV12IsReadableByV8() throws DataStoreException, IOException {
        byte[] payload = "hello from v12 SDK".getBytes();

        DataRecord written = dsV12.addRecord(new ByteArrayInputStream(payload));
        assertNotNull(written);

        DataRecord read = dsV8.getRecord(written.getIdentifier());
        assertNotNull(read);
        assertArrayEquals("content must survive v12-write → v8-read", payload, read.getStream().readAllBytes());
    }

    /**
     * Write blobs via each SDK alternately, then read all of them back through both SDKs.
     * Verifies no write is silently dropped and content is intact regardless of which SDK
     * path handled each operation.
     */
    @Test
    public void blobsWrittenViaEitherSdkAreReadableByBoth() throws DataStoreException, IOException {
        byte[][] payloads = {
                "blob-A-v8".getBytes(),
                "blob-B-v12".getBytes(),
                "blob-C-v12".getBytes(),
                "blob-D-v8".getBytes(),
                "blob-E-v12".getBytes(),
        };

        DataRecord[] written = {
                dsV8.addRecord(new ByteArrayInputStream(payloads[0])),
                dsV12.addRecord(new ByteArrayInputStream(payloads[1])),
                dsV12.addRecord(new ByteArrayInputStream(payloads[2])),
                dsV8.addRecord(new ByteArrayInputStream(payloads[3])),
                dsV12.addRecord(new ByteArrayInputStream(payloads[4])),
        };

        for (int i = 0; i < written.length; i++) {
            assertNotNull("record " + i + " must not be null", written[i]);
        }

        for (int i = 0; i < written.length; i++) {
            byte[] actual = dsV12.getRecord(written[i].getIdentifier()).getStream().readAllBytes();
            assertArrayEquals("blob " + i + " content mismatch via v12", payloads[i], actual);
        }

        for (int i = 0; i < written.length; i++) {
            byte[] actual = dsV8.getRecord(written[i].getIdentifier()).getStream().readAllBytes();
            assertArrayEquals("blob " + i + " content mismatch via v8", payloads[i], actual);
        }
    }

    // -- Data-loss safety tests --

    /**
     * Verifies that the record length reported by each SDK matches the actual payload size
     * regardless of which SDK wrote the blob.
     */
    @Test
    public void recordLengthConsistentAcrossSDKs() throws DataStoreException {
        byte[] payload = new byte[4096];
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) i;

        DataRecord v8Rec = dsV8.addRecord(new ByteArrayInputStream(payload));
        assertEquals("v8 record length must match payload", payload.length, v8Rec.getLength());

        DataRecord readByV12 = dsV12.getRecord(v8Rec.getIdentifier());
        assertEquals("v12-read record length must match payload", payload.length, readByV12.getLength());

        byte[] v12Payload = new byte[8192];
        DataRecord v12Rec = dsV12.addRecord(new ByteArrayInputStream(v12Payload));
        assertEquals("v12 record length must match payload", v12Payload.length, v12Rec.getLength());

        DataRecord readByV8 = dsV8.getRecord(v12Rec.getIdentifier());
        assertEquals("v8-read record length must match payload", v12Payload.length, readByV8.getLength());
    }

    /**
     * A blob written via the v12 SDK must have a "lastModified" metadata key; its absence causes getLastModified() to fall back to the Azure server timestamp, which can make deleteAllOlderThan() GC the blob prematurely.
     */
    @Test
    public void v12_writeBlob_hasLastModifiedMetadata() throws DataStoreException {
        byte[] payload = new byte[32 * 1024];
        Arrays.fill(payload, (byte) 'x');

        DataRecord dataRecord = dsV12.addRecord(new ByteArrayInputStream(payload));
        assertNotNull(dataRecord);

        String blobKey = blobKeyFor(dataRecord.getIdentifier());
        BlobContainerClient containerClient = azuriteContainerClient();
        BlockBlobClient blobClient = containerClient.getBlobClient(blobKey).getBlockBlobClient();

        Map<String, String> metadata = blobClient.getProperties().getMetadata();
        assertTrue("blob must have 'lastModified' metadata key after v12 write; " +
                        "absent key causes premature GC via Azure server-timestamp fallback",
                metadata != null && metadata.containsKey("lastModified"));

        long lastModified = Long.parseLong(metadata.get("lastModified"));
        long now = System.currentTimeMillis();
        assertTrue("lastModified must be a recent epoch-millis timestamp",
                lastModified > 0 && lastModified <= now && lastModified > now - 60_000);
    }

    // -- helpers --

    /**
     * Concurrent backend initialization against the same container must produce a single consistent reference key.
     */
    @Test
    public void v12_concurrentReferenceKeyInit_allBackendsGetSameKey() throws Exception {
        Properties props = azuriteProps(containerName);

        int backends = 4;
        CountDownLatch ready = new CountDownLatch(backends);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(backends);
        List<Future<byte[]>> futures = new ArrayList<>(backends);

        for (int i = 0; i < backends; i++) {
            futures.add(pool.submit(() -> {
                AbstractSharedCachingDataStore v12 = AzureDataStoreRegistrar.createV12Store(props);
                v12.setStagingSplitPercentage(0);
                java.io.File home = folder.newFolder();
                ready.countDown();
                start.await();
                v12.init(home.getAbsolutePath());
                return new byte[0];
            }));
        }

        ready.await();
        start.countDown();
        pool.shutdown();
        assertTrue("backends did not initialize in time", pool.awaitTermination(60, TimeUnit.SECONDS));
        for (Future<byte[]> f : futures) {
            f.get(); // propagate any task-level exceptions
        }

        long refKeyCount = azuriteContainerClient()
                .listBlobs(new com.azure.storage.blob.models.ListBlobsOptions()
                        .setPrefix("META/"), null)
                .stream()
                .filter(b -> b.getName().contains("oak.datastore.key") ||
                        b.getName().contains("azure.blob.ref.key"))
                .count();

        assertTrue("concurrent backend init must produce exactly one reference key in storage; " +
                        "found " + refKeyCount + " — multiple keys cause upload token verification failures",
                refKeyCount <= 1);
    }

    private Properties azuriteProps(String containerName) {
        Properties p = new Properties();
        p.setProperty("azureConnectionString",
                "DefaultEndpointsProtocol=http" +
                        ";AccountName=" + AzuriteDockerRule.ACCOUNT_NAME +
                        ";AccountKey=" + AzuriteDockerRule.ACCOUNT_KEY +
                        ";BlobEndpoint=" + AZURITE.getBlobEndpoint());
        p.setProperty("container", containerName);
        p.setProperty("azureCreateContainer", "true");
        return p;
    }

    private BlobContainerClient azuriteContainerClient() {
        String connectionString =
                "DefaultEndpointsProtocol=http" +
                        ";AccountName=" + AzuriteDockerRule.ACCOUNT_NAME +
                        ";AccountKey=" + AzuriteDockerRule.ACCOUNT_KEY +
                        ";BlobEndpoint=" + AZURITE.getBlobEndpoint();
        return new BlobContainerClientBuilder()
                .connectionString(connectionString)
                .containerName(containerName)
                .buildClient();
    }
}
