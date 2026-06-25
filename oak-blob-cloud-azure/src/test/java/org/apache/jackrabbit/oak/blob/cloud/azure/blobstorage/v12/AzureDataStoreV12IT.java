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

import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzuriteDockerRule;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Integration tests for AzureDataStoreV12 CRUD, deduplication, and GC via Azurite.
 * Mirrors TestAzureDS / AzureDataStoreTest for the v12 backend.
 */
public class AzureDataStoreV12IT {

    @ClassRule
    public static final AzuriteDockerRule AZURITE = new AzuriteDockerRule();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private AzureDataStoreV12 store;

    @Before
    public void setUp() throws DataStoreException, IOException {
        store = new AzureDataStoreV12();
        store.setProperties(azuriteProps("v12ds-" + System.nanoTime()));
        // 0% staging so all writes go directly to Azure — avoids local-staging code paths masking backend failures.
        store.setStagingSplitPercentage(0);
        store.init(folder.newFolder().getAbsolutePath());
    }

    @After
    public void tearDown() {
        try {
            store.close();
        } catch (Exception ignore) {
        }
    }

    /**
     * addRecord must return a record with correct length and a non-empty identifier.
     */
    @Test
    public void testAddRecord() throws DataStoreException, IOException {
        byte[] data = "hello world".getBytes();
        DataRecord record = store.addRecord(new ByteArrayInputStream(data));

        assertNotNull("record must be returned", record);
        assertEquals("record length must match input", data.length, record.getLength());
        assertFalse("record ID must be non-empty", record.getIdentifier().toString().isEmpty());
    }

    /**
     * getRecord must return the same content and length as what was written.
     */
    @Test
    public void testGetRecord() throws DataStoreException, IOException {
        byte[] data = "test data for get".getBytes();
        DataRecord added = store.addRecord(new ByteArrayInputStream(data));

        DataRecord fetched = store.getRecord(added.getIdentifier());

        assertNotNull("getRecord must not return null for an existing record", fetched);
        assertEquals("fetched record length must match original", data.length, fetched.getLength());
        assertEquals("fetched record ID must match", added.getIdentifier(), fetched.getIdentifier());
    }

    /**
     * getRecordIfStored for a non-existent ID must return null, not throw — callers treat null as "not found".
     */
    @Test
    public void testGetRecord_notFound_returnsNull() throws DataStoreException {
        assertNull("getRecordIfStored on unknown ID must return null",
                store.getRecordIfStored(new DataIdentifier("nonexistent1234567890abcdef")));
    }

    /**
     * Same content must produce the same record ID — deduplication is the core space-saving contract.
     */
    @Test
    public void testAddDuplicateRecord() throws DataStoreException, IOException {
        byte[] data = "identical content".getBytes();
        DataRecord r1 = store.addRecord(new ByteArrayInputStream(data));
        DataRecord r2 = store.addRecord(new ByteArrayInputStream(data));

        assertEquals("duplicate content must yield the same record ID", r1.getIdentifier(), r2.getIdentifier());
    }

    /**
     * deleteRecord must remove the blob so that subsequent getRecord returns null.
     */
    @Test
    public void testDeleteRecord() throws DataStoreException, IOException {
        DataRecord record = store.addRecord(new ByteArrayInputStream("to be deleted".getBytes()));
        DataIdentifier id = record.getIdentifier();

        store.deleteRecord(id);

        assertNull("deleted record must not be retrievable", store.getRecordIfStored(id));
    }

    /**
     * Records of different sizes must all round-trip correctly — exercises small, medium, and large code paths.
     */
    @Test
    public void testRecordsOfVaryingSizes() throws DataStoreException, IOException {
        int[] sizes = {100, 10 * 1024, 1024 * 1024};
        List<DataIdentifier> ids = new ArrayList<>();

        for (int size : sizes) {
            byte[] data = new byte[size];
            Arrays.fill(data, (byte) 0x42);
            DataRecord record = store.addRecord(new ByteArrayInputStream(data));
            assertEquals("stored record length must match for size=" + size, size, record.getLength());
            ids.add(record.getIdentifier());
        }

        for (int i = 0; i < sizes.length; i++) {
            DataRecord fetched = store.getRecord(ids.get(i));
            assertNotNull("record must be retrievable for size=" + sizes[i], fetched);
            assertEquals("fetched record length must match for size=" + sizes[i], sizes[i], fetched.getLength());
        }
    }

    private Properties azuriteProps(String containerName) {
        Properties p = new Properties();
        p.setProperty(AzureConstantsV12.AZURE_CONNECTION_STRING,
                "DefaultEndpointsProtocol=http" +
                        ";AccountName=" + AzuriteDockerRule.ACCOUNT_NAME +
                        ";AccountKey=" + AzuriteDockerRule.ACCOUNT_KEY +
                        ";BlobEndpoint=" + AZURITE.getBlobEndpoint());
        p.setProperty(AzureConstantsV12.AZURE_BLOB_CONTAINER_NAME, containerName);
        p.setProperty(AzureConstantsV12.AZURE_CREATE_CONTAINER, "true");
        p.setProperty(AzureConstantsV12.AZURE_BLOB_ENDPOINT, AZURITE.getBlobEndpoint());
        return p;
    }
}
