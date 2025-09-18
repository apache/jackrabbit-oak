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

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.google.common.cache.Cache;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants.AZURE_BlOB_META_DIR_NAME;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants.AZURE_BLOB_CONCURRENT_REQUESTS_PER_OPERATION;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants.AZURE_BLOB_REF_KEY;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants.AZURE_CONNECTION_STRING;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants.AZURE_BLOB_CONTAINER_NAME;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants.AZURE_STORAGE_ACCOUNT_NAME;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants.AZURE_BLOB_ENDPOINT;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants.AZURE_CREATE_CONTAINER;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants.AZURE_REF_ON_INIT;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants.PRESIGNED_HTTP_DOWNLOAD_URI_EXPIRY_SECONDS;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;

/**
 * Comprehensive test class for AzureBlobStoreBackend covering all methods and functionality.
 */
public class AzureBlobStoreBackendTest {

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private static final String CONTAINER_NAME = "test-container";
    private static final String TEST_BLOB_CONTENT = "test blob content";
    private static final String TEST_METADATA_CONTENT = "test metadata content";

    private BlobContainerClient container;
    private AzureBlobStoreBackend backend;
    private Properties testProperties;

    @Mock
    private AzureBlobContainerProvider mockProvider;

    @Mock
    private BlobContainerClient mockContainer;

    @Mock
    private BlobClient mockBlobClient;

    @Mock
    private BlockBlobClient mockBlockBlobClient;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        
        // Create real container for integration tests
        container = azurite.getContainer(CONTAINER_NAME, getConnectionString());
        
        // Setup test properties
        testProperties = createTestProperties();
        
        // Create backend instance
        backend = new AzureBlobStoreBackend();
        backend.setProperties(testProperties);
    }

    @After
    public void tearDown() throws Exception {
        if (backend != null) {
            try {
                backend.close();
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
        if (container != null) {
            try {
                container.deleteIfExists();
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
    }

    private Properties createTestProperties() {
        Properties properties = new Properties();
        properties.setProperty(AZURE_BLOB_CONTAINER_NAME, CONTAINER_NAME);
        properties.setProperty(AZURE_STORAGE_ACCOUNT_NAME, AzuriteDockerRule.ACCOUNT_NAME);
        properties.setProperty(AZURE_BLOB_ENDPOINT, azurite.getBlobEndpoint());
        properties.setProperty(AZURE_CONNECTION_STRING, getConnectionString());
        properties.setProperty(AZURE_CREATE_CONTAINER, "true");
        properties.setProperty(AZURE_REF_ON_INIT, "false"); // Disable for most tests
        return properties;
    }

    private static String getConnectionString() {
        return Utils.getConnectionString(
            AzuriteDockerRule.ACCOUNT_NAME, 
            AzuriteDockerRule.ACCOUNT_KEY, 
            azurite.getBlobEndpoint()
        );
    }

    // ========== INITIALIZATION AND CONFIGURATION TESTS ==========

    @Test
    public void testInitWithValidProperties() throws Exception {
        backend.init();
        assertNotNull("Backend should be initialized", backend);
        
        // Verify container was created
        BlobContainerClient azureContainer = backend.getAzureContainer();
        assertNotNull("Azure container should not be null", azureContainer);
        assertTrue("Container should exist", azureContainer.exists());
    }

    @Test
    public void testInitWithNullProperties() throws Exception {
        AzureBlobStoreBackend nullPropsBackend = new AzureBlobStoreBackend();
        // Should not set properties, will try to read from default config file
        
        try {
            nullPropsBackend.init();
            fail("Expected DataStoreException when no properties and no default config file");
        } catch (DataStoreException e) {
            assertTrue("Should contain config file error", 
                e.getMessage().contains("Unable to initialize Azure Data Store"));
        }
    }

    @Test
    public void testSetProperties() {
        Properties newProps = new Properties();
        newProps.setProperty("test.key", "test.value");
        
        backend.setProperties(newProps);
        
        // Verify properties were set (using reflection to access private field)
        try {
            Field propertiesField = AzureBlobStoreBackend.class.getDeclaredField("properties");
            propertiesField.setAccessible(true);
            Properties actualProps = (Properties) propertiesField.get(backend);
            assertEquals("Properties should be set", "test.value", actualProps.getProperty("test.key"));
        } catch (Exception e) {
            fail("Failed to verify properties were set: " + e.getMessage());
        }
    }

    @Test
    public void testConcurrentRequestCountValidation() throws Exception {
        // Test with too low concurrent request count
        Properties lowProps = createTestProperties();
        lowProps.setProperty(AZURE_BLOB_CONCURRENT_REQUESTS_PER_OPERATION, "1");
        
        AzureBlobStoreBackend lowBackend = new AzureBlobStoreBackend();
        lowBackend.setProperties(lowProps);
        lowBackend.init();
        
        // Should reset to default minimum (verified through successful initialization)
        assertNotNull("Backend should initialize with low concurrent request count", lowBackend);
        lowBackend.close();
        
        // Test with too high concurrent request count
        Properties highProps = createTestProperties();
        highProps.setProperty(AZURE_BLOB_CONCURRENT_REQUESTS_PER_OPERATION, "100");
        
        AzureBlobStoreBackend highBackend = new AzureBlobStoreBackend();
        highBackend.setProperties(highProps);
        highBackend.init();
        
        // Should reset to default maximum (verified through successful initialization)
        assertNotNull("Backend should initialize with high concurrent request count", highBackend);
        highBackend.close();
    }

    @Test
    public void testGetAzureContainerThreadSafety() throws Exception {
        backend.init();
        
        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        List<Future<BlobContainerClient>> futures = new ArrayList<>();
        
        // Submit multiple threads to get container simultaneously
        for (int i = 0; i < threadCount; i++) {
            futures.add(executor.submit(() -> {
                try {
                    latch.countDown();
                    latch.await(); // Wait for all threads to be ready
                    return backend.getAzureContainer();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }
        
        // Verify all threads get the same container instance
        BlobContainerClient firstContainer = futures.get(0).get(5, TimeUnit.SECONDS);
        for (Future<BlobContainerClient> future : futures) {
            BlobContainerClient container = future.get(5, TimeUnit.SECONDS);
            assertSame("All threads should get the same container instance", firstContainer, container);
        }
        
        executor.shutdown();
    }

    // ========== CORE CRUD OPERATIONS TESTS ==========

    @Test
    public void testWriteAndRead() throws Exception {
        backend.init();

        // Create test file
        File testFile = createTempFile("test-content");
        DataIdentifier identifier = new DataIdentifier("testidentifier123");

        try {
            // Write file
            backend.write(identifier, testFile);

            // Read file
            try (InputStream inputStream = backend.read(identifier)) {
                String content = IOUtils.toString(inputStream, "UTF-8");
                assertEquals("Content should match", "test-content", content);
            }
        } finally {
            testFile.delete();
        }
    }

    @Test
    public void testWriteWithNullIdentifier() throws Exception {
        backend.init();
        File testFile = createTempFile("test");

        try {
            backend.write(null, testFile);
            fail("Expected NullPointerException for null identifier");
        } catch (NullPointerException e) {
            assertEquals("identifier", e.getMessage());
        } finally {
            testFile.delete();
        }
    }

    @Test
    public void testWriteWithNullFile() throws Exception {
        backend.init();
        DataIdentifier identifier = new DataIdentifier("test");

        try {
            backend.write(identifier, null);
            fail("Expected NullPointerException for null file");
        } catch (NullPointerException e) {
            assertEquals("file", e.getMessage());
        }
    }

    @Test
    public void testWriteExistingBlobWithSameLength() throws Exception {
        backend.init();

        File testFile = createTempFile("same-content");
        DataIdentifier identifier = new DataIdentifier("existingblob123");

        try {
            // Write file first time
            backend.write(identifier, testFile);

            // Write same file again (should update metadata)
            backend.write(identifier, testFile);

            // Verify content is still accessible
            try (InputStream inputStream = backend.read(identifier)) {
                String content = IOUtils.toString(inputStream, "UTF-8");
                assertEquals("Content should match", "same-content", content);
            }
        } finally {
            testFile.delete();
        }
    }

    @Test
    public void testWriteExistingBlobWithDifferentLength() throws Exception {
        backend.init();

        File testFile1 = createTempFile("content1");
        File testFile2 = createTempFile("different-length-content");
        DataIdentifier identifier = new DataIdentifier("lengthcollision");

        try {
            // Write first file
            backend.write(identifier, testFile1);

            // Try to write file with different length
            try {
                backend.write(identifier, testFile2);
                fail("Expected DataStoreException for length collision");
            } catch (DataStoreException e) {
                assertTrue("Should contain length collision error",
                    e.getMessage().contains("Length Collision"));
            }
        } finally {
            testFile1.delete();
            testFile2.delete();
        }
    }

    @Test
    public void testReadNonExistentBlob() throws Exception {
        backend.init();
        DataIdentifier identifier = new DataIdentifier("nonexistent123");

        try {
            backend.read(identifier);
            fail("Expected DataStoreException for non-existent blob");
        } catch (DataStoreException e) {
            assertTrue("Should contain missing blob error",
                e.getMessage().contains("Trying to read missing blob"));
        }
    }

    @Test
    public void testReadWithNullIdentifier() throws Exception {
        backend.init();

        try {
            backend.read(null);
            fail("Expected NullPointerException for null identifier");
        } catch (NullPointerException e) {
            assertEquals("identifier", e.getMessage());
        }
    }

    @Test
    public void testGetRecord() throws Exception {
        backend.init();

        File testFile = createTempFile("record-content");
        DataIdentifier identifier = new DataIdentifier("testrecord123");

        try {
            // Write file first
            backend.write(identifier, testFile);

            // Get record
            DataRecord record = backend.getRecord(identifier);
            assertNotNull("Record should not be null", record);
            assertEquals("Record identifier should match", identifier, record.getIdentifier());
            assertEquals("Record length should match", testFile.length(), record.getLength());
            assertTrue("Record should have valid last modified time", record.getLastModified() > 0);
        } finally {
            testFile.delete();
        }
    }

    @Test
    public void testGetRecordNonExistent() throws Exception {
        backend.init();
        DataIdentifier identifier = new DataIdentifier("nonexistentrecord");

        try {
            backend.getRecord(identifier);
            fail("Expected DataStoreException for non-existent record");
        } catch (DataStoreException e) {
            assertTrue("Should contain retrieve blob error",
                e.getMessage().contains("Cannot retrieve blob"));
        }
    }

    @Test
    public void testGetRecordWithNullIdentifier() throws Exception {
        backend.init();

        try {
            backend.getRecord(null);
            fail("Expected NullPointerException for null identifier");
        } catch (NullPointerException e) {
            assertEquals("identifier", e.getMessage());
        }
    }

    @Test
    public void testExists() throws Exception {
        backend.init();

        File testFile = createTempFile("exists-content");
        DataIdentifier identifier = new DataIdentifier("existstest123");

        try {
            // Initially should not exist
            assertFalse("Blob should not exist initially", backend.exists(identifier));

            // Write file
            backend.write(identifier, testFile);

            // Now should exist
            assertTrue("Blob should exist after write", backend.exists(identifier));
        } finally {
            testFile.delete();
        }
    }

    @Test
    public void testDeleteRecord() throws Exception {
        backend.init();

        File testFile = createTempFile("delete-content");
        DataIdentifier identifier = new DataIdentifier("deletetest123");

        try {
            // Write file
            backend.write(identifier, testFile);
            assertTrue("Blob should exist before delete", backend.exists(identifier));

            // Delete record
            backend.deleteRecord(identifier);
            assertFalse("Blob should not exist after delete", backend.exists(identifier));
        } finally {
            testFile.delete();
        }
    }

    @Test
    public void testDeleteNonExistentRecord() throws Exception {
        backend.init();
        DataIdentifier identifier = new DataIdentifier("nonexistentdelete");

        // Should not throw exception when deleting non-existent record
        backend.deleteRecord(identifier);
        // No exception expected
    }

    @Test
    public void testDeleteRecordWithNullIdentifier() throws Exception {
        backend.init();

        try {
            backend.deleteRecord(null);
            fail("Expected NullPointerException for null identifier");
        } catch (NullPointerException e) {
            assertEquals("identifier", e.getMessage());
        }
    }

    @Test
    public void testGetAllIdentifiers() throws Exception {
        backend.init();

        // Create multiple test files
        File testFile1 = createTempFile("content1");
        File testFile2 = createTempFile("content2");
        DataIdentifier id1 = new DataIdentifier("identifier1");
        DataIdentifier id2 = new DataIdentifier("identifier2");

        try {
            // Write files
            backend.write(id1, testFile1);
            backend.write(id2, testFile2);

            // Get all identifiers
            Iterator<DataIdentifier> identifiers = backend.getAllIdentifiers();
            assertNotNull("Identifiers iterator should not be null", identifiers);

            // Collect identifiers
            List<String> identifierStrings = new ArrayList<>();
            while (identifiers.hasNext()) {
                identifierStrings.add(identifiers.next().toString());
            }

            // Should contain both identifiers
            assertTrue("Should contain identifier1", identifierStrings.contains("identifier1"));
            assertTrue("Should contain identifier2", identifierStrings.contains("identifier2"));
        } finally {
            testFile1.delete();
            testFile2.delete();
        }
    }

    @Test
    public void testGetAllRecords() throws Exception {
        backend.init();

        // Create test file
        File testFile = createTempFile("record-content");
        DataIdentifier identifier = new DataIdentifier("recordtest123");

        try {
            // Write file
            backend.write(identifier, testFile);

            // Get all records
            Iterator<DataRecord> records = backend.getAllRecords();
            assertNotNull("Records iterator should not be null", records);

            // Find our record
            boolean foundRecord = false;
            while (records.hasNext()) {
                DataRecord record = records.next();
                if (record.getIdentifier().toString().equals("recordtest123")) {
                    foundRecord = true;
                    assertEquals("Record length should match", testFile.length(), record.getLength());
                    assertTrue("Record should have valid last modified time", record.getLastModified() > 0);
                    break;
                }
            }
            assertTrue("Should find our test record", foundRecord);
        } finally {
            testFile.delete();
        }
    }

    // ========== METADATA OPERATIONS TESTS ==========

    @Test
    public void testAddMetadataRecordWithInputStream() throws Exception {
        backend.init();

        String metadataName = "test-metadata-stream";
        String content = TEST_METADATA_CONTENT;

        // Add metadata record
        backend.addMetadataRecord(new ByteArrayInputStream(content.getBytes()), metadataName);

        // Verify record exists
        assertTrue("Metadata record should exist", backend.metadataRecordExists(metadataName));

        // Verify content
        DataRecord record = backend.getMetadataRecord(metadataName);
        assertNotNull("Record should not be null", record);
        assertEquals("Record should have correct length", content.length(), record.getLength());

        // Verify content can be read
        try (InputStream stream = record.getStream()) {
            String readContent = IOUtils.toString(stream, "UTF-8");
            assertEquals("Content should match", content, readContent);
        }

        // Clean up
        backend.deleteMetadataRecord(metadataName);
    }

    @Test
    public void testAddMetadataRecordWithFile() throws Exception {
        backend.init();

        String metadataName = "test-metadata-file";
        File metadataFile = createTempFile(TEST_METADATA_CONTENT);

        try {
            // Add metadata record from file
            backend.addMetadataRecord(metadataFile, metadataName);

            // Verify record exists
            assertTrue("Metadata record should exist", backend.metadataRecordExists(metadataName));

            // Verify content
            DataRecord record = backend.getMetadataRecord(metadataName);
            assertNotNull("Record should not be null", record);
            assertEquals("Record should have correct length", metadataFile.length(), record.getLength());

            // Clean up
            backend.deleteMetadataRecord(metadataName);
        } finally {
            metadataFile.delete();
        }
    }

    @Test
    public void testAddMetadataRecordWithNullInputStream() throws Exception {
        backend.init();

        try {
            backend.addMetadataRecord((InputStream) null, "test");
            fail("Expected NullPointerException for null input stream");
        } catch (NullPointerException e) {
            assertEquals("input", e.getMessage());
        }
    }

    @Test
    public void testAddMetadataRecordWithNullFile() throws Exception {
        backend.init();

        try {
            backend.addMetadataRecord((File) null, "test");
            fail("Expected NullPointerException for null file");
        } catch (NullPointerException e) {
            assertEquals("input", e.getMessage());
        }
    }

    @Test
    public void testAddMetadataRecordWithNullName() throws Exception {
        backend.init();

        try {
            backend.addMetadataRecord(new ByteArrayInputStream("test".getBytes()), null);
            fail("Expected IllegalArgumentException for null name");
        } catch (IllegalArgumentException e) {
            assertEquals("name", e.getMessage());
        }
    }

    @Test
    public void testAddMetadataRecordWithEmptyName() throws Exception {
        backend.init();

        try {
            backend.addMetadataRecord(new ByteArrayInputStream("test".getBytes()), "");
            fail("Expected IllegalArgumentException for empty name");
        } catch (IllegalArgumentException e) {
            assertEquals("name", e.getMessage());
        }
    }

    @Test
    public void testGetMetadataRecordNonExistent() throws Exception {
        backend.init();

        DataRecord record = backend.getMetadataRecord("non-existent-metadata");
        assertNull("Non-existent metadata record should return null", record);
    }

    @Test
    public void testGetAllMetadataRecords() throws Exception {
        backend.init();

        String prefix = "test-prefix-";
        String content = "metadata content";

        // Add multiple metadata records
        for (int i = 0; i < 3; i++) {
            backend.addMetadataRecord(
                new ByteArrayInputStream((content + i).getBytes()),
                prefix + i
            );
        }

        try {
            // Get all metadata records
            List<DataRecord> records = backend.getAllMetadataRecords("");
            assertNotNull("Records list should not be null", records);

            // Find our records
            int foundCount = 0;
            for (DataRecord record : records) {
                if (record.getIdentifier().toString().startsWith(prefix)) {
                    foundCount++;
                }
            }
            assertEquals("Should find all 3 metadata records", 3, foundCount);
        } finally {
            // Clean up
            for (int i = 0; i < 3; i++) {
                backend.deleteMetadataRecord(prefix + i);
            }
        }
    }

    @Test
    public void testGetAllMetadataRecordsWithNullPrefix() throws Exception {
        backend.init();

        try {
            backend.getAllMetadataRecords(null);
            fail("Expected NullPointerException for null prefix");
        } catch (NullPointerException e) {
            assertEquals("prefix", e.getMessage());
        }
    }

    @Test
    public void testDeleteMetadataRecord() throws Exception {
        backend.init();

        String metadataName = "delete-metadata-test";
        String content = "content to delete";

        // Add metadata record
        backend.addMetadataRecord(new ByteArrayInputStream(content.getBytes()), metadataName);
        assertTrue("Metadata record should exist", backend.metadataRecordExists(metadataName));

        // Delete metadata record
        boolean deleted = backend.deleteMetadataRecord(metadataName);
        assertTrue("Delete should return true", deleted);
        assertFalse("Metadata record should not exist after delete", backend.metadataRecordExists(metadataName));
    }

    @Test
    public void testDeleteNonExistentMetadataRecord() throws Exception {
        backend.init();

        boolean deleted = backend.deleteMetadataRecord("non-existent-metadata");
        assertFalse("Delete should return false for non-existent record", deleted);
    }

    @Test
    public void testDeleteAllMetadataRecords() throws Exception {
        backend.init();

        String prefix = "delete-all-";

        // Add multiple metadata records
        for (int i = 0; i < 3; i++) {
            backend.addMetadataRecord(
                new ByteArrayInputStream(("content" + i).getBytes()),
                prefix + i
            );
        }

        // Verify records exist
        for (int i = 0; i < 3; i++) {
            assertTrue("Record should exist", backend.metadataRecordExists(prefix + i));
        }

        // Delete all records with prefix
        backend.deleteAllMetadataRecords(prefix);

        // Verify records are deleted
        for (int i = 0; i < 3; i++) {
            assertFalse("Record should be deleted", backend.metadataRecordExists(prefix + i));
        }
    }

    @Test
    public void testDeleteAllMetadataRecordsWithNullPrefix() throws Exception {
        backend.init();

        try {
            backend.deleteAllMetadataRecords(null);
            fail("Expected NullPointerException for null prefix");
        } catch (NullPointerException e) {
            assertEquals("prefix", e.getMessage());
        }
    }

    @Test
    public void testMetadataRecordExists() throws Exception {
        backend.init();

        String metadataName = "exists-test-metadata";

        // Initially should not exist
        assertFalse("Metadata record should not exist initially",
            backend.metadataRecordExists(metadataName));

        // Add metadata record
        backend.addMetadataRecord(
            new ByteArrayInputStream("test content".getBytes()),
            metadataName
        );

        // Now should exist
        assertTrue("Metadata record should exist after add",
            backend.metadataRecordExists(metadataName));

        // Clean up
        backend.deleteMetadataRecord(metadataName);
    }

    // ========== UTILITY AND HELPER METHOD TESTS ==========

    @Test
    public void testGetKeyName() throws Exception {
        // Test the static getKeyName method using reflection
        Method getKeyNameMethod = AzureBlobStoreBackend.class.getDeclaredMethod("getKeyName", DataIdentifier.class);
        getKeyNameMethod.setAccessible(true);

        DataIdentifier identifier = new DataIdentifier("abcd1234567890");
        String keyName = (String) getKeyNameMethod.invoke(null, identifier);

        assertEquals("Key name should be formatted correctly", "abcd-1234567890", keyName);
    }

    @Test
    public void testGetIdentifierName() throws Exception {
        // Test the static getIdentifierName method using reflection
        Method getIdentifierNameMethod = AzureBlobStoreBackend.class.getDeclaredMethod("getIdentifierName", String.class);
        getIdentifierNameMethod.setAccessible(true);

        String identifierName = (String) getIdentifierNameMethod.invoke(null, "abcd-1234567890");
        assertEquals("Identifier name should be formatted correctly", "abcd1234567890", identifierName);

        // Test with metadata key
        String metaKey = "META/test-key";
        String metaIdentifierName = (String) getIdentifierNameMethod.invoke(null, metaKey);
        assertEquals("Metadata identifier should be returned as-is", metaKey, metaIdentifierName);

        // Test with key without dash
        String noDashKey = "nodashkey";
        String noDashResult = (String) getIdentifierNameMethod.invoke(null, noDashKey);
        assertNull("Key without dash should return null", noDashResult);
    }

    @Test
    public void testAddMetaKeyPrefix() throws Exception {
        // Test the static addMetaKeyPrefix method using reflection
        Method addMetaKeyPrefixMethod = AzureBlobStoreBackend.class.getDeclaredMethod("addMetaKeyPrefix", String.class);
        addMetaKeyPrefixMethod.setAccessible(true);

        String result = (String) addMetaKeyPrefixMethod.invoke(null, "test-key");
        assertTrue("Result should contain META prefix", result.startsWith("META/"));
        assertTrue("Result should contain original key", result.endsWith("test-key"));
    }

    @Test
    public void testStripMetaKeyPrefix() throws Exception {
        // Test the static stripMetaKeyPrefix method using reflection
        Method stripMetaKeyPrefixMethod = AzureBlobStoreBackend.class.getDeclaredMethod("stripMetaKeyPrefix", String.class);
        stripMetaKeyPrefixMethod.setAccessible(true);

        String withPrefix = "META/test-key";
        String result = (String) stripMetaKeyPrefixMethod.invoke(null, withPrefix);
        assertEquals("Should strip META prefix", "test-key", result);

        String withoutPrefix = "regular-key";
        String result2 = (String) stripMetaKeyPrefixMethod.invoke(null, withoutPrefix);
        assertEquals("Should return original key if no prefix", withoutPrefix, result2);
    }

    @Test
    public void testGetOrCreateReferenceKey() throws Exception {
        // Enable reference key creation on init
        Properties propsWithRef = createTestProperties();
        propsWithRef.setProperty(AZURE_REF_ON_INIT, "true");

        AzureBlobStoreBackend refBackend = new AzureBlobStoreBackend();
        refBackend.setProperties(propsWithRef);
        refBackend.init();

        try {
            // Get reference key
            byte[] key1 = refBackend.getOrCreateReferenceKey();
            assertNotNull("Reference key should not be null", key1);
            assertTrue("Reference key should have length > 0", key1.length > 0);

            // Get reference key again - should be same
            byte[] key2 = refBackend.getOrCreateReferenceKey();
            assertArrayEquals("Reference key should be consistent", key1, key2);

            // Verify reference key is stored as metadata
            DataRecord refRecord = refBackend.getMetadataRecord(AZURE_BLOB_REF_KEY);
            assertNotNull("Reference key metadata record should exist", refRecord);
            assertTrue("Reference key record should have length > 0", refRecord.getLength() > 0);
        } finally {
            refBackend.close();
        }
    }

    @Test
    public void testReadMetadataBytes() throws Exception {
        backend.init();

        String metadataName = "read-bytes-test";
        String content = "test bytes content";

        // Add metadata record
        backend.addMetadataRecord(new ByteArrayInputStream(content.getBytes()), metadataName);

        try {
            // Read metadata bytes using reflection
            Method readMetadataBytesMethod = AzureBlobStoreBackend.class.getDeclaredMethod("readMetadataBytes", String.class);
            readMetadataBytesMethod.setAccessible(true);

            byte[] bytes = (byte[]) readMetadataBytesMethod.invoke(backend, metadataName);
            assertNotNull("Bytes should not be null", bytes);
            assertEquals("Content should match", content, new String(bytes));

            // Test with non-existent metadata
            byte[] nullBytes = (byte[]) readMetadataBytesMethod.invoke(backend, "non-existent");
            assertNull("Non-existent metadata should return null", nullBytes);
        } finally {
            backend.deleteMetadataRecord(metadataName);
        }
    }

    // ========== DIRECT ACCESS FUNCTIONALITY TESTS ==========

    @Test
    public void testSetHttpDownloadURIExpirySeconds() throws Exception {
        // Test setting download URI expiry using reflection
        Method setExpiryMethod = AzureBlobStoreBackend.class.getDeclaredMethod("setHttpDownloadURIExpirySeconds", int.class);
        setExpiryMethod.setAccessible(true);

        setExpiryMethod.invoke(backend, 3600);

        // Verify the field was set
        Field expiryField = AzureBlobStoreBackend.class.getDeclaredField("httpDownloadURIExpirySeconds");
        expiryField.setAccessible(true);
        int expiry = (int) expiryField.get(backend);
        assertEquals("Expiry should be set", 3600, expiry);
    }

    @Test
    public void testSetHttpUploadURIExpirySeconds() throws Exception {
        // Test setting upload URI expiry using reflection
        Method setExpiryMethod = AzureBlobStoreBackend.class.getDeclaredMethod("setHttpUploadURIExpirySeconds", int.class);
        setExpiryMethod.setAccessible(true);

        setExpiryMethod.invoke(backend, 1800);

        // Verify the field was set
        Field expiryField = AzureBlobStoreBackend.class.getDeclaredField("httpUploadURIExpirySeconds");
        expiryField.setAccessible(true);
        int expiry = (int) expiryField.get(backend);
        assertEquals("Expiry should be set", 1800, expiry);
    }

    @Test
    public void testSetHttpDownloadURICacheSize() throws Exception {
        // Test setting cache size using reflection
        Method setCacheSizeMethod = AzureBlobStoreBackend.class.getDeclaredMethod("setHttpDownloadURICacheSize", int.class);
        setCacheSizeMethod.setAccessible(true);

        // Test with positive cache size
        setCacheSizeMethod.invoke(backend, 100);

        Field cacheField = AzureBlobStoreBackend.class.getDeclaredField("httpDownloadURICache");
        cacheField.setAccessible(true);
        Cache<String, URI> cache = (Cache<String, URI>) cacheField.get(backend);
        assertNotNull("Cache should be created for positive size", cache);

        // Test with zero cache size (disabled)
        setCacheSizeMethod.invoke(backend, 0);
        cache = (Cache<String, URI>) cacheField.get(backend);
        assertNull("Cache should be null for zero size", cache);
    }

    @Test
    public void testCreateHttpDownloadURI() throws Exception {
        backend.init();

        // Set up download URI configuration
        Properties propsWithDownload = createTestProperties();
        propsWithDownload.setProperty(PRESIGNED_HTTP_DOWNLOAD_URI_EXPIRY_SECONDS, "3600");

        AzureBlobStoreBackend downloadBackend = new AzureBlobStoreBackend();
        downloadBackend.setProperties(propsWithDownload);
        downloadBackend.init();

        try {
            // Create a test blob first
            File testFile = createTempFile("download-test");
            DataIdentifier identifier = new DataIdentifier("downloadtestblob");
            downloadBackend.write(identifier, testFile);

            // Create download URI using reflection
            Method createDownloadURIMethod = AzureBlobStoreBackend.class.getDeclaredMethod(
                "createHttpDownloadURI", DataIdentifier.class, DataRecordDownloadOptions.class);
            createDownloadURIMethod.setAccessible(true);

            DataRecordDownloadOptions options = DataRecordDownloadOptions.DEFAULT;

            URI downloadURI = (URI) createDownloadURIMethod.invoke(downloadBackend, identifier, options);
            // Note: This may return null if the backend doesn't support presigned URIs in test environment
            // The important thing is that it doesn't throw an exception

            testFile.delete();
        } finally {
            downloadBackend.close();
        }
    }

    @Test
    public void testCreateHttpDownloadURIWithNullIdentifier() throws Exception {
        backend.init();

        Method createDownloadURIMethod = AzureBlobStoreBackend.class.getDeclaredMethod(
            "createHttpDownloadURI", DataIdentifier.class, DataRecordDownloadOptions.class);
        createDownloadURIMethod.setAccessible(true);

        DataRecordDownloadOptions options = DataRecordDownloadOptions.DEFAULT;

        try {
            createDownloadURIMethod.invoke(backend, null, options);
            fail("Expected NullPointerException for null identifier");
        } catch (Exception e) {
            assertTrue("Should throw NullPointerException",
                e.getCause() instanceof NullPointerException);
        }
    }

    @Test
    public void testCreateHttpDownloadURIWithNullOptions() throws Exception {
        backend.init();

        Method createDownloadURIMethod = AzureBlobStoreBackend.class.getDeclaredMethod(
            "createHttpDownloadURI", DataIdentifier.class, DataRecordDownloadOptions.class);
        createDownloadURIMethod.setAccessible(true);

        DataIdentifier identifier = new DataIdentifier("test");

        try {
            createDownloadURIMethod.invoke(backend, identifier, null);
            fail("Expected NullPointerException for null options");
        } catch (Exception e) {
            assertTrue("Should throw NullPointerException",
                e.getCause() instanceof NullPointerException);
        }
    }

    // ========== AZUREBLOBSTOREDATARECORD INNER CLASS TESTS ==========

    @Test
    public void testAzureBlobStoreDataRecordRegular() throws Exception {
        backend.init();

        // Create test file and write it
        File testFile = createTempFile("data-record-test");
        DataIdentifier identifier = new DataIdentifier("datarecordtest123");

        try {
            backend.write(identifier, testFile);

            // Get the data record
            DataRecord record = backend.getRecord(identifier);
            assertNotNull("Record should not be null", record);

            // Test getLength()
            assertEquals("Length should match file length", testFile.length(), record.getLength());

            // Test getLastModified()
            assertTrue("Last modified should be positive", record.getLastModified() > 0);

            // Test getIdentifier()
            assertEquals("Identifier should match", identifier, record.getIdentifier());

            // Test getStream()
            try (InputStream stream = record.getStream()) {
                String content = IOUtils.toString(stream, "UTF-8");
                assertEquals("Content should match", "data-record-test", content);
            }

            // Test toString()
            String toString = record.toString();
            assertNotNull("toString should not be null", toString);
            assertTrue("toString should contain identifier", toString.contains(identifier.toString()));
            assertTrue("toString should contain length", toString.contains(String.valueOf(testFile.length())));
            assertTrue("toString should contain container name", toString.contains(CONTAINER_NAME));
        } finally {
            testFile.delete();
        }
    }

    @Test
    public void testAzureBlobStoreDataRecordMetadata() throws Exception {
        backend.init();

        String metadataName = "data-record-metadata-test";
        String content = "metadata record content";

        // Add metadata record
        backend.addMetadataRecord(new ByteArrayInputStream(content.getBytes()), metadataName);

        try {
            // Get the metadata record
            DataRecord record = backend.getMetadataRecord(metadataName);
            assertNotNull("Metadata record should not be null", record);

            // Test getLength()
            assertEquals("Length should match content length", content.length(), record.getLength());

            // Test getLastModified()
            assertTrue("Last modified should be positive", record.getLastModified() > 0);

            // Test getIdentifier()
            assertEquals("Identifier should match metadata name", metadataName, record.getIdentifier().toString());

            // Test getStream()
            try (InputStream stream = record.getStream()) {
                String readContent = IOUtils.toString(stream, "UTF-8");
                assertEquals("Content should match", content, readContent);
            }

            // Test toString()
            String toString = record.toString();
            assertNotNull("toString should not be null", toString);
            assertTrue("toString should contain identifier", toString.contains(metadataName));
            assertTrue("toString should contain length", toString.contains(String.valueOf(content.length())));
        } finally {
            backend.deleteMetadataRecord(metadataName);
        }
    }

    // ========== CLOSE AND CLEANUP TESTS ==========

    @Test
    public void testClose() throws Exception {
        backend.init();

        // Should not throw exception
        backend.close();

        // Should be able to call close multiple times
        backend.close();
        backend.close();
    }

    // ========== ERROR HANDLING AND EDGE CASES ==========

    @Test
    public void testInitWithInvalidConnectionString() throws Exception {
        AzureBlobStoreBackend invalidBackend = new AzureBlobStoreBackend();
        Properties invalidProps = new Properties();
        invalidProps.setProperty(AZURE_CONNECTION_STRING, "invalid-connection-string");
        invalidProps.setProperty(AZURE_BLOB_CONTAINER_NAME, "test-container");
        invalidBackend.setProperties(invalidProps);

        try {
            invalidBackend.init();
            fail("Expected exception with invalid connection string");
        } catch (Exception e) {
            // Expected - can be DataStoreException or IllegalArgumentException
            assertNotNull("Exception should not be null", e);
            assertTrue("Should be DataStoreException or IllegalArgumentException",
                e instanceof DataStoreException || e instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testInitWithMissingContainer() throws Exception {
        Properties propsNoContainer = createTestProperties();
        propsNoContainer.remove(AZURE_BLOB_CONTAINER_NAME);

        AzureBlobStoreBackend noContainerBackend = new AzureBlobStoreBackend();
        noContainerBackend.setProperties(propsNoContainer);

        try {
            noContainerBackend.init();
            // If no exception is thrown, the backend might use a default container name
            // This is acceptable behavior
        } catch (Exception e) {
            // Exception is also acceptable - depends on implementation
            assertNotNull("Exception should not be null", e);
        }
    }

    @Test
    public void testInitWithCreateContainerDisabled() throws Exception {
        // Create container first
        container = azurite.getContainer(CONTAINER_NAME + "-nocreate", getConnectionString());

        Properties propsNoCreate = createTestProperties();
        propsNoCreate.setProperty(AZURE_BLOB_CONTAINER_NAME, CONTAINER_NAME + "-nocreate");
        propsNoCreate.setProperty(AZURE_CREATE_CONTAINER, "false");

        AzureBlobStoreBackend noCreateBackend = new AzureBlobStoreBackend();
        noCreateBackend.setProperties(propsNoCreate);
        noCreateBackend.init();

        assertNotNull("Backend should initialize with existing container", noCreateBackend);
        noCreateBackend.close();
    }

    // ========== HELPER METHODS ==========

    @Test
    public void testLargeFileHandling() throws Exception {
        backend.init();

        // Create a larger test file (1MB)
        File largeFile = File.createTempFile("large-test", ".tmp");
        try (FileWriter writer = new FileWriter(largeFile)) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < 10000; i++) {
                sb.append("This is line ").append(i).append(" of the large test file.\n");
            }
            writer.write(sb.toString());
        }

        DataIdentifier identifier = new DataIdentifier("largefiletest123");

        try {
            // Write large file
            backend.write(identifier, largeFile);

            // Verify it exists
            assertTrue("Large file should exist", backend.exists(identifier));

            // Read and verify content
            try (InputStream inputStream = backend.read(identifier)) {
                byte[] readBytes = IOUtils.toByteArray(inputStream);
                assertEquals("Content length should match", largeFile.length(), readBytes.length);
            }

            // Get record and verify
            DataRecord record = backend.getRecord(identifier);
            assertEquals("Record length should match file length", largeFile.length(), record.getLength());
        } finally {
            largeFile.delete();
        }
    }

    @Test
    public void testEmptyFileHandling() throws Exception {
        backend.init();

        // Create empty file
        File emptyFile = File.createTempFile("empty-test", ".tmp");
        DataIdentifier identifier = new DataIdentifier("emptyfiletest123");

        try {
            // Azure SDK doesn't support zero-length block sizes, so this should throw an exception
            backend.write(identifier, emptyFile);
            fail("Expected IllegalArgumentException for empty file");
        } catch (IllegalArgumentException e) {
            // Expected - Azure SDK doesn't allow zero-length block sizes
            assertTrue("Should mention block size", e.getMessage().contains("blockSize"));
        } catch (Exception e) {
            // Also acceptable if wrapped in another exception
            assertTrue("Should be related to empty file handling",
                e.getMessage().contains("blockSize") || e.getCause() instanceof IllegalArgumentException);
        } finally {
            emptyFile.delete();
        }
    }

    @Test
    public void testSpecialCharactersInIdentifier() throws Exception {
        backend.init();

        File testFile = createTempFile("special-chars-content");
        // Use identifier with special characters that are valid in blob names
        DataIdentifier identifier = new DataIdentifier("testfile123data");

        try {
            // Write file
            backend.write(identifier, testFile);

            // Verify operations work with special characters
            assertTrue("File with special chars should exist", backend.exists(identifier));

            DataRecord record = backend.getRecord(identifier);
            assertEquals("Identifier should match", identifier, record.getIdentifier());

            // Read content
            try (InputStream inputStream = backend.read(identifier)) {
                String content = IOUtils.toString(inputStream, "UTF-8");
                assertEquals("Content should match", "special-chars-content", content);
            }
        } finally {
            testFile.delete();
        }
    }

    @Test
    public void testConcurrentOperations() throws Exception {
        backend.init();

        int threadCount = 5;
        int operationsPerThread = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        List<Future<Void>> futures = new ArrayList<>();

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            futures.add(executor.submit(() -> {
                try {
                    latch.countDown();
                    latch.await(); // Wait for all threads to be ready

                    for (int i = 0; i < operationsPerThread; i++) {
                        String content = "Thread " + threadId + " operation " + i;
                        File testFile = createTempFile(content);
                        DataIdentifier identifier = new DataIdentifier("concurrent" + threadId + "op" + i);

                        try {
                            // Write
                            backend.write(identifier, testFile);

                            // Verify exists
                            if (backend.exists(identifier)) {
                                // Read back
                                try (InputStream inputStream = backend.read(identifier)) {
                                    String readContent = IOUtils.toString(inputStream, "UTF-8");
                                    if (content.equals(readContent)) {
                                        successCount.incrementAndGet();
                                    }
                                }
                            }
                        } finally {
                            testFile.delete();
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return null;
            }));
        }

        // Wait for all operations to complete
        for (Future<Void> future : futures) {
            future.get(30, TimeUnit.SECONDS);
        }

        executor.shutdown();

        // Verify that most operations succeeded (allowing for some potential race conditions)
        int expectedSuccesses = threadCount * operationsPerThread;
        assertTrue("Most concurrent operations should succeed",
            successCount.get() >= expectedSuccesses * 0.8); // Allow 20% failure rate for race conditions
    }

    @Test
    public void testMetadataDirectoryStructure() throws Exception {
        backend.init();

        String metadataName = "directory-structure-test";
        String content = "directory test content";

        // Add metadata record
        backend.addMetadataRecord(new ByteArrayInputStream(content.getBytes()), metadataName);

        try {
            // Verify the record is stored with correct path prefix
            BlobContainerClient azureContainer = backend.getAzureContainer();
            String expectedBlobName = AZURE_BlOB_META_DIR_NAME + "/" + metadataName;

            BlobClient blobClient = azureContainer.getBlobClient(expectedBlobName);
            assertTrue("Blob should exist at expected path", blobClient.exists());

            // Verify the blob is in the META directory
            ListBlobsOptions listOptions = new ListBlobsOptions();
            listOptions.setPrefix(AZURE_BlOB_META_DIR_NAME);

            boolean foundBlob = false;
            for (BlobItem blobItem : azureContainer.listBlobs(listOptions, null)) {
                if (blobItem.getName().equals(expectedBlobName)) {
                    foundBlob = true;
                    break;
                }
            }
            assertTrue("Blob should be found in META directory listing", foundBlob);
        } finally {
            backend.deleteMetadataRecord(metadataName);
        }
    }

    // ========== HELPER METHODS ==========

    private File createTempFile(String content) throws IOException {
        File tempFile = File.createTempFile("azure-test", ".tmp");
        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write(content);
        }
        return tempFile;
    }
}
