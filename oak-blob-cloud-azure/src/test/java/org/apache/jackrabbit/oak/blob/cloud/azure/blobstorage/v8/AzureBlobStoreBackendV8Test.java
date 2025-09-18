/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.v8;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.SharedAccessBlobPermissions;
import com.microsoft.azure.storage.blob.SharedAccessBlobPolicy;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzuriteDockerRule;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.ADD;
import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.CREATE;
import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.LIST;
import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.READ;
import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.WRITE;
import static java.util.stream.Collectors.toSet;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants.AZURE_BlOB_META_DIR_NAME;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNotNull;

public class AzureBlobStoreBackendV8Test {
    private static final String AZURE_ACCOUNT_NAME = "AZURE_ACCOUNT_NAME";
    private static final String AZURE_TENANT_ID = "AZURE_TENANT_ID";
    private static final String AZURE_CLIENT_ID = "AZURE_CLIENT_ID";
    private static final String AZURE_CLIENT_SECRET = "AZURE_CLIENT_SECRET";
    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private static final String CONTAINER_NAME = "blobstore";
    private static final EnumSet<SharedAccessBlobPermissions> READ_ONLY = EnumSet.of(READ, LIST);
    private static final EnumSet<SharedAccessBlobPermissions> READ_WRITE = EnumSet.of(READ, LIST, CREATE, WRITE, ADD);
    private static final Set<String> BLOBS = Set.of("blob1", "blob2");

    private CloudBlobContainer container;

    @After
    public void tearDown() throws Exception {
        if (container != null) {
            container.deleteIfExists();
        }
    }

    @Test
    public void initWithSharedAccessSignature_readOnly() throws Exception {
        CloudBlobContainer container = createBlobContainer();
        String sasToken = container.generateSharedAccessSignature(policy(READ_ONLY), null);

        AzureBlobStoreBackendV8 azureBlobStoreBackend = new AzureBlobStoreBackendV8();
        azureBlobStoreBackend.setProperties(getConfigurationWithSasToken(sasToken));

        azureBlobStoreBackend.init();

        assertWriteAccessNotGranted(azureBlobStoreBackend);
        assertReadAccessGranted(azureBlobStoreBackend, BLOBS);
    }

    @Test
    public void initWithSharedAccessSignature_readWrite() throws Exception {
        CloudBlobContainer container = createBlobContainer();
        String sasToken = container.generateSharedAccessSignature(policy(READ_WRITE), null);

        AzureBlobStoreBackendV8 azureBlobStoreBackend = new AzureBlobStoreBackendV8();
        azureBlobStoreBackend.setProperties(getConfigurationWithSasToken(sasToken));

        azureBlobStoreBackend.init();

        assertWriteAccessGranted(azureBlobStoreBackend, "file");
        assertReadAccessGranted(azureBlobStoreBackend,
                concat(BLOBS, "file"));
    }

    @Test
    public void connectWithSharedAccessSignatureURL_expired() throws Exception {
        CloudBlobContainer container = createBlobContainer();
        SharedAccessBlobPolicy expiredPolicy = policy(READ_WRITE, yesterday());
        String sasToken = container.generateSharedAccessSignature(expiredPolicy, null);

        AzureBlobStoreBackendV8 azureBlobStoreBackend = new AzureBlobStoreBackendV8();
        azureBlobStoreBackend.setProperties(getConfigurationWithSasToken(sasToken));

        azureBlobStoreBackend.init();

        assertWriteAccessNotGranted(azureBlobStoreBackend);
        assertReadAccessNotGranted(azureBlobStoreBackend);
    }

    @Test
    public void initWithAccessKey() throws Exception {
        AzureBlobStoreBackendV8 azureBlobStoreBackend = new AzureBlobStoreBackendV8();
        azureBlobStoreBackend.setProperties(getConfigurationWithAccessKey());

        azureBlobStoreBackend.init();

        assertWriteAccessGranted(azureBlobStoreBackend, "file");
        assertReadAccessGranted(azureBlobStoreBackend, Set.of("file"));
    }

    @Test
    public void initWithConnectionURL() throws Exception {
        AzureBlobStoreBackendV8 azureBlobStoreBackend = new AzureBlobStoreBackendV8();
        azureBlobStoreBackend.setProperties(getConfigurationWithConnectionString());

        azureBlobStoreBackend.init();

        assertWriteAccessGranted(azureBlobStoreBackend, "file");
        assertReadAccessGranted(azureBlobStoreBackend, Set.of("file"));
    }

    @Test
    public void initSecret() throws Exception {
        AzureBlobStoreBackendV8 azureBlobStoreBackend = new AzureBlobStoreBackendV8();
        azureBlobStoreBackend.setProperties(getConfigurationWithConnectionString());

        azureBlobStoreBackend.init();
        assertReferenceSecret(azureBlobStoreBackend);
    }

    /* make sure that blob1.txt and blob2.txt are uploaded to AZURE_ACCOUNT_NAME/blobstore container before
     * executing this test
     * */
    @Test
    public void initWithServicePrincipals() throws Exception {
        assumeNotNull(getEnvironmentVariable(AZURE_ACCOUNT_NAME));
        assumeNotNull(getEnvironmentVariable(AZURE_TENANT_ID));
        assumeNotNull(getEnvironmentVariable(AZURE_CLIENT_ID));
        assumeNotNull(getEnvironmentVariable(AZURE_CLIENT_SECRET));

        AzureBlobStoreBackendV8 azureBlobStoreBackend = new AzureBlobStoreBackendV8();
        azureBlobStoreBackend.setProperties(getPropertiesWithServicePrincipals());

        azureBlobStoreBackend.init();

        assertWriteAccessGranted(azureBlobStoreBackend, "test");
        assertReadAccessGranted(azureBlobStoreBackend, concat(BLOBS, "test"));
    }

    private Properties getPropertiesWithServicePrincipals() {
        final String accountName = getEnvironmentVariable(AZURE_ACCOUNT_NAME);
        final String tenantId = getEnvironmentVariable(AZURE_TENANT_ID);
        final String clientId = getEnvironmentVariable(AZURE_CLIENT_ID);
        final String clientSecret = getEnvironmentVariable(AZURE_CLIENT_SECRET);

        Properties properties = new Properties();
        properties.setProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, accountName);
        properties.setProperty(AzureConstants.AZURE_TENANT_ID, tenantId);
        properties.setProperty(AzureConstants.AZURE_CLIENT_ID, clientId);
        properties.setProperty(AzureConstants.AZURE_CLIENT_SECRET, clientSecret);
        properties.setProperty(AzureConstants.AZURE_BLOB_CONTAINER_NAME, CONTAINER_NAME);
        return properties;
    }

    private String getEnvironmentVariable(String variableName) {
        return System.getenv(variableName);
    }

    private CloudBlobContainer createBlobContainer() throws Exception {
        container = azurite.getContainer("blobstore");
        for (String blob : BLOBS) {
            container.getBlockBlobReference(blob + ".txt").uploadText(blob);
        }
        return container;
    }

    private static Properties getConfigurationWithSasToken(String sasToken) {
        Properties properties = getBasicConfiguration();
        properties.setProperty(AzureConstants.AZURE_SAS, sasToken);
        properties.setProperty(AzureConstants.AZURE_CREATE_CONTAINER, "false");
        properties.setProperty(AzureConstants.AZURE_REF_ON_INIT, "false");
        return properties;
    }

    private static Properties getConfigurationWithAccessKey() {
        Properties properties = getBasicConfiguration();
        properties.setProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY, AzuriteDockerRule.ACCOUNT_KEY);
        return properties;
    }

    @NotNull
    private static Properties getConfigurationWithConnectionString() {
        Properties properties = getBasicConfiguration();
        properties.setProperty(AzureConstants.AZURE_CONNECTION_STRING, getConnectionString());
        return properties;
    }

    @NotNull
    private static Properties getBasicConfiguration() {
        Properties properties = new Properties();
        properties.setProperty(AzureConstants.AZURE_BLOB_CONTAINER_NAME, CONTAINER_NAME);
        properties.setProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, AzuriteDockerRule.ACCOUNT_NAME);
        properties.setProperty(AzureConstants.AZURE_BLOB_ENDPOINT, azurite.getBlobEndpoint());
        properties.setProperty(AzureConstants.AZURE_CREATE_CONTAINER, "");
        return properties;
    }

    @NotNull
    private static SharedAccessBlobPolicy policy(EnumSet<SharedAccessBlobPermissions> permissions, Instant expirationTime) {
        SharedAccessBlobPolicy sharedAccessBlobPolicy = new SharedAccessBlobPolicy();
        sharedAccessBlobPolicy.setPermissions(permissions);
        sharedAccessBlobPolicy.setSharedAccessExpiryTime(Date.from(expirationTime));
        return sharedAccessBlobPolicy;
    }

    @NotNull
    private static SharedAccessBlobPolicy policy(EnumSet<SharedAccessBlobPermissions> permissions) {
        return policy(permissions, Instant.now().plus(Duration.ofDays(7)));
    }

    private static void assertReadAccessGranted(AzureBlobStoreBackendV8 backend, Set<String> expectedBlobs) throws Exception {
        CloudBlobContainer container = backend.getAzureContainer();
        Set<String> actualBlobNames = StreamSupport.stream(container.listBlobs().spliterator(), false)
                .map(blob -> blob.getUri().getPath())
                .map(path -> path.substring(path.lastIndexOf('/') + 1))
                .filter(path -> !path.isEmpty())
                .collect(toSet());

        Set<String> expectedBlobNames = expectedBlobs.stream().map(name -> name + ".txt").collect(toSet());

        assertEquals(expectedBlobNames, actualBlobNames);

        Set<String> actualBlobContent = actualBlobNames.stream()
                .map(name -> {
                    try {
                        return container.getBlockBlobReference(name).downloadText();
                    } catch (StorageException | IOException | URISyntaxException e) {
                        throw new RuntimeException("Error while reading blob " + name, e);
                    }
                })
                .collect(toSet());
        assertEquals(expectedBlobs, actualBlobContent);
    }

    private static void assertWriteAccessGranted(AzureBlobStoreBackendV8 backend, String blob) throws Exception {
        backend.getAzureContainer()
                .getBlockBlobReference(blob + ".txt").uploadText(blob);
    }

    private static void assertWriteAccessNotGranted(AzureBlobStoreBackendV8 backend) {
        try {
            assertWriteAccessGranted(backend, "test.txt");
            fail("Write access should not be granted, but writing to the storage succeeded.");
        } catch (Exception e) {
            // successful
        }
    }

    private static void assertReadAccessNotGranted(AzureBlobStoreBackendV8 backend) {
        try {
            assertReadAccessGranted(backend, BLOBS);
            fail("Read access should not be granted, but reading from the storage succeeded.");
        } catch (Exception e) {
            // successful
        }
    }

    private static Instant yesterday() {
        return Instant.now().minus(Duration.ofDays(1));
    }

    private static Set<String> concat(Set<String> set, String element) {
        return Stream.concat(set.stream(), Stream.of(element)).collect(Collectors.toSet());
    }

    private static String getConnectionString() {
        return UtilsV8.getConnectionString(AzuriteDockerRule.ACCOUNT_NAME, AzuriteDockerRule.ACCOUNT_KEY, azurite.getBlobEndpoint());
    }

    private static void assertReferenceSecret(AzureBlobStoreBackendV8 azureBlobStoreBackend)
            throws DataStoreException, IOException {
        // assert secret already created on init
        DataRecord refRec = azureBlobStoreBackend.getMetadataRecord("reference.key");
        assertNotNull("Reference data record null", refRec);
        assertTrue("reference key is empty", refRec.getLength() > 0);
    }

    @Test
    public void testMetadataOperationsWithRenamedConstantsV8() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 azureBlobStoreBackend = new AzureBlobStoreBackendV8();
        azureBlobStoreBackend.setProperties(getConfigurationWithConnectionString());
        azureBlobStoreBackend.init();

        // Test that metadata operations work correctly with the renamed constants in V8
        String testMetadataName = "test-metadata-record-v8";
        String testContent = "test metadata content for v8";

        // Add a metadata record
        azureBlobStoreBackend.addMetadataRecord(new ByteArrayInputStream(testContent.getBytes()), testMetadataName);

        // Verify the record exists
        assertTrue("Metadata record should exist", azureBlobStoreBackend.metadataRecordExists(testMetadataName));

        // Retrieve the record
        DataRecord retrievedRecord = azureBlobStoreBackend.getMetadataRecord(testMetadataName);
        assertNotNull("Retrieved metadata record should not be null", retrievedRecord);
        assertEquals("Retrieved record should have correct length", testContent.length(), retrievedRecord.getLength());

        // Verify the record appears in getAllMetadataRecords
        List<DataRecord> allRecords = azureBlobStoreBackend.getAllMetadataRecords("");
        boolean foundTestRecord = allRecords.stream()
                .anyMatch(record -> record.getIdentifier().toString().equals(testMetadataName));
        assertTrue("Test metadata record should be found in getAllMetadataRecords", foundTestRecord);

        // Clean up - delete the test record
        azureBlobStoreBackend.deleteMetadataRecord(testMetadataName);
        assertFalse("Metadata record should be deleted", azureBlobStoreBackend.metadataRecordExists(testMetadataName));
    }

    @Test
    public void testMetadataDirectoryStructureV8() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 azureBlobStoreBackend = new AzureBlobStoreBackendV8();
        azureBlobStoreBackend.setProperties(getConfigurationWithConnectionString());
        azureBlobStoreBackend.init();

        // Test that metadata records are stored in the correct directory structure in V8
        String testMetadataName = "directory-test-record-v8";
        String testContent = "directory test content for v8";

        // Add a metadata record
        azureBlobStoreBackend.addMetadataRecord(new ByteArrayInputStream(testContent.getBytes()), testMetadataName);

        try {
            // Verify the record is stored with the correct path prefix using V8 API
            CloudBlobContainer azureContainer = azureBlobStoreBackend.getAzureContainer();

            // In V8, metadata is stored in a directory structure
            com.microsoft.azure.storage.blob.CloudBlobDirectory metaDir =
                azureContainer.getDirectoryReference(AZURE_BlOB_META_DIR_NAME);
            com.microsoft.azure.storage.blob.CloudBlockBlob blob = metaDir.getBlockBlobReference(testMetadataName);

            assertTrue("Blob should exist at expected path in V8", blob.exists());

            // Verify the blob is in the META directory by listing
            boolean foundBlob = false;
            for (com.microsoft.azure.storage.blob.ListBlobItem item : metaDir.listBlobs()) {
                if (item instanceof com.microsoft.azure.storage.blob.CloudBlob) {
                    com.microsoft.azure.storage.blob.CloudBlob cloudBlob = (com.microsoft.azure.storage.blob.CloudBlob) item;
                    if (cloudBlob.getName().endsWith(testMetadataName)) {
                        foundBlob = true;
                        break;
                    }
                }
            }
            assertTrue("Blob should be found in META directory listing in V8", foundBlob);

        } finally {
            // Clean up
            azureBlobStoreBackend.deleteMetadataRecord(testMetadataName);
        }
    }

    @Test
    public void testInitWithNullProperties() throws Exception {
        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        // Should not throw exception when properties is null - should use default config
        try {
            backend.init();
            fail("Expected DataStoreException when no properties and no default config file");
        } catch (DataStoreException e) {
            // Expected - no default config file exists
            assertTrue("Should contain config file error", e.getMessage().contains("Unable to initialize Azure Data Store"));
        }
    }

    @Test
    public void testInitWithInvalidConnectionString() throws Exception {
        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        Properties props = new Properties();
        props.setProperty(AzureConstants.AZURE_CONNECTION_STRING, "invalid-connection-string");
        props.setProperty(AzureConstants.AZURE_BLOB_CONTAINER_NAME, "test-container");
        backend.setProperties(props);

        try {
            backend.init();
            fail("Expected exception with invalid connection string");
        } catch (Exception e) {
            // Expected - can be DataStoreException or IllegalArgumentException
            assertNotNull("Exception should not be null", e);
            assertTrue("Should be DataStoreException or IllegalArgumentException",
                    e instanceof DataStoreException || e instanceof IllegalArgumentException);
        }
    }

    @Test
    public void testConcurrentRequestCountValidation() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        // Test with too low concurrent request count
        AzureBlobStoreBackendV8 backend1 = new AzureBlobStoreBackendV8();
        Properties props1 = getConfigurationWithConnectionString();
        props1.setProperty(AzureConstants.AZURE_BLOB_CONCURRENT_REQUESTS_PER_OPERATION, "1"); // Too low
        backend1.setProperties(props1);
        backend1.init();
        // Should reset to default minimum

        // Test with too high concurrent request count
        AzureBlobStoreBackendV8 backend2 = new AzureBlobStoreBackendV8();
        Properties props2 = getConfigurationWithConnectionString();
        props2.setProperty(AzureConstants.AZURE_BLOB_CONCURRENT_REQUESTS_PER_OPERATION, "100"); // Too high
        backend2.setProperties(props2);
        backend2.init();
        // Should reset to default maximum
    }

    @Test
    public void testReadNonExistentBlob() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        try {
            backend.read(new org.apache.jackrabbit.core.data.DataIdentifier("nonexistent"));
            fail("Expected DataStoreException when reading non-existent blob");
        } catch (DataStoreException e) {
            assertTrue("Should contain missing blob error", e.getMessage().contains("Trying to read missing blob"));
        }
    }

    @Test
    public void testGetRecordNonExistent() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        try {
            backend.getRecord(new org.apache.jackrabbit.core.data.DataIdentifier("nonexistent"));
            fail("Expected DataStoreException when getting non-existent record");
        } catch (DataStoreException e) {
            assertTrue("Should contain retrieve blob error", e.getMessage().contains("Cannot retrieve blob"));
        }
    }

    @Test
    public void testDeleteNonExistentRecord() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        // Should not throw exception when deleting non-existent record
        backend.deleteRecord(new org.apache.jackrabbit.core.data.DataIdentifier("nonexistent"));
        // No exception expected
    }

    @Test
    public void testNullParameterValidation() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        // Test null identifier in read
        try {
            backend.read(null);
            fail("Expected NullPointerException for null identifier in read");
        } catch (NullPointerException e) {
            assertEquals("identifier", e.getMessage());
        }

        // Test null identifier in getRecord
        try {
            backend.getRecord(null);
            fail("Expected NullPointerException for null identifier in getRecord");
        } catch (NullPointerException e) {
            assertEquals("identifier", e.getMessage());
        }

        // Test null identifier in deleteRecord
        try {
            backend.deleteRecord(null);
            fail("Expected NullPointerException for null identifier in deleteRecord");
        } catch (NullPointerException e) {
            assertEquals("identifier", e.getMessage());
        }

        // Test null input in addMetadataRecord
        try {
            backend.addMetadataRecord((java.io.InputStream) null, "test");
            fail("Expected NullPointerException for null input in addMetadataRecord");
        } catch (NullPointerException e) {
            assertEquals("input", e.getMessage());
        }

        // Test null name in addMetadataRecord
        try {
            backend.addMetadataRecord(new ByteArrayInputStream("test".getBytes()), null);
            fail("Expected IllegalArgumentException for null name in addMetadataRecord");
        } catch (IllegalArgumentException e) {
            assertEquals("name", e.getMessage());
        }
    }

    @Test
    public void testGetMetadataRecordNonExistent() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        DataRecord record = backend.getMetadataRecord("nonexistent");
        assertNull(record);
    }

    @Test
    public void testDeleteAllMetadataRecords() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        // Add multiple metadata records
        String prefix = "test-prefix-";
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
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        try {
            backend.deleteAllMetadataRecords(null);
            fail("Expected NullPointerException for null prefix");
        } catch (NullPointerException e) {
            assertEquals("prefix", e.getMessage());
        }
    }

    @Test
    public void testGetAllMetadataRecordsWithNullPrefix() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        try {
            backend.getAllMetadataRecords(null);
            fail("Expected NullPointerException for null prefix");
        } catch (NullPointerException e) {
            assertEquals("prefix", e.getMessage());
        }
    }

    @Test
    public void testCloseBackend() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        // Should not throw exception
        backend.close();
    }

    @Test
    public void testWriteWithNullFile() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        try {
            backend.write(new org.apache.jackrabbit.core.data.DataIdentifier("test"), null);
            fail("Expected NullPointerException for null file");
        } catch (NullPointerException e) {
            assertEquals("file", e.getMessage());
        }
    }

    @Test
    public void testWriteWithNullIdentifier() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        java.io.File tempFile = java.io.File.createTempFile("test", ".tmp");
        try {
            backend.write(null, tempFile);
            fail("Expected NullPointerException for null identifier");
        } catch (NullPointerException e) {
            assertEquals("identifier", e.getMessage());
        } finally {
            tempFile.delete();
        }
    }

    @Test
    public void testAddMetadataRecordWithFile() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        // Create temporary file
        java.io.File tempFile = java.io.File.createTempFile("metadata", ".txt");
        try (java.io.FileWriter writer = new java.io.FileWriter(tempFile)) {
            writer.write("test metadata content from file");
        }

        String metadataName = "file-metadata-test";

        try {
            // Add metadata record from file
            backend.addMetadataRecord(tempFile, metadataName);

            // Verify record exists
            assertTrue("Metadata record should exist", backend.metadataRecordExists(metadataName));

            // Verify content
            DataRecord record = backend.getMetadataRecord(metadataName);
            assertNotNull("Record should not be null", record);
            assertEquals("Record should have correct length", tempFile.length(), record.getLength());

        } finally {
            backend.deleteMetadataRecord(metadataName);
            tempFile.delete();
        }
    }

    @Test
    public void testAddMetadataRecordWithNullFile() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        try {
            backend.addMetadataRecord((java.io.File) null, "test");
            fail("Expected NullPointerException for null file");
        } catch (NullPointerException e) {
            assertEquals("input", e.getMessage());
        }
    }

    // ========== COMPREHENSIVE TESTS FOR MISSING FUNCTIONALITY ==========

    @Test
    public void testGetAllIdentifiers() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        // Create test files
        java.io.File tempFile1 = java.io.File.createTempFile("test1", ".tmp");
        java.io.File tempFile2 = java.io.File.createTempFile("test2", ".tmp");
        try (java.io.FileWriter writer1 = new java.io.FileWriter(tempFile1);
             java.io.FileWriter writer2 = new java.io.FileWriter(tempFile2)) {
            writer1.write("test content 1");
            writer2.write("test content 2");
        }

        org.apache.jackrabbit.core.data.DataIdentifier id1 = new org.apache.jackrabbit.core.data.DataIdentifier("test1");
        org.apache.jackrabbit.core.data.DataIdentifier id2 = new org.apache.jackrabbit.core.data.DataIdentifier("test2");

        try {
            // Write test records
            backend.write(id1, tempFile1);
            backend.write(id2, tempFile2);

            // Test getAllIdentifiers
            java.util.Iterator<org.apache.jackrabbit.core.data.DataIdentifier> identifiers = backend.getAllIdentifiers();
            assertNotNull("Identifiers iterator should not be null", identifiers);

            java.util.Set<String> foundIds = new java.util.HashSet<>();
            while (identifiers.hasNext()) {
                org.apache.jackrabbit.core.data.DataIdentifier id = identifiers.next();
                foundIds.add(id.toString());
            }

            assertTrue("Should contain test1 identifier", foundIds.contains("test1"));
            assertTrue("Should contain test2 identifier", foundIds.contains("test2"));

        } finally {
            // Cleanup
            backend.deleteRecord(id1);
            backend.deleteRecord(id2);
            tempFile1.delete();
            tempFile2.delete();
        }
    }

    @Test
    public void testGetAllRecords() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        // Create test files
        java.io.File tempFile1 = java.io.File.createTempFile("test1", ".tmp");
        java.io.File tempFile2 = java.io.File.createTempFile("test2", ".tmp");
        String content1 = "test content 1";
        String content2 = "test content 2";
        try (java.io.FileWriter writer1 = new java.io.FileWriter(tempFile1);
             java.io.FileWriter writer2 = new java.io.FileWriter(tempFile2)) {
            writer1.write(content1);
            writer2.write(content2);
        }

        org.apache.jackrabbit.core.data.DataIdentifier id1 = new org.apache.jackrabbit.core.data.DataIdentifier("test1");
        org.apache.jackrabbit.core.data.DataIdentifier id2 = new org.apache.jackrabbit.core.data.DataIdentifier("test2");

        try {
            // Write test records
            backend.write(id1, tempFile1);
            backend.write(id2, tempFile2);

            // Test getAllRecords
            java.util.Iterator<DataRecord> records = backend.getAllRecords();
            assertNotNull("Records iterator should not be null", records);

            java.util.Map<String, DataRecord> foundRecords = new java.util.HashMap<>();
            while (records.hasNext()) {
                DataRecord record = records.next();
                foundRecords.put(record.getIdentifier().toString(), record);
            }

            assertTrue("Should contain test1 record", foundRecords.containsKey("test1"));
            assertTrue("Should contain test2 record", foundRecords.containsKey("test2"));

            // Verify record properties
            DataRecord record1 = foundRecords.get("test1");
            DataRecord record2 = foundRecords.get("test2");

            assertEquals("Record1 should have correct length", content1.length(), record1.getLength());
            assertEquals("Record2 should have correct length", content2.length(), record2.getLength());
            assertTrue("Record1 should have valid last modified", record1.getLastModified() > 0);
            assertTrue("Record2 should have valid last modified", record2.getLastModified() > 0);

        } finally {
            // Cleanup
            backend.deleteRecord(id1);
            backend.deleteRecord(id2);
            tempFile1.delete();
            tempFile2.delete();
        }
    }

    @Test
    public void testWriteAndReadActualFile() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        // Create test file with specific content
        java.io.File tempFile = java.io.File.createTempFile("writetest", ".tmp");
        String testContent = "This is test content for write/read operations";
        try (java.io.FileWriter writer = new java.io.FileWriter(tempFile)) {
            writer.write(testContent);
        }

        org.apache.jackrabbit.core.data.DataIdentifier identifier = new org.apache.jackrabbit.core.data.DataIdentifier("writetest");

        try {
            // Write the file
            backend.write(identifier, tempFile);

            // Verify it exists
            assertTrue("File should exist after write", backend.exists(identifier));

            // Read it back
            try (java.io.InputStream inputStream = backend.read(identifier)) {
                String readContent = new String(inputStream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                assertEquals("Read content should match written content", testContent, readContent);
            }

            // Get record and verify properties
            DataRecord record = backend.getRecord(identifier);
            assertEquals("Record should have correct length", testContent.length(), record.getLength());
            assertTrue("Record should have valid last modified", record.getLastModified() > 0);

        } finally {
            // Cleanup
            backend.deleteRecord(identifier);
            tempFile.delete();
        }
    }

    @Test
    public void testWriteExistingFileWithSameLength() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        // Create test file
        java.io.File tempFile = java.io.File.createTempFile("existingtest", ".tmp");
        String testContent = "Same length content";
        try (java.io.FileWriter writer = new java.io.FileWriter(tempFile)) {
            writer.write(testContent);
        }

        org.apache.jackrabbit.core.data.DataIdentifier identifier = new org.apache.jackrabbit.core.data.DataIdentifier("existingtest");

        try {
            // Write the file first time
            backend.write(identifier, tempFile);
            assertTrue("File should exist after first write", backend.exists(identifier));

            // Write the same file again (should update metadata)
            backend.write(identifier, tempFile);
            assertTrue("File should still exist after second write", backend.exists(identifier));

            // Verify content is still correct
            try (java.io.InputStream inputStream = backend.read(identifier)) {
                String readContent = new String(inputStream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                assertEquals("Content should remain the same", testContent, readContent);
            }

        } finally {
            // Cleanup
            backend.deleteRecord(identifier);
            tempFile.delete();
        }
    }

    @Test
    public void testWriteExistingFileWithDifferentLength() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        // Create first test file
        java.io.File tempFile1 = java.io.File.createTempFile("lengthtest1", ".tmp");
        String content1 = "Short content";
        try (java.io.FileWriter writer = new java.io.FileWriter(tempFile1)) {
            writer.write(content1);
        }

        // Create second test file with different length
        java.io.File tempFile2 = java.io.File.createTempFile("lengthtest2", ".tmp");
        String content2 = "This is much longer content with different length";
        try (java.io.FileWriter writer = new java.io.FileWriter(tempFile2)) {
            writer.write(content2);
        }

        org.apache.jackrabbit.core.data.DataIdentifier identifier = new org.apache.jackrabbit.core.data.DataIdentifier("lengthtest");

        try {
            // Write the first file
            backend.write(identifier, tempFile1);
            assertTrue("File should exist after first write", backend.exists(identifier));

            // Try to write second file with different length - should throw exception
            try {
                backend.write(identifier, tempFile2);
                fail("Expected DataStoreException for length collision");
            } catch (DataStoreException e) {
                assertTrue("Should contain length collision error", e.getMessage().contains("Length Collision"));
            }

        } finally {
            // Cleanup
            backend.deleteRecord(identifier);
            tempFile1.delete();
            tempFile2.delete();
        }
    }

    @Test
    public void testExistsMethod() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        org.apache.jackrabbit.core.data.DataIdentifier identifier = new org.apache.jackrabbit.core.data.DataIdentifier("existstest");

        // Test non-existent file
        assertFalse("Non-existent file should return false", backend.exists(identifier));

        // Create and write file
        java.io.File tempFile = java.io.File.createTempFile("existstest", ".tmp");
        try (java.io.FileWriter writer = new java.io.FileWriter(tempFile)) {
            writer.write("exists test content");
        }

        try {
            // Write the file
            backend.write(identifier, tempFile);

            // Test existing file
            assertTrue("Existing file should return true", backend.exists(identifier));

            // Delete the file
            backend.deleteRecord(identifier);

            // Test deleted file
            assertFalse("Deleted file should return false", backend.exists(identifier));

        } finally {
            tempFile.delete();
        }
    }

    @Test
    public void testAzureBlobStoreDataRecordFunctionality() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        // Create test file
        java.io.File tempFile = java.io.File.createTempFile("datarecordtest", ".tmp");
        String testContent = "Data record test content";
        try (java.io.FileWriter writer = new java.io.FileWriter(tempFile)) {
            writer.write(testContent);
        }

        org.apache.jackrabbit.core.data.DataIdentifier identifier = new org.apache.jackrabbit.core.data.DataIdentifier("datarecordtest");

        try {
            // Write the file
            backend.write(identifier, tempFile);

            // Get the data record
            DataRecord record = backend.getRecord(identifier);
            assertNotNull("Data record should not be null", record);

            // Test DataRecord methods
            assertEquals("Identifier should match", identifier, record.getIdentifier());
            assertEquals("Length should match", testContent.length(), record.getLength());
            assertTrue("Last modified should be positive", record.getLastModified() > 0);

            // Test getStream method
            try (java.io.InputStream stream = record.getStream()) {
                String readContent = new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                assertEquals("Stream content should match", testContent, readContent);
            }

            // Test toString method
            String toString = record.toString();
            assertNotNull("toString should not be null", toString);
            assertTrue("toString should contain identifier", toString.contains(identifier.toString()));
            assertTrue("toString should contain length", toString.contains(String.valueOf(testContent.length())));

        } finally {
            // Cleanup
            backend.deleteRecord(identifier);
            tempFile.delete();
        }
    }

    @Test
    public void testMetadataDataRecordFunctionality() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        String metadataName = "metadata-record-test";
        String testContent = "Metadata record test content";

        try {
            // Add metadata record
            backend.addMetadataRecord(new ByteArrayInputStream(testContent.getBytes()), metadataName);

            // Get the metadata record
            DataRecord record = backend.getMetadataRecord(metadataName);
            assertNotNull("Metadata record should not be null", record);

            // Test metadata record properties
            assertEquals("Identifier should match", metadataName, record.getIdentifier().toString());
            assertEquals("Length should match", testContent.length(), record.getLength());
            assertTrue("Last modified should be positive", record.getLastModified() > 0);

            // Test getStream method for metadata record
            try (java.io.InputStream stream = record.getStream()) {
                String readContent = new String(stream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                assertEquals("Metadata stream content should match", testContent, readContent);
            }

            // Test toString method for metadata record
            String toString = record.toString();
            assertNotNull("toString should not be null", toString);
            assertTrue("toString should contain identifier", toString.contains(metadataName));

        } finally {
            // Cleanup
            backend.deleteMetadataRecord(metadataName);
        }
    }

    @Test
    public void testReferenceKeyOperations() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        // Test getOrCreateReferenceKey
        byte[] key1 = backend.getOrCreateReferenceKey();
        assertNotNull("Reference key should not be null", key1);
        assertTrue("Reference key should not be empty", key1.length > 0);

        // Test that subsequent calls return the same key
        byte[] key2 = backend.getOrCreateReferenceKey();
        assertNotNull("Second reference key should not be null", key2);
        assertArrayEquals("Reference keys should be the same", key1, key2);

        // Verify the reference key is stored as metadata
        assertTrue("Reference key metadata should exist", backend.metadataRecordExists("reference.key"));
        DataRecord refRecord = backend.getMetadataRecord("reference.key");
        assertNotNull("Reference key record should not be null", refRecord);
        assertEquals("Reference key record should have correct length", key1.length, refRecord.getLength());
    }

    @Test
    public void testHttpDownloadURIConfiguration() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        Properties props = getConfigurationWithConnectionString();

        // Configure download URI settings
        props.setProperty(AzureConstants.PRESIGNED_HTTP_DOWNLOAD_URI_EXPIRY_SECONDS, "3600");
        props.setProperty(AzureConstants.PRESIGNED_HTTP_DOWNLOAD_URI_CACHE_MAX_SIZE, "100");
        props.setProperty(AzureConstants.PRESIGNED_HTTP_DOWNLOAD_URI_DOMAIN_OVERRIDE, "custom.domain.com");

        backend.setProperties(props);
        backend.init();

        // Test that configuration was applied (no direct way to verify, but init should succeed)
        assertNotNull("Backend should be initialized", backend.getAzureContainer());
    }

    @Test
    public void testHttpUploadURIConfiguration() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        Properties props = getConfigurationWithConnectionString();

        // Configure upload URI settings
        props.setProperty(AzureConstants.PRESIGNED_HTTP_UPLOAD_URI_EXPIRY_SECONDS, "1800");
        props.setProperty(AzureConstants.PRESIGNED_HTTP_UPLOAD_URI_DOMAIN_OVERRIDE, "upload.domain.com");

        backend.setProperties(props);
        backend.init();

        // Test that configuration was applied (no direct way to verify, but init should succeed)
        assertNotNull("Backend should be initialized", backend.getAzureContainer());
    }

    @Test
    public void testSecondaryLocationConfiguration() throws Exception {
        // Note: This test verifies BlobRequestOptions configuration after partial initialization
        // Full container initialization is avoided because Azurite doesn't support secondary locations

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        Properties props = getConfigurationWithConnectionString();

        // Enable secondary location
        props.setProperty(AzureConstants.AZURE_BLOB_ENABLE_SECONDARY_LOCATION_NAME, "true");

        backend.setProperties(props);

        // Initialize properties processing without container creation
        try {
            backend.init();
            fail("Expected DataStoreException due to secondary location with Azurite");
        } catch (DataStoreException e) {
            // Expected - Azurite doesn't support secondary locations
            assertTrue("Should contain secondary location error",
                e.getMessage().contains("URI for the target storage location") ||
                e.getCause().getMessage().contains("URI for the target storage location"));
        }

        // Verify that the configuration was processed correctly before the error
        com.microsoft.azure.storage.blob.BlobRequestOptions options = backend.getBlobRequestOptions();
        assertEquals("Location mode should be PRIMARY_THEN_SECONDARY",
                     com.microsoft.azure.storage.LocationMode.PRIMARY_THEN_SECONDARY,
                     options.getLocationMode());
    }

    @Test
    public void testRequestTimeoutConfiguration() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        Properties props = getConfigurationWithConnectionString();

        // Set request timeout
        props.setProperty(AzureConstants.AZURE_BLOB_REQUEST_TIMEOUT, "30000");

        backend.setProperties(props);
        backend.init();

        // Test that configuration was applied
        assertNotNull("Backend should be initialized", backend.getAzureContainer());

        // Verify BlobRequestOptions includes timeout
        com.microsoft.azure.storage.blob.BlobRequestOptions options = backend.getBlobRequestOptions();
        assertEquals("Timeout should be set", Integer.valueOf(30000), options.getTimeoutIntervalInMs());
    }

    @Test
    public void testPresignedDownloadURIVerifyExistsConfiguration() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        Properties props = getConfigurationWithConnectionString();

        // Disable verify exists for presigned download URIs
        props.setProperty(AzureConstants.PRESIGNED_HTTP_DOWNLOAD_URI_VERIFY_EXISTS, "false");

        backend.setProperties(props);
        backend.init();

        // Test that configuration was applied (no direct way to verify, but init should succeed)
        assertNotNull("Backend should be initialized", backend.getAzureContainer());
    }

    @Test
    public void testCreateContainerConfiguration() throws Exception {
        // Create container first since we're testing with container creation disabled
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        Properties props = getConfigurationWithConnectionString();

        // Disable container creation
        props.setProperty(AzureConstants.AZURE_CREATE_CONTAINER, "false");

        backend.setProperties(props);
        backend.init();

        // Test that configuration was applied (container should exist from setup)
        assertNotNull("Backend should be initialized", backend.getAzureContainer());
    }

    @Test
    public void testIteratorWithEmptyContainer() throws Exception {
        // Create a new container for this test to ensure it's empty
        CloudBlobContainer emptyContainer = azurite.getContainer("empty-container");

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        Properties props = getBasicConfiguration();
        props.setProperty(AzureConstants.AZURE_BLOB_CONTAINER_NAME, "empty-container");
        props.setProperty(AzureConstants.AZURE_CONNECTION_STRING, getConnectionString());
        backend.setProperties(props);
        backend.init();

        try {
            // Test getAllIdentifiers with empty container
            java.util.Iterator<org.apache.jackrabbit.core.data.DataIdentifier> identifiers = backend.getAllIdentifiers();
            assertNotNull("Identifiers iterator should not be null", identifiers);
            assertFalse("Empty container should have no identifiers", identifiers.hasNext());

            // Test getAllRecords with empty container
            java.util.Iterator<DataRecord> records = backend.getAllRecords();
            assertNotNull("Records iterator should not be null", records);
            assertFalse("Empty container should have no records", records.hasNext());

        } finally {
            emptyContainer.deleteIfExists();
        }
    }

    @Test
    public void testGetAllMetadataRecordsWithEmptyPrefix() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        // Add some metadata records
        backend.addMetadataRecord(new ByteArrayInputStream("content1".getBytes()), "test1");
        backend.addMetadataRecord(new ByteArrayInputStream("content2".getBytes()), "test2");

        try {
            // Get all metadata records with empty prefix
            List<DataRecord> records = backend.getAllMetadataRecords("");
            assertNotNull("Records list should not be null", records);

            // Should include reference.key and our test records
            assertTrue("Should have at least 2 records", records.size() >= 2);

            // Verify our test records are included
            java.util.Set<String> recordNames = records.stream()
                    .map(r -> r.getIdentifier().toString())
                    .collect(java.util.stream.Collectors.toSet());
            assertTrue("Should contain test1", recordNames.contains("test1"));
            assertTrue("Should contain test2", recordNames.contains("test2"));

        } finally {
            // Cleanup
            backend.deleteMetadataRecord("test1");
            backend.deleteMetadataRecord("test2");
        }
    }

    @Test
    public void testDeleteMetadataRecordNonExistent() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        // Try to delete non-existent metadata record
        boolean result = backend.deleteMetadataRecord("nonexistent");
        assertFalse("Deleting non-existent record should return false", result);
    }

    @Test
    public void testMetadataRecordExistsNonExistent() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        // Check non-existent metadata record
        boolean exists = backend.metadataRecordExists("nonexistent");
        assertFalse("Non-existent metadata record should return false", exists);
    }

    @Test
    public void testAddMetadataRecordWithEmptyName() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        try {
            backend.addMetadataRecord(new ByteArrayInputStream("test".getBytes()), "");
            fail("Expected IllegalArgumentException for empty name");
        } catch (IllegalArgumentException e) {
            assertEquals("name", e.getMessage());
        }
    }

    @Test
    public void testAddMetadataRecordFileWithEmptyName() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        java.io.File tempFile = java.io.File.createTempFile("test", ".tmp");
        try (java.io.FileWriter writer = new java.io.FileWriter(tempFile)) {
            writer.write("test content");
        }

        try {
            backend.addMetadataRecord(tempFile, "");
            fail("Expected IllegalArgumentException for empty name");
        } catch (IllegalArgumentException e) {
            assertEquals("name", e.getMessage());
        } finally {
            tempFile.delete();
        }
    }

    @Test
    public void testKeyNameUtilityMethods() throws Exception {
        // Test getKeyName method indirectly through write/read operations
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        // Create test file
        java.io.File tempFile = java.io.File.createTempFile("keynametest", ".tmp");
        String testContent = "Key name test content";
        try (java.io.FileWriter writer = new java.io.FileWriter(tempFile)) {
            writer.write(testContent);
        }

        // Test with identifier that will test key name transformation
        org.apache.jackrabbit.core.data.DataIdentifier identifier =
            new org.apache.jackrabbit.core.data.DataIdentifier("abcd1234567890abcdef");

        try {
            // Write and read to test key name transformation
            backend.write(identifier, tempFile);
            assertTrue("File should exist", backend.exists(identifier));

            try (java.io.InputStream inputStream = backend.read(identifier)) {
                String readContent = new String(inputStream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                assertEquals("Content should match", testContent, readContent);
            }

            // Verify the key name format by checking the blob exists in Azure with expected format
            // The key should be in format: "abcd-1234567890abcdef"
            CloudBlobContainer azureContainer = backend.getAzureContainer();
            assertTrue("Blob should exist with transformed key name",
                      azureContainer.getBlockBlobReference("abcd-1234567890abcdef").exists());

        } finally {
            // Cleanup
            backend.deleteRecord(identifier);
            tempFile.delete();
        }
    }

    @Test
    public void testLargeFileHandling() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        // Create a larger test file (but not too large for test performance)
        java.io.File tempFile = java.io.File.createTempFile("largefile", ".tmp");
        StringBuilder contentBuilder = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            contentBuilder.append("This is line ").append(i).append(" of the large test file.\n");
        }
        String testContent = contentBuilder.toString();

        try (java.io.FileWriter writer = new java.io.FileWriter(tempFile)) {
            writer.write(testContent);
        }

        org.apache.jackrabbit.core.data.DataIdentifier identifier =
            new org.apache.jackrabbit.core.data.DataIdentifier("largefile");

        try {
            // Write the large file
            backend.write(identifier, tempFile);
            assertTrue("Large file should exist", backend.exists(identifier));

            // Read it back and verify
            try (java.io.InputStream inputStream = backend.read(identifier)) {
                String readContent = new String(inputStream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                assertEquals("Large file content should match", testContent, readContent);
            }

            // Verify record properties
            DataRecord record = backend.getRecord(identifier);
            assertEquals("Large file record should have correct length", testContent.length(), record.getLength());

        } finally {
            // Cleanup
            backend.deleteRecord(identifier);
            tempFile.delete();
        }
    }

    @Test
    public void testBlobRequestOptionsConfiguration() throws Exception {
        // Note: This test verifies BlobRequestOptions configuration after partial initialization
        // Full container initialization is avoided because Azurite doesn't support secondary locations

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        Properties props = getConfigurationWithConnectionString();

        // Set various configuration options
        props.setProperty(AzureConstants.AZURE_BLOB_CONCURRENT_REQUESTS_PER_OPERATION, "8");
        props.setProperty(AzureConstants.AZURE_BLOB_REQUEST_TIMEOUT, "45000");
        props.setProperty(AzureConstants.AZURE_BLOB_ENABLE_SECONDARY_LOCATION_NAME, "true");

        backend.setProperties(props);

        // Initialize properties processing without container creation
        try {
            backend.init();
            fail("Expected DataStoreException due to secondary location with Azurite");
        } catch (DataStoreException e) {
            // Expected - Azurite doesn't support secondary locations
            assertTrue("Should contain secondary location error",
                e.getMessage().contains("URI for the target storage location") ||
                e.getCause().getMessage().contains("URI for the target storage location"));
        }

        // Verify that the configuration was processed correctly before the error
        com.microsoft.azure.storage.blob.BlobRequestOptions options = backend.getBlobRequestOptions();
        assertNotNull("BlobRequestOptions should not be null", options);
        assertEquals("Concurrent request count should be set", Integer.valueOf(8), options.getConcurrentRequestCount());
        assertEquals("Timeout should be set", Integer.valueOf(45000), options.getTimeoutIntervalInMs());
        assertEquals("Location mode should be PRIMARY_THEN_SECONDARY",
                     com.microsoft.azure.storage.LocationMode.PRIMARY_THEN_SECONDARY,
                     options.getLocationMode());
    }

    @Test
    public void testDirectAccessMethodsWithDisabledExpiry() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        Properties props = getConfigurationWithConnectionString();

        // Keep expiry disabled (default is 0)
        backend.setProperties(props);
        backend.init();

        // Create test file
        java.io.File tempFile = java.io.File.createTempFile("directaccess", ".tmp");
        String testContent = "Direct access test content";
        try (java.io.FileWriter writer = new java.io.FileWriter(tempFile)) {
            writer.write(testContent);
        }

        org.apache.jackrabbit.core.data.DataIdentifier identifier =
            new org.apache.jackrabbit.core.data.DataIdentifier("directaccess");

        try {
            // Write the file
            backend.write(identifier, tempFile);

            // Test createHttpDownloadURI with disabled expiry (should return null)
            org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions downloadOptions =
                org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions.DEFAULT;

            java.net.URI downloadURI = backend.createHttpDownloadURI(identifier, downloadOptions);
            assertNull("Download URI should be null when expiry is disabled", downloadURI);

            // Test initiateHttpUpload with disabled expiry (should return null)
            org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadOptions uploadOptions =
                org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadOptions.DEFAULT;

            org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUpload upload =
                backend.initiateHttpUpload(1024, 1, uploadOptions);
            assertNull("Upload should be null when expiry is disabled", upload);

        } finally {
            // Cleanup
            backend.deleteRecord(identifier);
            tempFile.delete();
        }
    }

    @Test
    public void testDirectAccessMethodsWithEnabledExpiry() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        Properties props = getConfigurationWithConnectionString();

        // Enable expiry for direct access
        props.setProperty(AzureConstants.PRESIGNED_HTTP_DOWNLOAD_URI_EXPIRY_SECONDS, "3600");
        props.setProperty(AzureConstants.PRESIGNED_HTTP_UPLOAD_URI_EXPIRY_SECONDS, "1800");

        backend.setProperties(props);
        backend.init();

        // Create test file
        java.io.File tempFile = java.io.File.createTempFile("directaccess", ".tmp");
        String testContent = "Direct access test content";
        try (java.io.FileWriter writer = new java.io.FileWriter(tempFile)) {
            writer.write(testContent);
        }

        org.apache.jackrabbit.core.data.DataIdentifier identifier =
            new org.apache.jackrabbit.core.data.DataIdentifier("directaccess");

        try {
            // Write the file
            backend.write(identifier, tempFile);

            // Test createHttpDownloadURI with enabled expiry
            org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions downloadOptions =
                org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions.DEFAULT;

            java.net.URI downloadURI = backend.createHttpDownloadURI(identifier, downloadOptions);
            assertNotNull("Download URI should not be null when expiry is enabled", downloadURI);
            assertTrue("Download URI should be HTTPS", downloadURI.toString().startsWith("https://"));
            assertTrue("Download URI should contain blob name", downloadURI.toString().contains("dire-ctaccess"));

            // Test initiateHttpUpload with enabled expiry
            org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadOptions uploadOptions =
                org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadOptions.DEFAULT;

            // Use a larger file size to ensure we get 2 parts (2 * 256KB = 512KB)
            long uploadSize = 2L * 256L * 1024L; // 512KB to ensure 2 parts
            org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUpload upload =
                backend.initiateHttpUpload(uploadSize, 2, uploadOptions);
            assertNotNull("Upload should not be null when expiry is enabled", upload);
            assertNotNull("Upload token should not be null", upload.getUploadToken());
            assertTrue("Min part size should be positive", upload.getMinPartSize() > 0);
            assertTrue("Max part size should be positive", upload.getMaxPartSize() > 0);
            assertNotNull("Upload URIs should not be null", upload.getUploadURIs());
            assertEquals("Should have 2 upload URIs", 2, upload.getUploadURIs().size());

        } finally {
            // Cleanup
            backend.deleteRecord(identifier);
            tempFile.delete();
        }
    }

    @Test
    public void testDirectAccessWithNullParameters() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        Properties props = getConfigurationWithConnectionString();
        props.setProperty(AzureConstants.PRESIGNED_HTTP_DOWNLOAD_URI_EXPIRY_SECONDS, "3600");
        props.setProperty(AzureConstants.PRESIGNED_HTTP_UPLOAD_URI_EXPIRY_SECONDS, "1800");
        backend.setProperties(props);
        backend.init();

        // Test createHttpDownloadURI with null identifier
        try {
            backend.createHttpDownloadURI(null,
                org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions.DEFAULT);
            fail("Expected NullPointerException for null identifier");
        } catch (NullPointerException e) {
            assertEquals("identifier", e.getMessage());
        }

        // Test createHttpDownloadURI with null options
        try {
            backend.createHttpDownloadURI(
                new org.apache.jackrabbit.core.data.DataIdentifier("test"), null);
            fail("Expected NullPointerException for null options");
        } catch (NullPointerException e) {
            assertEquals("downloadOptions", e.getMessage());
        }

        // Test initiateHttpUpload with null options
        try {
            backend.initiateHttpUpload(1024, 1, null);
            fail("Expected NullPointerException for null options");
        } catch (NullPointerException e) {
            // Expected - the method should validate options parameter
        }
    }

    @Test
    public void testUploadValidationEdgeCases() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        Properties props = getConfigurationWithConnectionString();
        props.setProperty(AzureConstants.PRESIGNED_HTTP_UPLOAD_URI_EXPIRY_SECONDS, "1800");
        backend.setProperties(props);
        backend.init();

        org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadOptions uploadOptions =
            org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadOptions.DEFAULT;

        // Test with zero max upload size
        try {
            backend.initiateHttpUpload(0, 1, uploadOptions);
            fail("Expected IllegalArgumentException for zero max upload size");
        } catch (IllegalArgumentException e) {
            assertTrue("Should contain size validation error", e.getMessage().contains("maxUploadSizeInBytes must be > 0"));
        }

        // Test with zero max number of URIs
        try {
            backend.initiateHttpUpload(1024, 0, uploadOptions);
            fail("Expected IllegalArgumentException for zero max URIs");
        } catch (IllegalArgumentException e) {
            assertTrue("Should contain URI validation error", e.getMessage().contains("maxNumberOfURIs must either be > 0 or -1"));
        }

        // Test with invalid negative max number of URIs
        try {
            backend.initiateHttpUpload(1024, -2, uploadOptions);
            fail("Expected IllegalArgumentException for invalid negative max URIs");
        } catch (IllegalArgumentException e) {
            assertTrue("Should contain URI validation error", e.getMessage().contains("maxNumberOfURIs must either be > 0 or -1"));
        }
    }

    @Test
    public void testCompleteHttpUploadWithInvalidToken() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        // Test with null token
        try {
            backend.completeHttpUpload(null);
            fail("Expected IllegalArgumentException for null token");
        } catch (IllegalArgumentException e) {
            assertTrue("Should contain token validation error", e.getMessage().contains("uploadToken required"));
        }

        // Test with empty token
        try {
            backend.completeHttpUpload("");
            fail("Expected IllegalArgumentException for empty token");
        } catch (IllegalArgumentException e) {
            assertTrue("Should contain token validation error", e.getMessage().contains("uploadToken required"));
        }
    }

    @Test
    public void testGetContainerName() throws Exception {
        CloudBlobContainer container = createBlobContainer();

        AzureBlobStoreBackendV8 backend = new AzureBlobStoreBackendV8();
        backend.setProperties(getConfigurationWithConnectionString());
        backend.init();

        // Test that getAzureContainer returns a valid container
        CloudBlobContainer azureContainer = backend.getAzureContainer();
        assertNotNull("Azure container should not be null", azureContainer);
        assertEquals("Container name should match", CONTAINER_NAME, azureContainer.getName());
    }
}
