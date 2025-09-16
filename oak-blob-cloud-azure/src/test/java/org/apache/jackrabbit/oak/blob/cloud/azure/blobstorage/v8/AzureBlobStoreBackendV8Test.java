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
}
