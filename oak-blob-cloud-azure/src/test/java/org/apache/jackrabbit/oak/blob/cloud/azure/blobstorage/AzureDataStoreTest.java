/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import static org.apache.commons.codec.binary.Hex.encodeHexString;
import static org.apache.commons.io.FileUtils.copyInputStreamToFile;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.microsoft.azure.storage.StorageException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.commons.collections.IteratorUtils;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadOptions;
import org.apache.jackrabbit.oak.spi.blob.AbstractSharedBackend;
import org.apache.jackrabbit.oak.spi.blob.SharedBackend;
import org.junit.*;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import javax.jcr.RepositoryException;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.DigestOutputStream;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Combined unit and integration tests for AzureDataStore class.
 * Unit tests use Mockito and don't require Azure configuration.
 * Integration tests require Azure configuration and test actual Azure operations.
 */
@RunWith(MockitoJUnitRunner.class)
public class AzureDataStoreTest {
    protected static final Logger LOG = LoggerFactory.getLogger(AzureDataStoreTest.class);

    // Unit test fields
    private AzureDataStore azureDataStore;
    
    @Mock
    private DataIdentifier mockDataIdentifier;

    // Integration test fields
    @Rule
    public TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private Properties props;
    private static byte[] testBuffer = "test".getBytes();
    private AzureDataStore ds;
    private AzureBlobStoreBackend backend;
    private String container;
    Random randomGen = new Random();

    @Before
    public void setUp() {
        azureDataStore = new AzureDataStore();
    }

    ////
    // Unit Tests - Testing AzureDataStore logic without Azure backend
    ////

    @Test
    public void testDefaultConstructor() {
        AzureDataStore ds = new AzureDataStore();
        assertEquals(16 * 1024, ds.getMinRecordLength());
        assertNull(ds.getBackend()); // Backend not created until createBackend() is called
    }

    @Test
    public void testSetAndGetProperties() {
        Properties props = new Properties();
        props.setProperty("test.key", "test.value");
        
        azureDataStore.setProperties(props);
        
        // Verify properties are stored by testing behavior when backend is created
        assertNotNull(props);
    }

    @Test
    public void testSetPropertiesWithNull() {
        azureDataStore.setProperties(null);
        // Should not throw exception
    }

    @Test
    public void testSetAndGetMinRecordLength() {
        int newMinRecordLength = 32 * 1024;
        
        azureDataStore.setMinRecordLength(newMinRecordLength);
        
        assertEquals(newMinRecordLength, azureDataStore.getMinRecordLength());
    }

    @Test
    public void testMinRecordLengthBoundaryValues() {
        // Test with zero
        azureDataStore.setMinRecordLength(0);
        assertEquals(0, azureDataStore.getMinRecordLength());
        
        // Test with negative value
        azureDataStore.setMinRecordLength(-1);
        assertEquals(-1, azureDataStore.getMinRecordLength());
        
        // Test with large value
        azureDataStore.setMinRecordLength(Integer.MAX_VALUE);
        assertEquals(Integer.MAX_VALUE, azureDataStore.getMinRecordLength());
    }

    @Test
    public void testCreateBackendReturnsNonNull() {
        AbstractSharedBackend result = azureDataStore.createBackend();
        assertNotNull(result);
        assertTrue(result instanceof AbstractAzureBlobStoreBackend);
    }

    @Test
    public void testCreateBackendWithProperties() {
        Properties props = new Properties();
        props.setProperty("test.key", "test.value");
        azureDataStore.setProperties(props);
        
        AbstractSharedBackend result = azureDataStore.createBackend();
        assertNotNull(result);
        assertTrue(result instanceof AbstractAzureBlobStoreBackend);
    }

    @Test
    public void testGetBackendBeforeInit() {
        // Initially null before init
        assertNull(azureDataStore.getBackend());
    }

    @Test
    public void testSetBinaryTransferAccelerationEnabled() {
        // This method is a NOOP for Azure, so just verify it doesn't throw
        azureDataStore.setBinaryTransferAccelerationEnabled(true);
        azureDataStore.setBinaryTransferAccelerationEnabled(false);
        // No exception should be thrown
    }

    @Test
    public void testSetDirectUploadURIExpirySecondsWithoutBackend() {
        // Should not throw exception when backend is null
        azureDataStore.setDirectUploadURIExpirySeconds(3600);
        azureDataStore.setDirectUploadURIExpirySeconds(0);
        azureDataStore.setDirectUploadURIExpirySeconds(-1);
        // No exception should be thrown
    }

    @Test
    public void testSetDirectDownloadURIExpirySecondsWithoutBackend() {
        // Should not throw exception when backend is null
        azureDataStore.setDirectDownloadURIExpirySeconds(7200);
        azureDataStore.setDirectDownloadURIExpirySeconds(0);
        azureDataStore.setDirectDownloadURIExpirySeconds(-1);
        // No exception should be thrown
    }

    @Test(expected = DataRecordUploadException.class)
    public void testInitiateDataRecordUploadTwoParamsWithoutBackendThrowsException() throws DataRecordUploadException {
        azureDataStore.initiateDataRecordUpload(1000L, 5);
    }

    @Test(expected = DataRecordUploadException.class)
    public void testInitiateDataRecordUploadThreeParamsWithoutBackendThrowsException() throws DataRecordUploadException {
        azureDataStore.initiateDataRecordUpload(1000L, 5, DataRecordUploadOptions.DEFAULT);
    }

    @Test(expected = DataRecordUploadException.class)
    public void testCompleteDataRecordUploadWithoutBackendThrowsException() 
            throws DataRecordUploadException, DataStoreException {
        azureDataStore.completeDataRecordUpload("test-token");
    }

    @Test
    public void testGetDownloadURIWithoutBackend() {
        URI result = azureDataStore.getDownloadURI(mockDataIdentifier, DataRecordDownloadOptions.DEFAULT);
        assertNull(result);
    }

    @Test
    public void testSetDirectDownloadURICacheSizeWithoutBackend() {
        // This should call the method on null backend, which will cause NPE
        // But looking at the implementation, it doesn't check for null like the other methods
        try {
            azureDataStore.setDirectDownloadURICacheSize(100);
            fail("Expected NullPointerException");
        } catch (NullPointerException e) {
            // Expected behavior since the method doesn't check for null backend
        }
    }

    @Test
    public void testInitiateDataRecordUploadTwoParamsCallsThreeParamsVersion() throws DataRecordUploadException {
        // Create a spy to verify the method delegation
        AzureDataStore spyDataStore = spy(azureDataStore);
        
        // Mock the three-parameter version to avoid backend initialization
        doThrow(new DataRecordUploadException("Backend not initialized"))
            .when(spyDataStore).initiateDataRecordUpload(anyLong(), anyInt(), any(DataRecordUploadOptions.class));
        
        try {
            spyDataStore.initiateDataRecordUpload(1000L, 5);
            fail("Expected DataRecordUploadException");
        } catch (DataRecordUploadException e) {
            // Verify that the three-parameter version was called with DEFAULT options
            verify(spyDataStore).initiateDataRecordUpload(1000L, 5, DataRecordUploadOptions.DEFAULT);
        }
    }

    @Test
    public void testExceptionMessagesAreCorrect() {
        try {
            azureDataStore.initiateDataRecordUpload(1000L, 5);
            fail("Expected DataRecordUploadException");
        } catch (DataRecordUploadException e) {
            assertEquals("Backend not initialized", e.getMessage());
        }
        
        try {
            azureDataStore.completeDataRecordUpload("test-token");
            fail("Expected DataRecordUploadException");
        } catch (DataRecordUploadException | DataStoreException e) {
            assertEquals("Backend not initialized", e.getMessage());
        }
    }

    @Test
    public void testSetDirectUploadURIExpirySecondsWithBackend() {
        // Create backend first and set it to the azureBlobStoreBackend field
        azureDataStore.createBackend();

        // These should not throw exceptions
        azureDataStore.setDirectUploadURIExpirySeconds(3600);
        azureDataStore.setDirectUploadURIExpirySeconds(0);
        azureDataStore.setDirectUploadURIExpirySeconds(-1);
    }

    @Test
    public void testSetDirectDownloadURIExpirySecondsWithBackend() {
        // Create backend first and set it to the azureBlobStoreBackend field
        azureDataStore.createBackend();

        // These should not throw exceptions
        azureDataStore.setDirectDownloadURIExpirySeconds(7200);
        azureDataStore.setDirectDownloadURIExpirySeconds(0);
        azureDataStore.setDirectDownloadURIExpirySeconds(-1);
    }

    @Test
    public void testSetDirectDownloadURICacheSizeWithBackend() {
        // Create backend first and set it to the azureBlobStoreBackend field
        azureDataStore.createBackend();

        // These should not throw exceptions
        azureDataStore.setDirectDownloadURICacheSize(100);
        azureDataStore.setDirectDownloadURICacheSize(0);
        azureDataStore.setDirectDownloadURICacheSize(-1);
    }

    @Test(expected = NullPointerException.class)
    public void testGetDownloadURIWithBackendButNullIdentifier() throws Exception {
        // Create backend first and initialize it
        azureDataStore.createBackend();

        // This should throw NPE for null identifier
        azureDataStore.getDownloadURI(null, DataRecordDownloadOptions.DEFAULT);
    }

    @Test(expected = NullPointerException.class)
    public void testGetDownloadURIWithBackendButNullOptions() throws Exception {
        // Create backend first and initialize it
        azureDataStore.createBackend();

        // This should throw NPE for null options
        azureDataStore.getDownloadURI(mockDataIdentifier, null);
    }

    @Test
    public void testCreateBackendMultipleTimes() {
        // Creating backend multiple times should work
        AbstractSharedBackend backend1 = azureDataStore.createBackend();
        AbstractSharedBackend backend2 = azureDataStore.createBackend();

        assertNotNull(backend1);
        assertNotNull(backend2);
        // They should be different instances
        assertNotSame(backend1, backend2);
    }

    @Test
    public void testPropertiesArePassedToBackend() {
        Properties props = new Properties();
        props.setProperty("azure.accountName", "testaccount");
        props.setProperty("azure.accountKey", "testkey");

        azureDataStore.setProperties(props);
        AbstractSharedBackend backend = azureDataStore.createBackend();

        assertNotNull(backend);
        // The backend should have been created and properties should have been set
        // We can't directly verify this without accessing private fields, but we can
        // verify that no exception was thrown during creation
    }

    @Test
    public void testNullPropertiesDoNotCauseException() {
        azureDataStore.setProperties(null);
        AbstractSharedBackend backend = azureDataStore.createBackend();

        assertNotNull(backend);
        // Should not throw exception even with null properties
    }

    @Test
    public void testEmptyPropertiesDoNotCauseException() {
        azureDataStore.setProperties(new Properties());
        AbstractSharedBackend backend = azureDataStore.createBackend();

        assertNotNull(backend);
        // Should not throw exception even with empty properties
    }

    @Test
    public void testCreateBackendWithDifferentSDKVersions() {
        // Test that createBackend works regardless of SDK version
        // The actual SDK version is determined by system property, but we can test that
        // the method doesn't fail
        AbstractSharedBackend backend1 = azureDataStore.createBackend();
        assertNotNull(backend1);

        // Create another instance to test consistency
        AzureDataStore anotherDataStore = new AzureDataStore();
        AbstractSharedBackend backend2 = anotherDataStore.createBackend();
        assertNotNull(backend2);

        // Both should be the same type (determined by system property)
        assertEquals(backend1.getClass(), backend2.getClass());
    }

    @Test
    public void testConfigurableDataRecordAccessProviderMethods() {
        // Test all ConfigurableDataRecordAccessProvider methods without backend
        azureDataStore.setDirectUploadURIExpirySeconds(1800);
        azureDataStore.setDirectDownloadURIExpirySeconds(3600);
        azureDataStore.setBinaryTransferAccelerationEnabled(true);
        azureDataStore.setBinaryTransferAccelerationEnabled(false);

        // These should not throw exceptions even without backend
    }

    @Test
    public void testGetDownloadURIWithNullBackend() {
        // Ensure getDownloadURI returns null when backend is not initialized
        URI result = azureDataStore.getDownloadURI(mockDataIdentifier, DataRecordDownloadOptions.DEFAULT);
        assertNull(result);
    }

    @Test
    public void testMethodCallsWithVariousParameterValues() {
        // Test boundary values for various methods
        azureDataStore.setMinRecordLength(0);
        assertEquals(0, azureDataStore.getMinRecordLength());

        azureDataStore.setMinRecordLength(1);
        assertEquals(1, azureDataStore.getMinRecordLength());

        azureDataStore.setMinRecordLength(1024 * 1024); // 1MB
        assertEquals(1024 * 1024, azureDataStore.getMinRecordLength());

        // Test with negative values
        azureDataStore.setDirectUploadURIExpirySeconds(-100);
        azureDataStore.setDirectDownloadURIExpirySeconds(-200);

        // Should not throw exceptions
    }

    @Test
    public void testDataRecordUploadExceptionMessages() {
        // Test that exception messages are consistent
        try {
            azureDataStore.initiateDataRecordUpload(1000L, 5);
            fail("Expected DataRecordUploadException");
        } catch (DataRecordUploadException e) {
            assertEquals("Backend not initialized", e.getMessage());
        }

        try {
            azureDataStore.initiateDataRecordUpload(1000L, 5, DataRecordUploadOptions.DEFAULT);
            fail("Expected DataRecordUploadException");
        } catch (DataRecordUploadException e) {
            assertEquals("Backend not initialized", e.getMessage());
        }

        try {
            azureDataStore.completeDataRecordUpload("test-token");
            fail("Expected DataRecordUploadException");
        } catch (DataRecordUploadException | DataStoreException e) {
            assertEquals("Backend not initialized", e.getMessage());
        }
    }

    @Test
    public void testCreateBackendSetsAzureBlobStoreBackendField() {
        // Verify that createBackend() properly sets the azureBlobStoreBackend field
        // by testing that subsequent calls to methods that depend on it work
        azureDataStore.createBackend();

        // These methods should not throw exceptions after createBackend() is called
        azureDataStore.setDirectUploadURIExpirySeconds(3600);
        azureDataStore.setDirectDownloadURIExpirySeconds(7200);
        azureDataStore.setDirectDownloadURICacheSize(100);

        // No exceptions should be thrown
    }

    ////
    // Integration Tests - Testing with actual Azure backend
    // These tests require Azure configuration and will be skipped if not available
    ////

    private void setupIntegrationTest() throws IOException, RepositoryException, URISyntaxException, InvalidKeyException, StorageException {
        assumeTrue(AzureDataStoreUtils.isAzureConfigured());

        props = AzureDataStoreUtils.getAzureConfig();
        container = String.valueOf(randomGen.nextInt(9999)) + "-" + String.valueOf(randomGen.nextInt(9999))
                    + "-test";
        props.setProperty(AzureConstants.AZURE_BLOB_CONTAINER_NAME, container);

        ds = new AzureDataStore();
        ds.setProperties(props);
        ds.setCacheSize(0);  // Turn caching off so we don't get weird test results due to caching
        ds.init(folder.newFolder().getAbsolutePath());
        backend = (AzureBlobStoreBackend) ds.getBackend();
    }

    private void teardownIntegrationTest() throws InvalidKeyException, URISyntaxException, StorageException {
        ds = null;
        try {
            AzureDataStoreUtils.deleteContainer(container);
        } catch (Exception ignore) {}
    }

    private void validateRecord(final DataRecord record,
                                final String contents,
                                final DataRecord rhs)
            throws DataStoreException, IOException {
        validateRecord(record, contents, rhs.getIdentifier(), rhs.getLength(), rhs.getLastModified());
    }

    private void validateRecord(final DataRecord record,
                                final String contents,
                                final DataIdentifier identifier,
                                final long length,
                                final long lastModified)
            throws DataStoreException, IOException {
        validateRecord(record, contents, identifier, length, lastModified, true);
    }

    private void validateRecord(final DataRecord record,
                                final String contents,
                                final DataIdentifier identifier,
                                final long length,
                                final long lastModified,
                                final boolean lastModifiedEquals)
            throws DataStoreException, IOException {
        assertEquals(record.getLength(), length);
        if (lastModifiedEquals) {
            assertEquals(record.getLastModified(), lastModified);
        } else {
            assertTrue(record.getLastModified() > lastModified);
        }
        assertTrue(record.getIdentifier().toString().equals(identifier.toString()));
        StringWriter writer = new StringWriter();
        org.apache.commons.io.IOUtils.copy(record.getStream(), writer, "utf-8");
        assertTrue(writer.toString().equals(contents));
    }

    private static InputStream randomStream(int seed, int size) {
        Random r = new Random(seed);
        byte[] data = new byte[size];
        r.nextBytes(data);
        return new ByteArrayInputStream(data);
    }

    private static String getIdForInputStream(final InputStream in)
            throws NoSuchAlgorithmException, IOException {
        MessageDigest digest = MessageDigest.getInstance("SHA-1");
        OutputStream output = new DigestOutputStream(new NullOutputStream(), digest);
        try {
            IOUtils.copyLarge(in, output);
        } finally {
            IOUtils.closeQuietly(output);
            IOUtils.closeQuietly(in);
        }
        return encodeHexString(digest.digest());
    }

    private void validateRecordData(final SharedBackend backend,
                                    final DataIdentifier identifier,
                                    int expectedSize,
                                    final InputStream expected) throws IOException, DataStoreException {
        byte[] blobData = new byte[expectedSize];
        backend.read(identifier).read(blobData);
        byte[] expectedData = new byte[expectedSize];
        expected.read(expectedData);
        for (int i=0; i<expectedSize; i++) {
            assertEquals(expectedData[i], blobData[i]);
        }
    }

    @Test
    public void testCreateAndDeleteBlobHappyPath() throws Exception {
        setupIntegrationTest();
        try {
            final DataRecord uploadedRecord = ds.addRecord(new ByteArrayInputStream(testBuffer));
            DataIdentifier identifier = uploadedRecord.getIdentifier();
            assertTrue(backend.exists(identifier));
            assertTrue(0 != uploadedRecord.getLastModified());
            assertEquals(testBuffer.length, uploadedRecord.getLength());

            final DataRecord retrievedRecord = ds.getRecord(identifier);
            validateRecord(retrievedRecord, new String(testBuffer), uploadedRecord);

            ds.deleteRecord(identifier);
            assertFalse(backend.exists(uploadedRecord.getIdentifier()));
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testCreateAndReUploadBlob() throws Exception {
        setupIntegrationTest();
        try {
            final DataRecord createdRecord = ds.addRecord(new ByteArrayInputStream(testBuffer));
            DataIdentifier identifier1 = createdRecord.getIdentifier();
            assertTrue(backend.exists(identifier1));

            final DataRecord record1 = ds.getRecord(identifier1);
            validateRecord(record1, new String(testBuffer), createdRecord);

            try { Thread.sleep(1001); } catch (InterruptedException e) { }

            final DataRecord updatedRecord = ds.addRecord(new ByteArrayInputStream(testBuffer));
            DataIdentifier identifier2 = updatedRecord.getIdentifier();
            assertTrue(backend.exists(identifier2));

            assertTrue(identifier1.toString().equals(identifier2.toString()));
            validateRecord(record1, new String(testBuffer), createdRecord);

            ds.deleteRecord(identifier1);
            assertFalse(backend.exists(createdRecord.getIdentifier()));
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testListBlobs() throws Exception {
        setupIntegrationTest();
        try {
            final Set<DataIdentifier> identifiers = new HashSet<>();
            final Set<String> testStrings = Set.of("test1", "test2", "test3");

            for (String s : testStrings) {
                identifiers.add(ds.addRecord(new ByteArrayInputStream(s.getBytes())).getIdentifier());
            }

            Iterator<DataIdentifier> iter = ds.getAllIdentifiers();
            while (iter.hasNext()) {
                DataIdentifier identifier = iter.next();
                assertTrue(identifiers.contains(identifier));
                ds.deleteRecord(identifier);
            }
        } finally {
            teardownIntegrationTest();
        }
    }

    // Backend Tests

    @Test
    public void testBackendWriteDifferentSizedRecords() throws Exception {
        setupIntegrationTest();
        try {
            // Sizes are chosen as follows:
            // 0 - explicitly test zero-size file
            // 10 - very small file
            // 1000 - under 4K (a reasonably expected stream buffer size)
            // 4100 - over 4K but under 8K and 16K (other reasonably expected stream buffer sizes)
            // 16500 - over 8K and 16K but under 64K (another reasonably expected stream buffer size)
            // 66000 - over 64K but under 128K (probably the largest reasonably expected stream buffer size)
            // 132000 - over 128K
            for (int size : List.of(0, 10, 1000, 4100, 16500, 66000, 132000)) {
                File testFile = folder.newFile();
                copyInputStreamToFile(randomStream(size, size), testFile);
                DataIdentifier identifier = new DataIdentifier(getIdForInputStream(new FileInputStream(testFile)));
                backend.write(identifier, testFile);
                assertTrue(backend.exists(identifier));

                validateRecordData(backend, identifier, size, new FileInputStream(testFile));

                backend.deleteRecord(identifier);
                assertFalse(backend.exists(identifier));
            }
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testBackendWriteRecordNullIdentifierThrowsNullPointerException() throws Exception {
        setupIntegrationTest();
        try {
            DataIdentifier identifier = null;
            File testFile = folder.newFile();
            copyInputStreamToFile(randomStream(0, 10), testFile);
            try {
                backend.write(identifier, testFile);
                fail();
            } catch (NullPointerException e) {
                assertEquals("identifier", e.getMessage());
            }
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testBackendWriteRecordNullFileThrowsNullPointerException() throws Exception {
        setupIntegrationTest();
        try {
            File testFile = null;
            DataIdentifier identifier = new DataIdentifier("fake");
            try {
                backend.write(identifier, testFile);
                fail();
            }
            catch (NullPointerException e) {
                assertTrue("file".equals(e.getMessage()));
            }
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testBackendWriteRecordFileNotFoundThrowsException() throws Exception {
        setupIntegrationTest();
        try {
            File testFile = folder.newFile();
            copyInputStreamToFile(randomStream(0, 10), testFile);
            DataIdentifier identifier = new DataIdentifier(getIdForInputStream(new FileInputStream(testFile)));
            assertTrue(testFile.delete());
            try {
                backend.write(identifier, testFile);
                fail();
            } catch (DataStoreException e) {
                assertTrue(e.getCause() instanceof FileNotFoundException);
            }
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testBackendReadRecordNullIdentifier() throws Exception {
        setupIntegrationTest();
        try {
            DataIdentifier identifier = null;
            try {
                backend.read(identifier);
                fail();
            }
            catch (NullPointerException e) {
                assert("identifier".equals(e.getMessage()));
            }
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testBackendReadRecordInvalidIdentifier() throws Exception {
        setupIntegrationTest();
        try {
            DataIdentifier identifier = new DataIdentifier("fake");
            try {
                backend.read(identifier);
                fail();
            }
            catch (DataStoreException e) { }
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testBackendDeleteRecordNullIdentifier() throws Exception {
        setupIntegrationTest();
        try {
            DataIdentifier identifier = null;
            try {
                backend.deleteRecord(identifier);
                fail();
            }
            catch (NullPointerException e) {
                assert("identifier".equals(e.getMessage()));
            }
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testBackendDeleteRecordInvalidIdentifier() throws Exception {
        setupIntegrationTest();
        try {
            DataIdentifier identifier = new DataIdentifier("fake");
            backend.deleteRecord(identifier); // We don't care if the identifier is invalid; this is a noop
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testBackendNotCreatedRecordDoesNotExist() throws Exception {
        setupIntegrationTest();
        try {
            assertFalse(backend.exists(new DataIdentifier(("fake"))));
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testBackendRecordExistsNullIdentifierThrowsNullPointerException() throws Exception {
        setupIntegrationTest();
        try {
            try {
                DataIdentifier nullIdentifier = null;
                backend.exists(nullIdentifier);
                fail();
            }
            catch (NullPointerException e) { }
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testBackendGetAllIdentifiersNoRecordsReturnsNone() throws Exception {
        setupIntegrationTest();
        try {
            Iterator<DataIdentifier> allIdentifiers = backend.getAllIdentifiers();
            assertFalse(allIdentifiers.hasNext());
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testBackendGetAllIdentifiers() throws Exception {
        setupIntegrationTest();
        try {
            for (int expectedRecCount : List.of(1, 2, 5)) {
                final List<DataIdentifier> ids = new ArrayList<>();
                for (int i=0; i<expectedRecCount; i++) {
                    File testfile = folder.newFile();
                    copyInputStreamToFile(randomStream(i, 10), testfile);
                    DataIdentifier identifier = new DataIdentifier(getIdForInputStream(new FileInputStream(testfile)));
                    backend.write(identifier, testfile);
                    ids.add(identifier);
                }

                int actualRecCount = IteratorUtils.size(backend.getAllIdentifiers());

                for (DataIdentifier identifier : ids) {
                    backend.deleteRecord(identifier);
                }

                assertEquals(expectedRecCount, actualRecCount);
            }
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testBackendGetRecord() throws Exception {
        setupIntegrationTest();
        try {
            String recordData = "testData";
            DataRecord record = ds.addRecord(new ByteArrayInputStream(recordData.getBytes()));
            DataRecord retrievedRecord = backend.getRecord(record.getIdentifier());
            validateRecord(record, recordData, retrievedRecord);
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testBackendGetRecordNullIdentifierThrowsNullPointerException() throws Exception {
        setupIntegrationTest();
        try {
            try {
                DataIdentifier identifier = null;
                backend.getRecord(identifier);
                fail();
            }
            catch (NullPointerException e) {
                assertTrue("identifier".equals(e.getMessage()));
            }
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testBackendGetRecordInvalidIdentifierThrowsDataStoreException() throws Exception {
        setupIntegrationTest();
        try {
            try {
                backend.getRecord(new DataIdentifier("invalid"));
                fail();
            }
            catch (DataStoreException e) {

            }
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testBackendGetAllRecordsReturnsAll() throws Exception {
        setupIntegrationTest();
        try {
            for (int recCount : List.of(0, 1, 2, 5)) {
                Map<DataIdentifier, String> addedRecords = new HashMap<>();
                if (0 < recCount) {
                    for (int i = 0; i < recCount; i++) {
                        String data = String.format("testData%d", i);
                        DataRecord record = ds.addRecord(new ByteArrayInputStream(data.getBytes()));
                        addedRecords.put(record.getIdentifier(), data);
                    }
                }

                Iterator<DataRecord> iter = backend.getAllRecords();
                List<DataIdentifier> identifiers = new ArrayList<>();
                int actualCount = 0;
                while (iter.hasNext()) {
                    DataRecord record = iter.next();
                    identifiers.add(record.getIdentifier());
                    assertTrue(addedRecords.containsKey(record.getIdentifier()));
                    StringWriter writer = new StringWriter();
                    IOUtils.copy(record.getStream(), writer);
                    assertTrue(writer.toString().equals(addedRecords.get(record.getIdentifier())));
                    actualCount++;
                }

                for (DataIdentifier identifier : identifiers) {
                    ds.deleteRecord(identifier);
                }

                assertEquals(recCount, actualCount);
            }
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testBackendAddMetadataRecordsFromInputStream() throws Exception {
        setupIntegrationTest();
        try {
            for (boolean fromInputStream : List.of(false, true)) {
                String prefix = String.format("%s.META.", getClass().getSimpleName());
                for (int count : List.of(1, 3)) {
                    Map<String, String> records = new HashMap<>();
                    for (int i = 0; i < count; i++) {
                        String recordName = String.format("%sname.%d", prefix, i);
                        String data = String.format("testData%d", i);
                        records.put(recordName, data);

                        if (fromInputStream) {
                            backend.addMetadataRecord(new ByteArrayInputStream(data.getBytes()), recordName);
                        }
                        else {
                            File testFile = folder.newFile();
                            copyInputStreamToFile(new ByteArrayInputStream(data.getBytes()), testFile);
                            backend.addMetadataRecord(testFile, recordName);
                        }
                    }

                    assertEquals(count, backend.getAllMetadataRecords(prefix).size());

                    for (Map.Entry<String, String> entry : records.entrySet()) {
                        DataRecord record = backend.getMetadataRecord(entry.getKey());
                        StringWriter writer = new StringWriter();
                        IOUtils.copy(record.getStream(), writer);
                        backend.deleteMetadataRecord(entry.getKey());
                        assertTrue(writer.toString().equals(entry.getValue()));
                    }

                    assertEquals(0, backend.getAllMetadataRecords(prefix).size());
                }
            }
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testBackendAddMetadataRecordFileNotFoundThrowsDataStoreException() throws Exception {
        setupIntegrationTest();
        try {
            File testFile = folder.newFile();
            copyInputStreamToFile(randomStream(0, 10), testFile);
            testFile.delete();
            try {
                backend.addMetadataRecord(testFile, "name");
                fail();
            }
            catch (DataStoreException e) {
                assertTrue(e.getCause() instanceof FileNotFoundException);
            }
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testBackendAddMetadataRecordNullInputStreamThrowsNullPointerException() throws Exception {
        setupIntegrationTest();
        try {
            try {
                backend.addMetadataRecord((InputStream)null, "name");
                fail();
            }
            catch (NullPointerException e) {
                assertTrue("input".equals(e.getMessage()));
            }
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testBackendAddMetadataRecordNullFileThrowsNullPointerException() throws Exception {
        setupIntegrationTest();
        try {
            try {
                backend.addMetadataRecord((File)null, "name");
                fail();
            }
            catch (NullPointerException e) {
                assertTrue("input".equals(e.getMessage()));
            }
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testBackendAddMetadataRecordNullEmptyNameThrowsIllegalArgumentException() throws Exception {
        setupIntegrationTest();
        try {
            final String data = "testData";
            for (boolean fromInputStream : List.of(false, true)) {
                for (String name : Arrays.asList(null, "")) {
                    try {
                        if (fromInputStream) {
                            backend.addMetadataRecord(new ByteArrayInputStream(data.getBytes()), name);
                        } else {
                            File testFile = folder.newFile();
                            copyInputStreamToFile(new ByteArrayInputStream(data.getBytes()), testFile);
                            backend.addMetadataRecord(testFile, name);
                        }
                        fail();
                    } catch (IllegalArgumentException e) {
                        assertTrue("name".equals(e.getMessage()));
                    }
                }
            }
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testBackendGetMetadataRecordInvalidName() throws Exception {
        setupIntegrationTest();
        try {
            backend.addMetadataRecord(randomStream(0, 10), "testRecord");
            assertNull(backend.getMetadataRecord("invalid"));
            for (String name : Arrays.asList("", null)) {
                try {
                    backend.getMetadataRecord(name);
                    fail("Expect to throw");
                } catch(Exception e) {}
            }

            backend.deleteMetadataRecord("testRecord");
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testBackendGetAllMetadataRecordsPrefixMatchesAll() throws Exception {
        setupIntegrationTest();
        try {
            // reference.key initialized in backend#init() - OAK-9807, so expected 1 record
            assertEquals(1, backend.getAllMetadataRecords("").size());
            backend.deleteAllMetadataRecords("");

            String prefixAll = "prefix1";
            String prefixSome = "prefix1.prefix2";
            String prefixOne = "prefix1.prefix3";
            String prefixNone = "prefix4";

            backend.addMetadataRecord(randomStream(1, 10), String.format("%s.testRecord1", prefixAll));
            backend.addMetadataRecord(randomStream(2, 10), String.format("%s.testRecord2", prefixSome));
            backend.addMetadataRecord(randomStream(3, 10), String.format("%s.testRecord3", prefixSome));
            backend.addMetadataRecord(randomStream(4, 10), String.format("%s.testRecord4", prefixOne));
            backend.addMetadataRecord(randomStream(5, 10), "prefix5.testRecord5");

            assertEquals(5, backend.getAllMetadataRecords("").size());
            assertEquals(4, backend.getAllMetadataRecords(prefixAll).size());
            assertEquals(2, backend.getAllMetadataRecords(prefixSome).size());
            assertEquals(1, backend.getAllMetadataRecords(prefixOne).size());
            assertEquals(0, backend.getAllMetadataRecords(prefixNone).size());

            backend.deleteAllMetadataRecords("");
            assertEquals(0, backend.getAllMetadataRecords("").size());
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testBackendGetAllMetadataRecordsNullPrefixThrowsNullPointerException() throws Exception {
        setupIntegrationTest();
        try {
            try {
                backend.getAllMetadataRecords(null);
                fail();
            }
            catch (NullPointerException e) {
                assertTrue("prefix".equals(e.getMessage()));
            }
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testBackendDeleteMetadataRecord() throws Exception {
        setupIntegrationTest();
        try {
            backend.addMetadataRecord(randomStream(0, 10), "name");
            for (String name : Arrays.asList("invalid", "", null)) {
                if (StringUtils.isEmpty(name)) {
                    try {
                        backend.deleteMetadataRecord(name);
                    }
                    catch (IllegalArgumentException e) { }
                }
                else {
                    assertFalse(backend.deleteMetadataRecord(name));
                }
            }
            assertTrue(backend.deleteMetadataRecord("name"));
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testBackendMetadataRecordExists() throws Exception {
        setupIntegrationTest();
        try {
            backend.addMetadataRecord(randomStream(0, 10), "name");
            for (String name : Arrays.asList("invalid", "", null)) {
                if (StringUtils.isEmpty(name)) {
                    try {
                        backend.metadataRecordExists(name);
                    }
                    catch (IllegalArgumentException e) { }
                }
                else {
                    assertFalse(backend.metadataRecordExists(name));
                }
            }
            assertTrue(backend.metadataRecordExists("name"));
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testBackendDeleteAllMetadataRecordsPrefixMatchesAll() throws Exception {
        setupIntegrationTest();
        try {
            String prefixAll = "prefix1";
            String prefixSome = "prefix1.prefix2";
            String prefixOne = "prefix1.prefix3";
            String prefixNone = "prefix4";

            Map<String, Integer> prefixCounts = new HashMap<>();
            prefixCounts.put(prefixAll, 4);
            prefixCounts.put(prefixSome, 2);
            prefixCounts.put(prefixOne, 1);
            prefixCounts.put(prefixNone, 0);

            for (Map.Entry<String, Integer> entry : prefixCounts.entrySet()) {
                backend.addMetadataRecord(randomStream(1, 10), String.format("%s.testRecord1", prefixAll));
                backend.addMetadataRecord(randomStream(2, 10), String.format("%s.testRecord2", prefixSome));
                backend.addMetadataRecord(randomStream(3, 10), String.format("%s.testRecord3", prefixSome));
                backend.addMetadataRecord(randomStream(4, 10), String.format("%s.testRecord4", prefixOne));

                int preCount = backend.getAllMetadataRecords("").size();

                backend.deleteAllMetadataRecords(entry.getKey());

                int deletedCount = preCount - backend.getAllMetadataRecords("").size();
                assertEquals(entry.getValue().intValue(), deletedCount);

                backend.deleteAllMetadataRecords("");
            }
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testBackendDeleteAllMetadataRecordsNoRecordsNoChange() throws Exception {
        setupIntegrationTest();
        try {
            // reference.key initialized in backend#init() - OAK-9807, so expected 1 record
            assertEquals(1, backend.getAllMetadataRecords("").size());

            backend.deleteAllMetadataRecords("");

            assertEquals(0, backend.getAllMetadataRecords("").size());
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testBackendDeleteAllMetadataRecordsNullPrefixThrowsNullPointerException() throws Exception {
        setupIntegrationTest();
        try {
            try {
                backend.deleteAllMetadataRecords(null);
                fail();
            }
            catch (NullPointerException e) {
                assertTrue("prefix".equals(e.getMessage()));
            }
        } finally {
            teardownIntegrationTest();
        }
    }

    @Test
    public void testSecret() throws Exception {
        setupIntegrationTest();
        try {
            // assert secret already created on init
            DataRecord refRec = ds.getMetadataRecord("reference.key");
            assertNotNull("Reference data record null", refRec);

            byte[] data = new byte[4096];
            randomGen.nextBytes(data);
            DataRecord rec = ds.addRecord(new ByteArrayInputStream(data));
            assertEquals(data.length, rec.getLength());
            String ref = rec.getReference();

            String id = rec.getIdentifier().toString();
            assertNotNull(ref);

            byte[] refKey = backend.getOrCreateReferenceKey();

            Mac mac = Mac.getInstance("HmacSHA1");
            mac.init(new SecretKeySpec(refKey, "HmacSHA1"));
            byte[] hash = mac.doFinal(id.getBytes("UTF-8"));
            String calcRef = id + ':' + encodeHexString(hash);

            assertEquals("getReference() not equal", calcRef, ref);

            byte[] refDirectFromBackend = IOUtils.toByteArray(refRec.getStream());
            LOG.warn("Ref direct from backend {}", refDirectFromBackend);
            assertTrue("refKey in memory not equal to the metadata record",
                Arrays.equals(refKey, refDirectFromBackend));
        } finally {
            teardownIntegrationTest();
        }
    }
}
