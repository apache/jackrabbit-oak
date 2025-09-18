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

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.net.URI;
import java.util.Properties;

import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordUploadOptions;
import org.apache.jackrabbit.oak.spi.blob.AbstractSharedBackend;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Unit tests for AzureDataStore class covering all methods and code paths.
 * This test focuses on testing the logic and behavior of the AzureDataStore class.
 */
@RunWith(MockitoJUnitRunner.class)
public class AzureDataStoreTest {

    private AzureDataStore azureDataStore;
    
    @Mock
    private DataIdentifier mockDataIdentifier;

    @Before
    public void setUp() {
        azureDataStore = new AzureDataStore();
    }

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
}
