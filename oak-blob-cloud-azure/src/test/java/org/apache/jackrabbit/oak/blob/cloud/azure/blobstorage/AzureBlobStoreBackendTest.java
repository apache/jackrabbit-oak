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
package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import com.google.common.collect.ImmutableSet;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.SharedAccessBlobPermissions;
import com.microsoft.azure.storage.blob.SharedAccessBlobPolicy;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.StreamSupport;
import org.apache.jackrabbit.core.data.DataIdentifier;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.plugins.blob.datastore.directaccess.DataRecordDownloadOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.ADD;
import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.CREATE;
import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.LIST;
import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.READ;
import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.WRITE;
import static java.util.stream.Collectors.toSet;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureTestHelper.getConfigurationWithAccessKey;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureTestHelper.getConfigurationWithConnectionString;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureTestHelper.getConfigurationWithSasToken;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureTestHelper.getContainer;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureTestHelper.policy;
import static org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureTestHelper.writeBlob;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class AzureBlobStoreBackendTest {
    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    private static final EnumSet<SharedAccessBlobPermissions> READ_ONLY = EnumSet.of(READ, LIST);
    private static final EnumSet<SharedAccessBlobPermissions> READ_WRITE = EnumSet.of(READ, LIST, CREATE, WRITE, ADD);
    private static final ImmutableSet<String> BLOBS = ImmutableSet.of("blob1", "blob2");

    private CloudBlobContainer container;

    @Before
    public void setUp() throws Exception {
        container = getContainer(azurite);
    }

    @After
    public void tearDown() throws Exception {
        container.deleteIfExists();
    }

    @Test
    public void initWithSharedAccessSignature_readOnly() throws Exception {
        CloudBlobContainer container = initBlobContainer();
        String sasToken = container.generateSharedAccessSignature(policy(READ_ONLY), null);

        AzureBlobStoreBackend azureBlobStoreBackend = new AzureBlobStoreBackend();
        azureBlobStoreBackend.setProperties(getConfigurationWithSasToken(sasToken, azurite));
        
        azureBlobStoreBackend.init();
        
        assertWriteAccessNotGranted(azureBlobStoreBackend);
        assertReadAccessGranted(azureBlobStoreBackend, BLOBS);
    }

    @Test
    public void initWithSharedAccessSignature_readWrite() throws Exception {
        CloudBlobContainer container = initBlobContainer();
        String sasToken = container.generateSharedAccessSignature(policy(READ_WRITE), null);

        AzureBlobStoreBackend azureBlobStoreBackend = new AzureBlobStoreBackend();
        azureBlobStoreBackend.setProperties(getConfigurationWithSasToken(sasToken, azurite));
        
        azureBlobStoreBackend.init();

        assertWriteAccessGranted(azureBlobStoreBackend, "file");
        assertReadAccessGranted(azureBlobStoreBackend, concat(BLOBS, "file"));
    }

    @Test
    public void connectWithSharedAccessSignatureURL_expired() throws Exception {
        CloudBlobContainer container = initBlobContainer();
        SharedAccessBlobPolicy expiredPolicy = policy(READ_WRITE, yesterday());
        String sasToken = container.generateSharedAccessSignature(expiredPolicy, null);

        AzureBlobStoreBackend azureBlobStoreBackend = new AzureBlobStoreBackend();
        azureBlobStoreBackend.setProperties(getConfigurationWithSasToken(sasToken, azurite));

        azureBlobStoreBackend.init();
        
        assertWriteAccessNotGranted(azureBlobStoreBackend);
        assertReadAccessNotGranted(azureBlobStoreBackend);
    }

    @Test
    public void initWithAccessKey() throws Exception {
        AzureBlobStoreBackend azureBlobStoreBackend = new AzureBlobStoreBackend();
        azureBlobStoreBackend.setProperties(getConfigurationWithAccessKey(azurite));

        azureBlobStoreBackend.init();
        
        assertWriteAccessGranted(azureBlobStoreBackend, "file");
        assertReadAccessGranted(azureBlobStoreBackend, ImmutableSet.of("file"));
    }

    @Test
    public void initWithConnectionURL() throws Exception {
        AzureBlobStoreBackend azureBlobStoreBackend = new AzureBlobStoreBackend();
        azureBlobStoreBackend.setProperties(getConfigurationWithConnectionString(azurite));

        azureBlobStoreBackend.init();

        assertWriteAccessGranted(azureBlobStoreBackend, "file");
        assertReadAccessGranted(azureBlobStoreBackend, ImmutableSet.of("file"));
    }

    @Test
    public void createHttpDownloadURI() throws DataStoreException {
        AzureBlobStoreBackend azureBlobStoreBackend = new AzureBlobStoreBackend();
        azureBlobStoreBackend.setProperties(getConfigurationWithAccessKey(azurite));
        azureBlobStoreBackend.init();
        DataIdentifier identifier = writeBlob(azureBlobStoreBackend, "test123456");

        URI downloadURI = azureBlobStoreBackend.createHttpDownloadURI(identifier, DataRecordDownloadOptions.DEFAULT);
        
        assertNotNull(downloadURI);
        assertEquals(AzuriteDockerRule.ACCOUNT_NAME + ".blob.core.windows.net", downloadURI.getHost());
        assertEquals("/blobstore/test-123456", downloadURI.getPath());
        assertValidDownloadUriQuery(downloadURI);
    }

    @Test
    public void createHttpDownloadURI_withCustomSasTokenGenerator() throws DataStoreException {
        AzureBlobStoreBackend azureBlobStoreBackend = new AzureBlobStoreBackend();
        azureBlobStoreBackend.setProperties(getConfigurationWithAccessKey(azurite));
        azureBlobStoreBackend.setSasTokenGenerator((blob, policy, optionalHeaders) -> "customSasToken");
        azureBlobStoreBackend.init();
        DataIdentifier identifier = writeBlob(azureBlobStoreBackend);

        URI downloadURI = azureBlobStoreBackend.createHttpDownloadURI(identifier, DataRecordDownloadOptions.DEFAULT);
        
        assertEquals("customSasToken", downloadURI.getQuery());
    }

    private static void assertValidDownloadUriQuery(URI downloadURI) {
        String query = downloadURI.getQuery();
        assertTrue("Expected 'sig' property in query, but was " + query, query.contains("sig="));
        assertTrue("Expected 'rscc' property in query, but was " + query, query.contains("rscc=private, max-age=60, immutable"));
        assertTrue("Expected 'sp' property in query, but was " + query, query.contains("sp=r"));
        assertTrue("Expected 'sr' property in query, but was " + query, query.contains("sr=b"));
        assertTrue("Expected 'se' property in query, but was " + query, query.contains("se="));
        assertTrue("Expected 'sv' property in query, but was " + query, query.contains("sv="));
    }

    private CloudBlobContainer initBlobContainer() throws Exception {
        for (String blob : BLOBS) {
            container.getBlockBlobReference(blob + ".txt").uploadText(blob);
        }
        return container;
    }

    private static void assertReadAccessGranted(AzureBlobStoreBackend backend, Set<String> expectedBlobs) throws Exception {
        CloudBlobContainer container = backend.getAzureContainer();
        Set<String> actualBlobNames = StreamSupport.stream(container.listBlobs().spliterator(), false)
            .map(blob -> blob.getUri().getPath())
            .map(path -> path.substring(path.lastIndexOf('/') + 1))
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

    private static void assertWriteAccessGranted(AzureBlobStoreBackend backend, String blob) throws Exception {
        backend.getAzureContainer()
            .getBlockBlobReference(blob + ".txt").uploadText(blob);
    }

    private static void assertWriteAccessNotGranted(AzureBlobStoreBackend backend) {
        try {
            assertWriteAccessGranted(backend, "test.txt");
            fail("Write access should not be granted, but writing to the storage succeeded.");
        } catch (Exception e) {
            // successful
        }
    }

    private static void assertReadAccessNotGranted(AzureBlobStoreBackend backend) {
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

    private static ImmutableSet<String> concat(ImmutableSet<String> set, String element) {
        return ImmutableSet.<String>builder().addAll(set).add(element).build();
    }

}