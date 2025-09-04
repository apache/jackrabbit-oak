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

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.SharedAccessBlobPermissions;
import com.microsoft.azure.storage.blob.SharedAccessBlobPolicy;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.EnumSet;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNotNull;

public class AzureBlobStoreBackendTest {
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

        AzureBlobStoreBackend azureBlobStoreBackend = new AzureBlobStoreBackend();
        azureBlobStoreBackend.setProperties(getConfigurationWithSasToken(sasToken));

        azureBlobStoreBackend.init();

        assertWriteAccessNotGranted(azureBlobStoreBackend);
        assertReadAccessGranted(azureBlobStoreBackend, BLOBS);
    }

    @Test
    public void initWithSharedAccessSignature_readWrite() throws Exception {
        CloudBlobContainer container = createBlobContainer();
        String sasToken = container.generateSharedAccessSignature(policy(READ_WRITE), null);

        AzureBlobStoreBackend azureBlobStoreBackend = new AzureBlobStoreBackend();
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

        AzureBlobStoreBackend azureBlobStoreBackend = new AzureBlobStoreBackend();
        azureBlobStoreBackend.setProperties(getConfigurationWithSasToken(sasToken));

        azureBlobStoreBackend.init();

        assertWriteAccessNotGranted(azureBlobStoreBackend);
        assertReadAccessNotGranted(azureBlobStoreBackend);
    }

    @Test
    public void initWithAccessKey() throws Exception {
        AzureBlobStoreBackend azureBlobStoreBackend = new AzureBlobStoreBackend();
        azureBlobStoreBackend.setProperties(getConfigurationWithAccessKey());

        azureBlobStoreBackend.init();

        assertWriteAccessGranted(azureBlobStoreBackend, "file");
        assertReadAccessGranted(azureBlobStoreBackend, Set.of("file"));
    }

    @Test
    public void initWithConnectionURL() throws Exception {
        AzureBlobStoreBackend azureBlobStoreBackend = new AzureBlobStoreBackend();
        azureBlobStoreBackend.setProperties(getConfigurationWithConnectionString());

        azureBlobStoreBackend.init();

        assertWriteAccessGranted(azureBlobStoreBackend, "file");
        assertReadAccessGranted(azureBlobStoreBackend, Set.of("file"));
    }

    @Test
    public void initSecret() throws Exception {
        AzureBlobStoreBackend azureBlobStoreBackend = new AzureBlobStoreBackend();
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

        AzureBlobStoreBackend azureBlobStoreBackend = new AzureBlobStoreBackend();
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

    private static void assertReadAccessGranted(AzureBlobStoreBackend backend, Set<String> expectedBlobs) throws Exception {
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

    private static Set<String> concat(Set<String> set, String element) {
        return Stream.concat(set.stream(), Stream.of(element)).collect(Collectors.toSet());
    }

    private static String getConnectionString() {
        return Utils.getConnectionString(AzuriteDockerRule.ACCOUNT_NAME, AzuriteDockerRule.ACCOUNT_KEY, azurite.getBlobEndpoint());
    }

    private static void assertReferenceSecret(AzureBlobStoreBackend azureBlobStoreBackend)
            throws DataStoreException, IOException {
        // assert secret already created on init
        DataRecord refRec = azureBlobStoreBackend.getMetadataRecord("reference.key");
        assertNotNull("Reference data record null", refRec);
        assertTrue("reference key is empty", refRec.getLength() > 0);
    }
}
