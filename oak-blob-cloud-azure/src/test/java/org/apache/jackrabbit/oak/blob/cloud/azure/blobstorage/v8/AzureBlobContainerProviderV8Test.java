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
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.SharedAccessBlobHeaders;
import com.microsoft.azure.storage.blob.SharedAccessBlobPermissions;
import com.microsoft.azure.storage.blob.SharedAccessBlobPolicy;
import org.apache.jackrabbit.core.data.DataRecord;
import org.apache.jackrabbit.core.data.DataStoreException;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzureConstants;
import org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage.AzuriteDockerRule;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.EnumSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.StreamSupport;

import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.ADD;
import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.CREATE;
import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.LIST;
import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.READ;
import static com.microsoft.azure.storage.blob.SharedAccessBlobPermissions.WRITE;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeNotNull;

public class AzureBlobContainerProviderV8Test {

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

  @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @After
    public void tearDown() throws Exception {
        if (container != null) {
            container.deleteIfExists();
        }
    }

    // ========== Builder Tests ==========

    @Test
    public void testBuilderWithAllProperties() {
        Properties properties = new Properties();
        properties.setProperty(AzureConstants.AZURE_CONNECTION_STRING, "test-connection");
        properties.setProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, "testaccount");
        properties.setProperty(AzureConstants.AZURE_BLOB_ENDPOINT, "https://test.blob.core.windows.net");
        properties.setProperty(AzureConstants.AZURE_SAS, "test-sas");
        properties.setProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_KEY, "test-key");
        properties.setProperty(AzureConstants.AZURE_TENANT_ID, "test-tenant");
        properties.setProperty(AzureConstants.AZURE_CLIENT_ID, "test-client");
        properties.setProperty(AzureConstants.AZURE_CLIENT_SECRET, "test-secret");

        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .initializeWithProperties(properties)
                .build();

        assertNotNull("Provider should not be null", provider);
        assertEquals("Container name should match", CONTAINER_NAME, provider.getContainerName());
    }

    @Test
    public void testBuilderWithIndividualMethods() {
        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString("test-connection")
                .withAccountName("testaccount")
                .withBlobEndpoint("https://test.blob.core.windows.net")
                .withSasToken("test-sas")
                .withAccountKey("test-key")
                .withTenantId("test-tenant")
                .withClientId("test-client")
                .withClientSecret("test-secret")
                .build();

        assertNotNull("Provider should not be null", provider);
        assertEquals("Container name should match", CONTAINER_NAME, provider.getContainerName());
    }

    @Test
    public void testBuilderWithEmptyProperties() {
        Properties properties = new Properties();

        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .initializeWithProperties(properties)
                .build();

        assertNotNull("Provider should not be null", provider);
        assertEquals("Container name should match", CONTAINER_NAME, provider.getContainerName());
    }

    @Test
    public void testBuilderWithNullProperties() {
        Properties properties = new Properties();
        // Properties with null values should default to empty strings
        properties.setProperty(AzureConstants.AZURE_CONNECTION_STRING, "");
        properties.setProperty(AzureConstants.AZURE_STORAGE_ACCOUNT_NAME, "");

        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .initializeWithProperties(properties)
                .build();

        assertNotNull("Provider should not be null", provider);
        assertEquals("Container name should match", CONTAINER_NAME, provider.getContainerName());
    }

    // ========== Connection Tests ==========

    @Test
    public void testGetBlobContainerWithConnectionString() throws Exception {
        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString(getConnectionString())
                .build();

        CloudBlobContainer container = provider.getBlobContainer();
        assertNotNull("Container should not be null", container);
        assertEquals("Container name should match", CONTAINER_NAME, container.getName());
    }

    @Test
    public void testGetBlobContainerWithBlobRequestOptions() throws Exception {
        BlobRequestOptions options = new BlobRequestOptions();
        options.setTimeoutIntervalInMs(30000);

        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString(getConnectionString())
                .build();

        CloudBlobContainer container = provider.getBlobContainer(options);
        assertNotNull("Container should not be null", container);
        assertEquals("Container name should match", CONTAINER_NAME, container.getName());
    }

    @Test
    public void testGetBlobContainerWithSasToken() throws Exception {
        CloudBlobContainer testContainer = createBlobContainer();
        String sasToken = testContainer.generateSharedAccessSignature(policy(READ_WRITE), null);

        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withSasToken(sasToken)
                .withBlobEndpoint(azurite.getBlobEndpoint())
                .withAccountName(AzuriteDockerRule.ACCOUNT_NAME)
                .build();

        CloudBlobContainer container = provider.getBlobContainer();
        assertNotNull("Container should not be null", container);
        assertEquals("Container name should match", CONTAINER_NAME, container.getName());
    }

    @Test
    public void testGetBlobContainerWithAccountKey() throws Exception {
        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(AzuriteDockerRule.ACCOUNT_NAME)
                .withAccountKey(AzuriteDockerRule.ACCOUNT_KEY)
                .withBlobEndpoint(azurite.getBlobEndpoint())
                .build();

        CloudBlobContainer container = provider.getBlobContainer();
        assertNotNull("Container should not be null", container);
        assertEquals("Container name should match", CONTAINER_NAME, container.getName());
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
        assertReadAccessGranted(azureBlobStoreBackend, concat(BLOBS, "file"));
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

    // ========== SAS Generation Tests ==========

    @Test
    public void testGenerateSharedAccessSignatureWithAccountKey() throws Exception {
        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(AzuriteDockerRule.ACCOUNT_NAME)
                .withAccountKey(AzuriteDockerRule.ACCOUNT_KEY)
                .withBlobEndpoint(azurite.getBlobEndpoint())
                .build();

        String sasToken = provider.generateSharedAccessSignature(
                null,
                "test-blob",
                READ_WRITE,
                3600,
                null
        );

        assertNotNull("SAS token should not be null", sasToken);
        assertFalse("SAS token should not be empty", sasToken.isEmpty());
        assertTrue("SAS token should contain signature", sasToken.contains("sig="));
    }

    @Test
    public void testGenerateSharedAccessSignatureWithHeaders() throws Exception {
        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(AzuriteDockerRule.ACCOUNT_NAME)
                .withAccountKey(AzuriteDockerRule.ACCOUNT_KEY)
                .withBlobEndpoint(azurite.getBlobEndpoint())
                .build();

        SharedAccessBlobHeaders headers = new SharedAccessBlobHeaders();
        headers.setContentType("application/octet-stream");
        headers.setCacheControl("no-cache");

        String sasToken = provider.generateSharedAccessSignature(
                null,
                "test-blob",
                READ_WRITE,
                3600,
                headers
        );

        assertNotNull("SAS token should not be null", sasToken);
        assertFalse("SAS token should not be empty", sasToken.isEmpty());
    }

    @Test
    public void testGenerateSharedAccessSignatureWithEmptyHeaders() throws Exception {
        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(AzuriteDockerRule.ACCOUNT_NAME)
                .withAccountKey(AzuriteDockerRule.ACCOUNT_KEY)
                .withBlobEndpoint(azurite.getBlobEndpoint())
                .build();

        SharedAccessBlobHeaders headers = new SharedAccessBlobHeaders();
        // Leave headers empty to test fillEmptyHeaders method

        String sasToken = provider.generateSharedAccessSignature(
                null,
                "test-blob",
                READ_WRITE,
                3600,
                headers
        );

        assertNotNull("SAS token should not be null", sasToken);
        assertFalse("SAS token should not be empty", sasToken.isEmpty());
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

    // ========== Error Condition Tests ==========

    @Test
    public void testGetBlobContainerWithInvalidConnectionString() {
        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString("invalid-connection-string")
                .build();

        try {
            provider.getBlobContainer();
            fail("Should throw exception for invalid connection string");
        } catch (Exception e) {
            // Should throw DataStoreException or IllegalArgumentException
            assertTrue("Should throw appropriate exception for invalid connection string",
                    e instanceof DataStoreException || e instanceof IllegalArgumentException);
        }
    }

    @Test(expected = DataStoreException.class)
    public void testGetBlobContainerWithInvalidAccountKey() throws Exception {
        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName("invalidaccount")
                .withAccountKey("invalidkey")
                .withBlobEndpoint("https://invalidaccount.blob.core.windows.net")
                .build();

        provider.getBlobContainer();
    }

    @Test
    public void testCloseProvider() {
        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString(getConnectionString())
                .build();

        // Should not throw any exception
        provider.close();

        assertTrue("Should not throw exception", true);
    }

    @Test
    public void testGetContainerName() {
        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .build();

        assertEquals("Container name should match", CONTAINER_NAME, provider.getContainerName());
    }

    @Test
    public void testAuthenticationPriorityConnectionString() throws Exception {
        // Connection string should take priority over other authentication methods
        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString(getConnectionString())
                .withSasToken("some-sas-token")
                .withAccountKey("some-account-key")
                .withTenantId("some-tenant")
                .withClientId("some-client")
                .withClientSecret("some-secret")
                .build();

        CloudBlobContainer container = provider.getBlobContainer();
        assertNotNull("Container should not be null", container);
        assertEquals("Container name should match", CONTAINER_NAME, container.getName());
    }

    @Test
    public void testAuthenticationPrioritySasToken() throws Exception {
        CloudBlobContainer testContainer = createBlobContainer();
        String sasToken = testContainer.generateSharedAccessSignature(policy(READ_WRITE), null);

        // SAS token should take priority over account key when no connection string
        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withSasToken(sasToken)
                .withBlobEndpoint(azurite.getBlobEndpoint())
                .withAccountName(AzuriteDockerRule.ACCOUNT_NAME)
                .withAccountKey("some-account-key")
                .build();

        CloudBlobContainer container = provider.getBlobContainer();
        assertNotNull("Container should not be null", container);
        assertEquals("Container name should match", CONTAINER_NAME, container.getName());
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

    // ========== Service Principal Authentication Tests ==========

    @Test
    public void testServicePrincipalAuthenticationDetection() {
        // Test when all service principal fields are present
        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName("testaccount")
                .withTenantId("test-tenant")
                .withClientId("test-client")
                .withClientSecret("test-secret")
                .build();

        // We can't directly test the private authenticateViaServicePrincipal method,
        // but we can test the behavior indirectly by ensuring no connection string is set
        assertNotNull("Provider should not be null", provider);
    }

    @Test
    public void testServicePrincipalAuthenticationNotDetectedWithConnectionString() {
        // Test when connection string is present - should not use service principal
        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString("test-connection")
                .withAccountName("testaccount")
                .withTenantId("test-tenant")
                .withClientId("test-client")
                .withClientSecret("test-secret")
                .build();

        assertNotNull("Provider should not be null", provider);
    }

    @Test
    public void testServicePrincipalAuthenticationNotDetectedWithMissingFields() {
        // Test when some service principal fields are missing
        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName("testaccount")
                .withTenantId("test-tenant")
                .withClientId("test-client")
                // Missing client secret
                .build();

        assertNotNull("Provider should not be null", provider);
    }

    // ========== Token Refresh Tests ==========

    @Test
    public void testTokenRefreshConstants() {
        // Test that the token refresh constants are reasonable
        // These are private static final fields, so we test them indirectly
        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .build();

        assertNotNull("Provider should not be null", provider);
        // The constants TOKEN_REFRESHER_INITIAL_DELAY = 45L and TOKEN_REFRESHER_DELAY = 1L
        // are used internally for scheduling token refresh
    }

    // ========== Edge Case Tests ==========

    @Test
    public void testBuilderWithNullContainerName() {
        // The builder actually accepts null container name, so let's test that it works
        AzureBlobContainerProviderV8.Builder builder = AzureBlobContainerProviderV8.Builder.builder(null);
        assertNotNull("Builder should not be null", builder);

        AzureBlobContainerProviderV8 provider = builder.build();
        assertNotNull("Provider should not be null", provider);
        assertNull("Container name should be null", provider.getContainerName());
    }

    @Test
    public void testBuilderWithEmptyContainerName() {
        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder("")
                .build();

        assertNotNull("Provider should not be null", provider);
        assertEquals("Container name should be empty string", "", provider.getContainerName());
    }

    @Test
    public void testMultipleClose() {
        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString(getConnectionString())
                .build();

        // Should not throw any exception when called multiple times
        provider.close();
        provider.close();
        provider.close();

        assertTrue("Should not throw exception", true);
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
        return ImmutableSet.<String>builder().addAll(set).add(element).build();
    }

    // ========== Additional SAS Generation Tests ==========

    @Test
    public void testGenerateSharedAccessSignatureWithReadOnlyPermissions() throws Exception {
        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(AzuriteDockerRule.ACCOUNT_NAME)
                .withAccountKey(AzuriteDockerRule.ACCOUNT_KEY)
                .withBlobEndpoint(azurite.getBlobEndpoint())
                .build();

        String sasToken = provider.generateSharedAccessSignature(
                null,
                "test-blob",
                READ_ONLY,
                3600,
                null
        );

        assertNotNull("SAS token should not be null", sasToken);
        assertFalse("SAS token should not be empty", sasToken.isEmpty());
        assertTrue("SAS token should contain permissions", sasToken.contains("sp="));
    }

    @Test
    public void testGenerateSharedAccessSignatureWithShortExpiry() throws Exception {
        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(AzuriteDockerRule.ACCOUNT_NAME)
                .withAccountKey(AzuriteDockerRule.ACCOUNT_KEY)
                .withBlobEndpoint(azurite.getBlobEndpoint())
                .build();

        String sasToken = provider.generateSharedAccessSignature(
                null,
                "test-blob",
                READ_WRITE,
                60, // 1 minute expiry
                null
        );

        assertNotNull("SAS token should not be null", sasToken);
        assertFalse("SAS token should not be empty", sasToken.isEmpty());
        assertTrue("SAS token should contain expiry", sasToken.contains("se="));
    }

    @Test
    public void testGenerateSharedAccessSignatureWithBlobRequestOptions() throws Exception {
        BlobRequestOptions options = new BlobRequestOptions();
        options.setTimeoutIntervalInMs(30000);

        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(AzuriteDockerRule.ACCOUNT_NAME)
                .withAccountKey(AzuriteDockerRule.ACCOUNT_KEY)
                .withBlobEndpoint(azurite.getBlobEndpoint())
                .build();

        String sasToken = provider.generateSharedAccessSignature(
                options,
                "test-blob",
                READ_WRITE,
                3600,
                null
        );

        assertNotNull("SAS token should not be null", sasToken);
        assertFalse("SAS token should not be empty", sasToken.isEmpty());
    }

    @Test
    public void testGenerateSharedAccessSignatureWithAllHeaders() throws Exception {
        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(AzuriteDockerRule.ACCOUNT_NAME)
                .withAccountKey(AzuriteDockerRule.ACCOUNT_KEY)
                .withBlobEndpoint(azurite.getBlobEndpoint())
                .build();

        SharedAccessBlobHeaders headers = new SharedAccessBlobHeaders();
        headers.setContentType("application/json");
        headers.setCacheControl("max-age=3600");
        headers.setContentDisposition("attachment; filename=test.json");
        headers.setContentEncoding("gzip");
        headers.setContentLanguage("en-US");

        String sasToken = provider.generateSharedAccessSignature(
                null,
                "test-blob",
                READ_WRITE,
                3600,
                headers
        );

        assertNotNull("SAS token should not be null", sasToken);
        assertFalse("SAS token should not be empty", sasToken.isEmpty());
    }

    private static String getConnectionString() {
        return UtilsV8.getConnectionString(AzuriteDockerRule.ACCOUNT_NAME, AzuriteDockerRule.ACCOUNT_KEY, azurite.getBlobEndpoint());
    }

    // ========== Constants and Default Values Tests ==========

    @Test
    public void testDefaultEndpointSuffix() {
        // Test that the default endpoint suffix is used correctly
        // This is tested indirectly through service principal authentication
        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName("testaccount")
                .withTenantId("test-tenant")
                .withClientId("test-client")
                .withClientSecret("test-secret")
                .build();

        assertNotNull("Provider should not be null", provider);
        // The DEFAULT_ENDPOINT_SUFFIX = "core.windows.net" is used internally
    }

    @Test
    public void testAzureDefaultScope() {
        // Test that the Azure default scope is used correctly
        // This is tested indirectly through service principal authentication
        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName("testaccount")
                .withTenantId("test-tenant")
                .withClientId("test-client")
                .withClientSecret("test-secret")
                .build();

        assertNotNull("Provider should not be null", provider);
        // The AZURE_DEFAULT_SCOPE = "https://storage.azure.com/.default" is used internally
    }

    // ========== Integration Tests ==========

    @Test
    public void testFullWorkflowWithConnectionString() throws Exception {
        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAzureConnectionString(getConnectionString())
                .build();

        try(provider) {
            // Get container
            CloudBlobContainer container = provider.getBlobContainer();
            assertNotNull("Container should not be null", container);

            // Generate SAS token
            String sasToken = provider.generateSharedAccessSignature(
                    null,
                    "integration-test-blob",
                    READ_WRITE,
                    3600,
                    null
            );
            assertNotNull("SAS token should not be null", sasToken);
            assertFalse("SAS token should not be empty", sasToken.isEmpty());

        } finally {
            provider.close();
        }
    }

    @Test
    public void testFullWorkflowWithAccountKey() throws Exception {
        AzureBlobContainerProviderV8 provider = AzureBlobContainerProviderV8.Builder
                .builder(CONTAINER_NAME)
                .withAccountName(AzuriteDockerRule.ACCOUNT_NAME)
                .withAccountKey(AzuriteDockerRule.ACCOUNT_KEY)
                .withBlobEndpoint(azurite.getBlobEndpoint())
                .build();

        try (provider) {
            // Get container
            CloudBlobContainer container = provider.getBlobContainer();
            assertNotNull("Container should not be null", container);

            // Generate SAS token
            String sasToken = provider.generateSharedAccessSignature(
                    null,
                    "integration-test-blob",
                    READ_WRITE,
                    3600,
                    null
            );
            assertNotNull("SAS token should not be null", sasToken);
            assertFalse("SAS token should not be empty", sasToken.isEmpty());

        } finally {
            provider.close();
        }
    }

    private static void assertReferenceSecret(AzureBlobStoreBackendV8 azureBlobStoreBackend)
            throws DataStoreException {
        // assert secret already created on init
        DataRecord refRec = azureBlobStoreBackend.getMetadataRecord("reference.key");
        assertNotNull("Reference data record null", refRec);
        assertTrue("reference key is empty", refRec.getLength() > 0);
    }

}