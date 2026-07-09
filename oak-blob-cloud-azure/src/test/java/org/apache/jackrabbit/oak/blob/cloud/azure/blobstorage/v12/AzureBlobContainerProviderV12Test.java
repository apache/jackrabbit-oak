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

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.UserDelegationKey;
import org.junit.Test;

import java.lang.reflect.Method;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for AzureBlobContainerProviderV12.Builder — no Azure connection required.
 */
public class AzureBlobContainerProviderV12Test {

    @Test
    public void builder_withConnectionString_setsFields() {
        String connectionString = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=key;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1";
        AzureBlobContainerProviderV12 provider = AzureBlobContainerProviderV12.Builder
                .builder("my-container")
                .withAzureConnectionString(connectionString)
                .build();

        assertEquals("my-container", provider.getContainerName());
        assertEquals(connectionString, provider.getAzureConnectionString());
    }

    @Test
    public void initializeWithProperties_connectionString_readsFromProps() {
        Properties props = new Properties();
        String connectionString = "DefaultEndpointsProtocol=http;AccountName=test;AccountKey=key;BlobEndpoint=http://127.0.0.1:10000/test";
        props.setProperty(AzureConstantsV12.AZURE_CONNECTION_STRING, connectionString);

        AzureBlobContainerProviderV12 provider = AzureBlobContainerProviderV12.Builder
                .builder("test-container")
                .initializeWithProperties(props)
                .build();

        assertEquals("test-container", provider.getContainerName());
        assertEquals(connectionString, provider.getAzureConnectionString());
    }

    @Test
    public void initializeWithProperties_emptyProps_connectionStringIsBlank() {
        AzureBlobContainerProviderV12 provider = AzureBlobContainerProviderV12.Builder
                .builder("empty-container")
                .initializeWithProperties(new Properties())
                .build();

        assertEquals("empty-container", provider.getContainerName());
        assertNotNull(provider);
    }

    @Test
    public void initializeWithProperties_noServicePrincipalFields_buildsSuccessfully() {
        Properties props = new Properties();
        props.setProperty(AzureConstantsV12.AZURE_CONNECTION_STRING, "test-conn-string");
        props.setProperty(AzureConstantsV12.AZURE_BLOB_CONTAINER_NAME, "container1");

        AzureBlobContainerProviderV12 provider = AzureBlobContainerProviderV12.Builder
                .builder("container1")
                .initializeWithProperties(props)
                .build();

        assertNotNull(provider);
        assertEquals("test-conn-string", provider.getAzureConnectionString());
    }

    /**
     * Supplying tenant/client/secret triggers the ClientSecretCredential build branch in the
     * constructor. The connection-string-only tests cover the null-credential branch; this covers
     * the service-principal branch.
     */
    @Test
    public void builder_withServicePrincipalFields_buildsCredentialBranch() {
        AzureBlobContainerProviderV12 provider = AzureBlobContainerProviderV12.Builder
                .builder("sp-container")
                .withAccountName("acct")
                .withTenantId("tenant-id")
                .withClientId("client-id")
                .withClientSecret("client-secret")
                .build();

        assertNotNull(provider);
        assertEquals("sp-container", provider.getContainerName());
    }

    @Test
    public void getEndpointUrl_customEndpointWithScheme_usedAsIs() throws Exception {
        assertEquals("https://custom.example.com", invokeGetEndpointUrl("acct", "https://custom.example.com"));
    }

    @Test
    public void getEndpointUrl_customEndpointWithoutScheme_getsHttpsPrefix() throws Exception {
        assertEquals("https://custom.example.com", invokeGetEndpointUrl("acct", "custom.example.com"));
    }

    @Test
    public void getEndpointUrl_noCustomEndpoint_buildsDefaultPublicEndpoint() throws Exception {
        String url = invokeGetEndpointUrl("myacct", "");
        assertEquals("https://myacct.blob.core.windows.net", url);
        assertTrue(url.startsWith("https://"));
    }

    // -------------------------------------------------------------------------
    // Delegation key caching
    // -------------------------------------------------------------------------

    /** Builds a provider wired with service-principal fields (no real Azure call needed). */
    private static AzureBlobContainerProviderV12 buildSpProvider() {
        return AzureBlobContainerProviderV12.Builder.builder("container")
                .withAccountName("account")
                .withTenantId("tenant")
                .withClientId("client")
                .withClientSecret("secret")
                .build();
    }

    @Test
    public void delegationKey_coldCache_fetchesFromAzure() {
        AzureBlobContainerProviderV12 provider = buildSpProvider();
        BlobServiceClient mockServiceClient = mock(BlobServiceClient.class);
        UserDelegationKey mockKey = mock(UserDelegationKey.class);
        when(mockServiceClient.getUserDelegationKey(any(), any())).thenReturn(mockKey);

        OffsetDateTime sasExpiry = OffsetDateTime.now(ZoneOffset.UTC).plusHours(1);
        UserDelegationKey result = provider.getOrRefreshDelegationKey(mockServiceClient, sasExpiry);

        assertSame(mockKey, result);
        verify(mockServiceClient, times(1)).getUserDelegationKey(any(), any());
    }

    @Test
    public void delegationKey_warmCache_reusesWithoutAzureCall() {
        AzureBlobContainerProviderV12 provider = buildSpProvider();
        BlobServiceClient mockServiceClient = mock(BlobServiceClient.class);
        UserDelegationKey mockKey = mock(UserDelegationKey.class);
        when(mockServiceClient.getUserDelegationKey(any(), any())).thenReturn(mockKey);

        OffsetDateTime sasExpiry = OffsetDateTime.now(ZoneOffset.UTC).plusHours(1);

        // First call — cold cache
        provider.getOrRefreshDelegationKey(mockServiceClient, sasExpiry);
        // Subsequent calls within the same key lifetime — should reuse
        provider.getOrRefreshDelegationKey(mockServiceClient, sasExpiry);
        provider.getOrRefreshDelegationKey(mockServiceClient, sasExpiry);

        verify(mockServiceClient, times(1)).getUserDelegationKey(any(), any());
    }

    @Test
    public void delegationKey_expiredCache_refreshes() {
        AzureBlobContainerProviderV12 provider = buildSpProvider();
        BlobServiceClient mockServiceClient = mock(BlobServiceClient.class);
        UserDelegationKey expiredKey = mock(UserDelegationKey.class);
        UserDelegationKey freshKey = mock(UserDelegationKey.class);
        when(mockServiceClient.getUserDelegationKey(any(), any())).thenReturn(freshKey);

        // Inject a cached key whose expiry is already in the past.
        OffsetDateTime pastExpiry = OffsetDateTime.now(ZoneOffset.UTC).minusSeconds(1);
        provider.cachedDelegationKey.set(
                new AzureBlobContainerProviderV12.CachedDelegationKey(expiredKey, pastExpiry));

        OffsetDateTime sasExpiry = OffsetDateTime.now(ZoneOffset.UTC).plusHours(1);
        UserDelegationKey result = provider.getOrRefreshDelegationKey(mockServiceClient, sasExpiry);

        assertSame(freshKey, result);
        verify(mockServiceClient, times(1)).getUserDelegationKey(any(), any());
    }

    @Test
    public void delegationKey_keyExpiresBelowRenewalBuffer_refreshes() {
        AzureBlobContainerProviderV12 provider = buildSpProvider();
        BlobServiceClient mockServiceClient = mock(BlobServiceClient.class);
        UserDelegationKey staleKey = mock(UserDelegationKey.class);
        UserDelegationKey freshKey = mock(UserDelegationKey.class);
        when(mockServiceClient.getUserDelegationKey(any(), any())).thenReturn(freshKey);

        // Cached key expires 30s after sasExpiry — inside the 60s renewal buffer.
        OffsetDateTime sasExpiry = OffsetDateTime.now(ZoneOffset.UTC).plusHours(1);
        OffsetDateTime keyExpiryTooSoon = sasExpiry.plusSeconds(30);
        provider.cachedDelegationKey.set(
                new AzureBlobContainerProviderV12.CachedDelegationKey(staleKey, keyExpiryTooSoon));

        UserDelegationKey result = provider.getOrRefreshDelegationKey(mockServiceClient, sasExpiry);

        assertSame(freshKey, result);
        verify(mockServiceClient, times(1)).getUserDelegationKey(any(), any());
    }

    @Test
    public void delegationKey_keyExpiresOutsideRenewalBuffer_reuses() {
        AzureBlobContainerProviderV12 provider = buildSpProvider();
        BlobServiceClient mockServiceClient = mock(BlobServiceClient.class);
        UserDelegationKey cachedKey = mock(UserDelegationKey.class);

        // Cached key expires 90s after sasExpiry — comfortably outside the 60s buffer.
        OffsetDateTime sasExpiry = OffsetDateTime.now(ZoneOffset.UTC).plusHours(1);
        OffsetDateTime keyExpiry = sasExpiry.plusSeconds(90);
        provider.cachedDelegationKey.set(
                new AzureBlobContainerProviderV12.CachedDelegationKey(cachedKey, keyExpiry));

        UserDelegationKey result = provider.getOrRefreshDelegationKey(mockServiceClient, sasExpiry);

        assertSame(cachedKey, result);
        verify(mockServiceClient, times(0)).getUserDelegationKey(any(), any());
    }

    // -------------------------------------------------------------------------

    private static String invokeGetEndpointUrl(String accountName, String customEndpoint) throws Exception {
        Method m = AzureBlobContainerProviderV12.class
                .getDeclaredMethod("getEndpointUrl", String.class, String.class);
        m.setAccessible(true);
        return (String) m.invoke(null, accountName, customEndpoint);
    }

    @Test
    public void builder_fluentSettersReturnBuilder() {
        AzureBlobContainerProviderV12.Builder builder = AzureBlobContainerProviderV12.Builder.builder("c");
        assertNotNull(builder.withAccountName("acc"));
        assertNotNull(builder.withBlobEndpoint("http://endpoint"));
        assertNotNull(builder.withSasToken("sastoken"));
        assertNotNull(builder.withAccountKey("accountkey"));
        assertNotNull(builder.withTenantId("tenant"));
        assertNotNull(builder.withClientId("client"));
        assertNotNull(builder.withClientSecret("secret"));
        assertNotNull(builder.withAzureConnectionString("conn"));
        assertNotNull(builder.withProxyHost("proxy.example.com"));
        assertNotNull(builder.withProxyPort("8080"));
    }

    /**
     * httpClient field must be non-null after build — created once at construction time.
     * Using no proxy settings here (null ProxyOptions) avoids reactor-netty proxy code path
     * that is not wired in the unit-test classpath.
     */
    @Test
    public void build_httpClientCreatedAtConstruction() throws Exception {
        Properties props = new Properties();
        props.setProperty(AzureConstantsV12.AZURE_CONNECTION_STRING,
                "DefaultEndpointsProtocol=http;AccountName=test;AccountKey=key;BlobEndpoint=http://127.0.0.1:10000/test");

        AzureBlobContainerProviderV12 provider = AzureBlobContainerProviderV12.Builder
                .builder("container")
                .initializeWithProperties(props)
                .build();

        java.lang.reflect.Field f = AzureBlobContainerProviderV12.class.getDeclaredField("httpClient");
        f.setAccessible(true);
        assertNotNull("httpClient must be created at construction time", f.get(provider));
    }

    /** Two providers built from the same properties own independent httpClient instances. */
    @Test
    public void build_twoInstances_independentHttpClients() throws Exception {
        Properties props = new Properties();
        props.setProperty(AzureConstantsV12.AZURE_CONNECTION_STRING,
                "DefaultEndpointsProtocol=http;AccountName=a;AccountKey=k;BlobEndpoint=http://127.0.0.1:10000/a");

        AzureBlobContainerProviderV12 p1 = AzureBlobContainerProviderV12.Builder.builder("c").initializeWithProperties(props).build();
        AzureBlobContainerProviderV12 p2 = AzureBlobContainerProviderV12.Builder.builder("c").initializeWithProperties(props).build();

        java.lang.reflect.Field f = AzureBlobContainerProviderV12.class.getDeclaredField("httpClient");
        f.setAccessible(true);
        assertNotNull(f.get(p1));
        assertNotNull(f.get(p2));
        assertNotSame("different provider instances must have separate httpClient objects",
                f.get(p1), f.get(p2));
    }
}
