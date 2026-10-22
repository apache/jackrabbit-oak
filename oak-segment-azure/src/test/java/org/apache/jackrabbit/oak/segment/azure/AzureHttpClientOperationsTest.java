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
package org.apache.jackrabbit.oak.segment.azure;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.UUID;

import com.azure.core.credential.TokenRequestContext;
import com.azure.core.http.ProxyOptions;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.core.util.Configuration;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import com.microsoft.aad.msal4j.MsalServiceException;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.apache.jackrabbit.oak.commons.Buffer;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockserver.junit.MockServerRule;
import org.mockserver.matchers.TimeToLive;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.verify.VerificationTimes;
import reactor.netty.http.client.HttpClient;

/**
 * Exercises the HTTP transport used by {@link AzurePersistence} against a local proxy and Azure responses.
 */
public class AzureHttpClientOperationsTest {

    private static final String STORAGE_ENDPOINT = "https://account.blob.core.windows.net";
    private static final String TOKEN_PATH = "/test-tenant/oauth2/v2.0/token";
    private static final String LAST_MODIFIED = "Wed, 21 Oct 2026 00:00:00 GMT";
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    @Rule
    public final MockServerRule server = new MockServerRule(this);

    /**
     * Prevents unmatched requests from being forwarded outside the test server.
     */
    @Before
    public void setUp() {
        server.getClient().when(HttpRequest.request(), Times.unlimited(), TimeToLive.unlimited(), -1)
                .respond(HttpResponse.response().withStatusCode(404));
    }

    /**
     * Downloads blob content using both synchronous and asynchronous clients through a proxy.
     */
    @Test(timeout = 30000)
    public void sendsRequestsThroughProxy() throws Exception {
        HttpRequest request = HttpRequest.request().withMethod("GET").withPath("/container/transport");
        server.getClient().when(request).respond(HttpResponse.response().withBody("through-proxy"));

        Assert.assertEquals("through-proxy", blobContainer().getBlobClient("transport")
                .downloadContent().toString());
        var content = storageClientBuilder().sasToken("sig=test").buildAsyncClient().getBlobAsyncClient("transport")
                .downloadContent().block(TIMEOUT);
        Assert.assertNotNull(content);
        Assert.assertEquals("through-proxy", content.toString());
        server.getClient().verify(request, VerificationTimes.exactly(2));
    }

    /**
     * Keeps a direct-request control independent of proxy configuration.
     */
    @Test(timeout = 30000)
    public void sendsRequestWithoutProxy() throws Exception {
        server.getClient().when(HttpRequest.request().withPath("/container/direct"))
                .respond(HttpResponse.response().withBody("direct"));
        var client = new NettyAsyncHttpClientBuilder()
                .configuration(Configuration.NONE).build();
        BlobContainerClient container = new BlobContainerClientBuilder()
                .endpoint("http://localhost:" + server.getPort()).containerName("container")
                .sasToken("sig=test").httpClient(client).buildClient();

        Assert.assertEquals("direct", container.getBlobClient("direct").downloadContent().toString());
    }

    /**
     * Obtains and applies a service-principal token to a proxied segment-listing request.
     */
    @Test(timeout = 30000)
    public void authenticatesServicePrincipalThroughProxy() throws Exception {
        stubIdentityMetadata();
        HttpRequest request = HttpRequest.request().withMethod("POST").withPath(TOKEN_PATH);
        server.getClient().when(request).respond(HttpResponse.response()
                .withHeader("Content-Type", "application/json")
                .withBody("{\"access_token\":\"test-access-token\",\"token_type\":\"Bearer\",\"expires_in\":3600}"));

        stubBlobListing("<BlobPrefix><Name>root/data00000a.tar/</Name></BlobPrefix>");
        BlobContainerClient container = storageClientBuilder().credential(servicePrincipal()).buildClient();
        AzurePersistence persistence = new AzurePersistence(container, "root");

        Assert.assertTrue(persistence.segmentFilesExist());
        server.getClient().verify(HttpRequest.request().withMethod("GET").withPath("/container")
                .withHeader("Authorization", "Bearer test-access-token"), VerificationTimes.exactly(1));
        server.getClient().verify(request, VerificationTimes.exactly(1));
        String body = server.getClient().retrieveRecordedRequests(request)[0].getBodyAsString();
        Assert.assertTrue(body, body.contains("grant_type=client_credentials"));
        Assert.assertTrue(body, body.contains("client_secret=test-secret"));
    }

    /**
     * Preserves authentication failures returned through the proxy.
     */
    @Test(timeout = 30000)
    public void reportsAuthenticationFailureThroughProxy() throws Exception {
        stubIdentityMetadata();
        HttpRequest request = HttpRequest.request().withMethod("POST").withPath(TOKEN_PATH);
        server.getClient().when(request).respond(HttpResponse.response().withStatusCode(400)
                .withHeader("Content-Type", "application/json")
                .withBody("{\"error\":\"invalid_client\",\"error_description\":\"test credentials rejected\"}"));
        ClientSecretCredential credential = servicePrincipal();

        RuntimeException failure = Assert.assertThrows(RuntimeException.class,
                () -> credential.getTokenSync(tokenRequest()));

        Throwable cause = failure;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        Assert.assertTrue(cause.toString(), cause instanceof MsalServiceException);
        Assert.assertEquals("invalid_client", ((MsalServiceException) cause).errorCode());
        server.getClient().verify(request, VerificationTimes.atLeast(1));
    }

    /**
     * Uploads and downloads segment bytes using Oak's archive writer and the real Azure transport.
     */
    @Test(timeout = 30000)
    public void writesAndReadsSegmentThroughProxy() throws Exception {
        stubBlobListing("");
        byte[] data = new byte[] {1, 2, 3, 4, 5};
        UUID id = UUID.randomUUID();
        String segmentPath = "/container/root%2Fdata00000a.tar%2F0000." + id;
        server.getClient().when(HttpRequest.request().withMethod("PUT")
                        .withPath("/container/root%2Fdata00000a.tar%2F.*"))
                .respond(HttpResponse.response().withStatusCode(201)
                        .withHeader("ETag", "\"test-etag\"")
                        .withHeader("Last-Modified", LAST_MODIFIED));
        server.getClient().when(HttpRequest.request().withMethod("GET").withPath(segmentPath))
                .respond(HttpResponse.response().withBody(data)
                        .withHeader("Content-Type", "application/octet-stream")
                        .withHeader("ETag", "\"test-etag\"")
                        .withHeader("Last-Modified", LAST_MODIFIED));
        WriteAccessController access = new WriteAccessController();
        access.enableWriting();

        AzureSegmentArchiveWriter writer = new AzureSegmentArchiveWriter(blobContainer(), "root",
                "data00000a.tar", new IOMonitorAdapter(), new FileStoreMonitorAdapter(), access);
        try {
            writer.writeSegment(id.getMostSignificantBits(), id.getLeastSignificantBits(),
                    data, 0, data.length, 1, 1, false);
            writer.flush();
            Buffer result = writer.readSegment(id.getMostSignificantBits(), id.getLeastSignificantBits());
            Assert.assertNotNull(result);
            byte[] actual = new byte[result.remaining()];
            result.get(actual);
            Assert.assertArrayEquals(data, actual);
        } finally {
            writer.close();
        }
        server.getClient().verify(HttpRequest.request().withMethod("PUT").withPath(segmentPath)
                .withBody(data), VerificationTimes.exactly(1));
        server.getClient().verify(HttpRequest.request().withMethod("GET").withPath(segmentPath),
                VerificationTimes.exactly(1));
        server.getClient().verify(HttpRequest.request().withMethod("PUT")
                .withPath("/container/root%2Fdata00000a.tar%2Fclosed"), VerificationTimes.exactly(1));
    }

    /**
     * Parses an archive listing received through the proxy using the segment persistence API.
     */
    @Test(timeout = 30000)
    public void listsSegmentArchivesThroughProxy() throws Exception {
        stubBlobListing("<BlobPrefix><Name>root/data00000a.tar/</Name></BlobPrefix>");
        AzurePersistence persistence = new AzurePersistence(blobContainer(), "root");

        Assert.assertTrue(persistence.segmentFilesExist());
        server.getClient().verify(HttpRequest.request().withMethod("GET").withPath("/container")
                .withQueryStringParameter("comp", "list"), VerificationTimes.exactly(1));
    }

    private NettyAsyncHttpClientBuilder proxiedClientBuilder() throws IOException {
        SslContext sslContext;
        try (InputStream certificate = getClass().getResourceAsStream(
                "/org/mockserver/socket/CertificateAuthorityCertificate.pem")) {
            Assert.assertNotNull(certificate);
            sslContext = SslContextBuilder.forClient().trustManager(certificate).build();
        }
        HttpClient transport = HttpClient.create().secure(ssl -> ssl.sslContext(sslContext));
        return new NettyAsyncHttpClientBuilder(transport)
                .configuration(Configuration.NONE)
                .proxy(new ProxyOptions(ProxyOptions.Type.HTTP,
                        new InetSocketAddress("localhost", server.getPort())))
                .connectTimeout(TIMEOUT).readTimeout(TIMEOUT).responseTimeout(TIMEOUT);
    }

    private BlobContainerClient blobContainer() throws IOException {
        return storageClientBuilder().sasToken("sig=test").buildClient();
    }

    private BlobContainerClientBuilder storageClientBuilder() throws IOException {
        return new BlobContainerClientBuilder().endpoint(STORAGE_ENDPOINT).containerName("container")
                .httpClient(proxiedClientBuilder().build())
                .retryOptions(new RequestRetryOptions(RetryPolicyType.FIXED, 1, 10, 1L, 1L, null));
    }

    private void stubBlobListing(String blobs) {
        server.getClient().when(HttpRequest.request().withMethod("GET").withPath("/container")
                        .withQueryStringParameter("comp", "list"))
                .respond(HttpResponse.response().withHeader("Content-Type", "application/xml")
                        .withBody("<EnumerationResults ServiceEndpoint=\"http://account.blob.core.windows.net\" "
                                + "ContainerName=\"container\"><Blobs>" + blobs
                                + "</Blobs><NextMarker /></EnumerationResults>"));
    }

    private void stubIdentityMetadata() {
        server.getClient().when(HttpRequest.request().withMethod("GET")
                        .withPath("/test-tenant/v2.0/.well-known/openid-configuration"))
                .respond(HttpResponse.response().withHeader("Content-Type", "application/json")
                        .withBody("{\"authorization_endpoint\":\"https://login.microsoftonline.com/test-tenant/oauth2/v2.0/authorize\","
                                + "\"token_endpoint\":\"https://login.microsoftonline.com" + TOKEN_PATH + "\","
                                + "\"issuer\":\"https://login.microsoftonline.com/test-tenant/v2.0\","
                                + "\"jwks_uri\":\"https://login.microsoftonline.com/test-tenant/discovery/v2.0/keys\"}"));
    }

    private ClientSecretCredential servicePrincipal() throws Exception {
        return new ClientSecretCredentialBuilder().tenantId("test-tenant").clientId("test-client")
                .clientSecret("test-secret").disableInstanceDiscovery()
                .configuration(Configuration.NONE).httpClient(proxiedClientBuilder().build()).maxRetry(0).build();
    }

    private TokenRequestContext tokenRequest() {
        return new TokenRequestContext().addScopes("https://storage.azure.com/.default");
    }
}
