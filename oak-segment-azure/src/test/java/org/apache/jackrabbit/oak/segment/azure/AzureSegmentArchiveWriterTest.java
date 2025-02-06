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
package org.apache.jackrabbit.oak.segment.azure;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.policy.RequestRetryOptions;
import org.apache.jackrabbit.oak.segment.azure.util.AzureRequestOptions;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.jetbrains.annotations.NotNull;
import org.junit.*;
import org.mockserver.client.MockServerClient;
import org.mockserver.junit.MockServerRule;
import org.mockserver.matchers.Times;
import org.mockserver.model.BinaryBody;
import org.mockserver.model.HttpRequest;

import java.io.IOException;
import java.util.UUID;

import static org.apache.jackrabbit.oak.segment.azure.AzuriteDockerRule.ACCOUNT_KEY;
import static org.apache.jackrabbit.oak.segment.azure.AzuriteDockerRule.ACCOUNT_NAME;
import static org.junit.Assert.assertThrows;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.verify.VerificationTimes.exactly;

public class AzureSegmentArchiveWriterTest {
    public static final String BASE_PATH = "/devstoreaccount1/oak-test";
    public static final int MAX_ATTEMPTS = 3;
    private static final String RETRY_ATTEMPTS = "segment.azure.retry.attempts";
    private static final String TIMEOUT_EXECUTION = "segment.timeout.execution";
    private static final String RETRY_INTERVAL_MS = "azure.segment.archive.writer.retries.intervalMs";
    private static final String WRITE_RETRY_ATTEMPTS = "azure.segment.archive.writer.retries.max";

    @Rule
    public MockServerRule mockServerRule = new MockServerRule(this);

    @SuppressWarnings("unused")
    private MockServerClient mockServerClient;

    @ClassRule
    public static AzuriteDockerRule azurite = new AzuriteDockerRule();

    @Before
    public void setUp() throws Exception {
        mockServerClient = new MockServerClient("localhost", mockServerRule.getPort());
        System.setProperty(RETRY_INTERVAL_MS, "100");
        System.setProperty(WRITE_RETRY_ATTEMPTS, Integer.toString(MAX_ATTEMPTS));

        // Disable Azure SDK own retry mechanism used by AzureSegmentArchiveWriter
        System.setProperty(RETRY_ATTEMPTS, "1");
        System.setProperty(TIMEOUT_EXECUTION, "1");
    }

    @AfterClass
    public static void setDown() {
        // resetting the values for the properties set in setUp(). otherwise these will apply to all the tests that are executed after
        System.clearProperty(RETRY_ATTEMPTS);
        System.clearProperty(TIMEOUT_EXECUTION);
        System.clearProperty(RETRY_INTERVAL_MS);
        System.clearProperty(WRITE_RETRY_ATTEMPTS);
    }

    @Test
    public void retryWhenFailureOnWriteBinaryReferences_eventuallySucceed() throws Exception {
        expectWriteRequests();

        HttpRequest writeBinaryReferencesRequest = getWriteBinaryReferencesRequest();
        // fail twice
        mockServerClient
                .when(writeBinaryReferencesRequest, Times.exactly(2))
                .respond(response().withStatusCode(500));
        // then succeed
        mockServerClient
                .when(writeBinaryReferencesRequest, Times.once())
                .respond(response().withStatusCode(201));

        SegmentArchiveWriter writer = createSegmentArchiveWriter();
        writeAndFlushSegment(writer);

        writer.writeBinaryReferences(new byte[10]);

        mockServerClient.verify(writeBinaryReferencesRequest, exactly(MAX_ATTEMPTS));
    }

    @Test
    public void retryWhenFailureOnWriteGraph_eventuallySucceed() throws Exception {
        expectWriteRequests();

        HttpRequest writeGraphRequest = getWriteGraphRequest();
        // fail twice
        mockServerClient
                .when(writeGraphRequest, Times.exactly(2))
                .respond(response().withStatusCode(500));
        // then succeed
        mockServerClient
                .when(writeGraphRequest, Times.once())
                .respond(response().withStatusCode(201));

        SegmentArchiveWriter writer = createSegmentArchiveWriter();
        writeAndFlushSegment(writer);

        writer.writeGraph(new byte[10]);

        mockServerClient.verify(writeGraphRequest, exactly(MAX_ATTEMPTS));
    }

    @Test
    public void retryWhenFailureOnClose_eventuallySucceed() throws Exception {
        expectWriteRequests();

        HttpRequest closeArchiveRequest = getCloseArchiveRequest();
        // fail twice
        mockServerClient
                .when(closeArchiveRequest, Times.exactly(2))
                .respond(response().withStatusCode(500));
        // then succeed
        mockServerClient
                .when(closeArchiveRequest, Times.once())
                .respond(response().withStatusCode(201));

        SegmentArchiveWriter writer = createSegmentArchiveWriter();
        writeAndFlushSegment(writer);

        writer.close();

        mockServerClient.verify(closeArchiveRequest, exactly(MAX_ATTEMPTS));
    }

    @Test
    public void retryWhenFailureOnClose_failAfterLastRetryAttempt() throws Exception {
        expectWriteRequests();

        HttpRequest closeArchiveRequest = getCloseArchiveRequest();
        // always fail
        mockServerClient
                .when(closeArchiveRequest, Times.unlimited())
                .respond(response().withStatusCode(500));

        SegmentArchiveWriter writer = createSegmentArchiveWriter();
        writeAndFlushSegment(writer);

        assertThrows(IOException.class, writer::close);

        mockServerClient.verify(closeArchiveRequest, exactly(MAX_ATTEMPTS));
    }


    private void writeAndFlushSegment(SegmentArchiveWriter writer) throws IOException {
        UUID u = UUID.randomUUID();
        writer.writeSegment(u.getMostSignificantBits(), u.getLeastSignificantBits(), new byte[10], 0, 10, 0, 0, false);
        writer.flush();
    }

    private void expectWriteRequests() {
        mockServerClient
                .when(getUploadSegmentDataRequest(), Times.once())
                .respond(response().withStatusCode(201));

        mockServerClient
                .when(getUploadSegmentMetadataRequest(), Times.once())
                .respond(response().withStatusCode(200));
    }

    @NotNull
    private SegmentArchiveWriter createSegmentArchiveWriter() throws  IOException {
        createContainerMock();
        BlobContainerClient readBlobContainerClient = getCloudStorageAccount("oak-test",  AzureRequestOptions.getRetryOptionsDefault());
        BlobContainerClient writeBlobContainerClient = getCloudStorageAccount("oak-test", AzureRequestOptions.getRetryOperationsOptimiseForWriteOperations());
        BlobContainerClient noRetryBlobContainerClient = getCloudStorageAccount("oak-test", null);
        writeBlobContainerClient.deleteIfExists();
        writeBlobContainerClient.createIfNotExists();

        WriteAccessController writeAccessController = new WriteAccessController();
        writeAccessController.enableWriting();
        AzurePersistence azurePersistence = new AzurePersistence(readBlobContainerClient, writeBlobContainerClient, noRetryBlobContainerClient, "oak");/**/
        azurePersistence.setWriteAccessController(writeAccessController);
        SegmentArchiveManager manager = azurePersistence.createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        SegmentArchiveWriter writer = manager.create("data00000a.tar");
        return writer;
    }

    private static HttpRequest getCloseArchiveRequest() {
        return request()
                .withMethod("PUT")
                .withPath(BASE_PATH + "/oak%2Fdata00000a.tar%2Fclosed");
    }

    private static HttpRequest getWriteBinaryReferencesRequest() {
        return request()
                .withMethod("PUT")
                .withPath(BASE_PATH + "/oak%2Fdata00000a.tar%2Fdata00000a.tar.brf");
    }

    private static HttpRequest getWriteGraphRequest() {
        return request()
                .withMethod("PUT")
                .withPath(BASE_PATH + "/oak%2Fdata00000a.tar%2Fdata00000a.tar.gph");
    }

    private static HttpRequest getUploadSegmentMetadataRequest() {
        return request()
                .withMethod("PUT")
                .withPath(BASE_PATH + "/oak%2Fdata00000a.tar%2F.*")
                .withQueryStringParameter("comp", "metadata");
    }

    private static HttpRequest getUploadSegmentDataRequest() {
        return request()
                .withMethod("PUT")
                .withPath(BASE_PATH + "/oak%2Fdata00000a.tar%2F.*")
                .withBody(new BinaryBody(new byte[10]));
    }

    private void createContainerMock() {
        mockServerClient
                .when(request()
                        .withMethod("PUT")
                        .withPath(BASE_PATH))
                .respond(response().withStatusCode(201).withBody("Container created successfully"));
    }

    public BlobContainerClient getCloudStorageAccount(String containerName, RequestRetryOptions retryOptions) {
        String blobEndpoint = "BlobEndpoint=http://localhost:" + mockServerRule.getPort() + "/devstoreaccount1";
        String accountName = "AccountName=" + ACCOUNT_NAME;
        String accountKey = "AccountKey=" + ACCOUNT_KEY;

        AzureHttpRequestLoggingTestingPolicy azureHttpRequestLoggingTestingPolicy = new AzureHttpRequestLoggingTestingPolicy();

        BlobServiceClientBuilder builder = new BlobServiceClientBuilder()
                .endpoint(azurite.getBlobEndpoint())
                .addPolicy(azureHttpRequestLoggingTestingPolicy)
                .connectionString(("DefaultEndpointsProtocol=http;" + accountName + ";" + accountKey + ";" + blobEndpoint));

        if (retryOptions != null) {
            builder.retryOptions(retryOptions);
        }

        BlobServiceClient blobServiceClient = builder.buildClient();

        return blobServiceClient.getBlobContainerClient(containerName);
    }

}
