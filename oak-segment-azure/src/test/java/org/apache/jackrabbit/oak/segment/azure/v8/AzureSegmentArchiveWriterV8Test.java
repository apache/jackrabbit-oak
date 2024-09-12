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
package org.apache.jackrabbit.oak.segment.azure.v8;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.jackrabbit.oak.segment.remote.WriteAccessController;
import org.apache.jackrabbit.oak.segment.spi.monitor.FileStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.IOMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.monitor.RemoteStoreMonitorAdapter;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveManager;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentArchiveWriter;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockserver.client.MockServerClient;
import org.mockserver.junit.MockServerRule;
import org.mockserver.matchers.Times;
import org.mockserver.model.BinaryBody;
import org.mockserver.model.HttpRequest;
import shaded_package.org.apache.http.client.utils.URIBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;

import static org.junit.Assert.assertThrows;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;
import static org.mockserver.verify.VerificationTimes.exactly;

public class AzureSegmentArchiveWriterV8Test {
    public static final String BASE_PATH = "/devstoreaccount1/oak-test";
    public static final int MAX_ATTEMPTS = 3;

    @Rule
    public MockServerRule mockServerRule = new MockServerRule(this);

    @SuppressWarnings("unused")
    private MockServerClient mockServerClient;

    private CloudBlobContainer container;

    @Before
    public void setUp() throws Exception {
        container = createCloudBlobContainer();

        System.setProperty("azure.segment.archive.writer.retries.intervalMs", "100");
        System.setProperty("azure.segment.archive.writer.retries.max", Integer.toString(MAX_ATTEMPTS));

        // Disable Azure SDK own retry mechanism used by AzureSegmentArchiveWriter
        System.setProperty("segment.azure.retry.attempts", "0");
        System.setProperty("segment.timeout.execution", "1");
    }

    @Test
    public void retryWhenFailureOnWriteBinaryReferences_eventuallySucceed() throws Exception {
        SegmentArchiveWriter writer = createSegmentArchiveWriter();
        writeAndFlushSegment(writer);

        HttpRequest writeBinaryReferencesRequest = getWriteBinaryReferencesRequest();
        // fail twice
        mockServerClient
                .when(writeBinaryReferencesRequest, Times.exactly(2))
                .respond(response().withStatusCode(500));
        // then succeed
        mockServerClient
                .when(writeBinaryReferencesRequest, Times.once())
                .respond(response().withStatusCode(201));

        writer.writeBinaryReferences(new byte[10]);

        mockServerClient.verify(writeBinaryReferencesRequest, exactly(MAX_ATTEMPTS));
    }

    @Test
    public void retryWhenFailureOnWriteGraph_eventuallySucceed() throws Exception {
        SegmentArchiveWriter writer = createSegmentArchiveWriter();
        writeAndFlushSegment(writer);

        HttpRequest writeGraphRequest = getWriteGraphRequest();
        // fail twice
        mockServerClient
                .when(writeGraphRequest, Times.exactly(2))
                .respond(response().withStatusCode(500));
        // then succeed
        mockServerClient
                .when(writeGraphRequest, Times.once())
                .respond(response().withStatusCode(201));

        writer.writeGraph(new byte[10]);

        mockServerClient.verify(writeGraphRequest, exactly(MAX_ATTEMPTS));
    }

    @Test
    public void retryWhenFailureOnClose_eventuallySucceed() throws Exception {
        SegmentArchiveWriter writer = createSegmentArchiveWriter();
        writeAndFlushSegment(writer);

        HttpRequest closeArchiveRequest = getCloseArchiveRequest();
        // fail twice
        mockServerClient
                .when(closeArchiveRequest, Times.exactly(2))
                .respond(response().withStatusCode(500));
        // then succeed
        mockServerClient
                .when(closeArchiveRequest, Times.once())
                .respond(response().withStatusCode(201));

        writer.close();

        mockServerClient.verify(closeArchiveRequest, exactly(MAX_ATTEMPTS));
    }

    @Test
    public void retryWhenFailureOnClose_failAfterLastRetryAttempt() throws Exception {
        SegmentArchiveWriter writer = createSegmentArchiveWriter();
        writeAndFlushSegment(writer);

        HttpRequest closeArchiveRequest = getCloseArchiveRequest();
        // always fail
        mockServerClient
                .when(closeArchiveRequest, Times.unlimited())
                .respond(response().withStatusCode(500));


        assertThrows(IOException.class, writer::close);

        mockServerClient.verify(closeArchiveRequest, exactly(MAX_ATTEMPTS));
    }


    private void writeAndFlushSegment(SegmentArchiveWriter writer) throws IOException {
        expectWriteRequests();
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
    private SegmentArchiveWriter createSegmentArchiveWriter() throws URISyntaxException, IOException {
        WriteAccessController writeAccessController = new WriteAccessController();
        writeAccessController.enableWriting();
        AzurePersistenceV8 azurePersistenceV8 = new AzurePersistenceV8(container.getDirectoryReference("oak"));/**/
        azurePersistenceV8.setWriteAccessController(writeAccessController);
        SegmentArchiveManager manager = azurePersistenceV8.createArchiveManager(false, false, new IOMonitorAdapter(), new FileStoreMonitorAdapter(), new RemoteStoreMonitorAdapter());
        SegmentArchiveWriter writer = manager.create("data00000a.tar");
        return writer;
    }

    private static HttpRequest getCloseArchiveRequest() {
        return request()
                .withMethod("PUT")
                .withPath(BASE_PATH + "/oak/data00000a.tar/closed");
    }

    private static HttpRequest getWriteBinaryReferencesRequest() {
        return request()
                .withMethod("PUT")
                .withPath(BASE_PATH + "/oak/data00000a.tar/data00000a.tar.brf");
    }

    private static HttpRequest getWriteGraphRequest() {
        return request()
                .withMethod("PUT")
                .withPath(BASE_PATH + "/oak/data00000a.tar/data00000a.tar.gph");
    }

    private static HttpRequest getUploadSegmentMetadataRequest() {
        return request()
                .withMethod("PUT")
                .withPath(BASE_PATH + "/oak/data00000a.tar/.*")
                .withQueryStringParameter("comp", "metadata");
    }

    private static HttpRequest getUploadSegmentDataRequest() {
        return request()
                .withMethod("PUT")
                .withPath(BASE_PATH + "/oak/data00000a.tar/.*")
                .withBody(new BinaryBody(new byte[10]));
    }

    @NotNull
    private CloudBlobContainer createCloudBlobContainer() throws URISyntaxException, StorageException {
        URI uri = new URIBuilder()
                .setScheme("http")
                .setHost(mockServerClient.remoteAddress().getHostName())
                .setPort(mockServerClient.remoteAddress().getPort())
                .setPath(BASE_PATH)
                .build();

        return new CloudBlobContainer(uri);
    }
}
