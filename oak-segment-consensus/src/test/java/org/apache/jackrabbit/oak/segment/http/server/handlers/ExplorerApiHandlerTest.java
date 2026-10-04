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
package org.apache.jackrabbit.oak.segment.http.server.handlers;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.segment.consensus.service.AppliedLogPosition;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EmptyHook;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExplorerApiHandlerTest {

    @Test
    public void testHandleExploreNodeReturnsChildrenAndProperties() throws Exception {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        NodeBuilder root = nodeStore.getRoot().builder();
        NodeBuilder doc = root.child("content").child("doc");
        doc.child("child-a");
        doc.setProperty("title", "Hello");
        doc.setProperty("enabled", true);
        doc.setProperty("version", 7L);
        doc.setProperty("tags", Arrays.asList("one", "two"), Type.STRINGS);
        nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);

        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        ExplorerApiHandler handler = new ExplorerApiHandler(nodeStore, Paths.get("/tmp/store"), null);
        handler.handleExploreNode(response, "/content/doc");

        verify(response).setStatus(HttpServletResponse.SC_OK);
        String json = body.toString();
        assertTrue(json.contains("\"path\":\"/content/doc\""));
        assertTrue(json.contains("\"children\":[\"child-a\"]"));
        assertTrue(json.contains("\"title\":\"Hello\""));
        assertTrue(json.contains("\"enabled\":\"true\""));
        assertTrue(json.contains("\"version\":\"7\""));
        assertTrue(json.contains("\"tags\":[\"one\",\"two\"]"));
    }

    @Test
    public void rootListingHidesTheConsensusWatermarkNode() throws Exception {
        MemoryNodeStore nodeStore = new MemoryNodeStore();
        NodeBuilder root = nodeStore.getRoot().builder();
        root.child("oak-chain");
        new AppliedLogPosition(64L, 0, 0L).writeTo(root);
        nodeStore.merge(root, EmptyHook.INSTANCE, CommitInfo.EMPTY);
        StringWriter body = new StringWriter();

        new ExplorerApiHandler(nodeStore, Paths.get("/tmp/store"), null).handleExploreNode(responseWithBody(body), "/");

        assertTrue(body.toString(), body.toString().contains("\"children\":[\"oak-chain\"]"));
    }

    @Test
    public void testHandleExploreNodeReturnsNotFoundForMissingPath() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        ExplorerApiHandler handler = new ExplorerApiHandler(new MemoryNodeStore(), Paths.get("/tmp/store"), null);
        handler.handleExploreNode(response, "/missing");

        verify(response).setStatus(HttpServletResponse.SC_NOT_FOUND);
        assertTrue(body.toString().contains("Node not found"));
    }

    @Test
    public void testHandleRecentSegmentsReturnsNewestJournalEntries() throws Exception {
        Path dir = Files.createTempDirectory("explorer-recent-segments");
        try {
            Files.write(dir.resolve("journal.log"), Arrays.asList(
                "seg-001 2026-03-20T00:00:00Z",
                "seg-002 2026-03-20T00:01:00Z",
                "seg-003 2026-03-20T00:02:00Z"
            ));

            StringWriter body = new StringWriter();
            HttpServletResponse response = responseWithBody(body);
            ExplorerApiHandler handler = new ExplorerApiHandler(new MemoryNodeStore(), dir, null);

            handler.handleRecentSegments(response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            String json = body.toString();
            assertTrue(json.contains("\"id\":\"seg-003\""));
            assertTrue(json.contains("\"id\":\"seg-002\""));
            assertTrue(json.indexOf("seg-003") < json.indexOf("seg-002"));
        } finally {
            deleteRecursively(dir);
        }
    }

    @Test
    public void testHandleTarFilesReturnsTarMetadata() throws Exception {
        Path dir = Files.createTempDirectory("explorer-tars");
        try {
            Files.write(dir.resolve("journal.log"), Arrays.asList(
                "seg-001 now",
                "seg-002 later",
                "seg-003 later"
            ));
            Files.write(dir.resolve("data00000a.tar"), new byte[16]);
            Files.write(dir.resolve("data00001a.tar"), new byte[32]);

            StringWriter body = new StringWriter();
            HttpServletResponse response = responseWithBody(body);
            ExplorerApiHandler handler = new ExplorerApiHandler(new MemoryNodeStore(), dir, null);

            handler.handleTarFiles(response);

            verify(response).setStatus(HttpServletResponse.SC_OK);
            String json = body.toString();
            assertTrue(json.contains("\"name\":\"data00000a.tar\""));
            assertTrue(json.contains("\"name\":\"data00001a.tar\""));
            assertTrue(json.contains("\"estimatedCount\":true"));
            assertTrue(json.contains("\"size\":16"));
            assertTrue(json.contains("\"size\":32"));
        } finally {
            deleteRecursively(dir);
        }
    }

    @Test
    public void testHandleBlobStreamRejectsMissingBlobStore() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);

        ExplorerApiHandler handler = new ExplorerApiHandler(new MemoryNodeStore(), Paths.get("/tmp/store"), null);
        handler.handleBlobStream(mock(HttpServletRequest.class), response, "blob-1");

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        assertTrue(body.toString().contains("BlobStore not configured"));
    }

    @Test
    public void testHandleBlobStreamReturnsNotFoundWhenBlobMissing() throws Exception {
        StringWriter body = new StringWriter();
        HttpServletResponse response = responseWithBody(body);
        BlobStore blobStore = mock(BlobStore.class);
        when(blobStore.getInputStream("blob-404")).thenReturn(null);

        ExplorerApiHandler handler = new ExplorerApiHandler(new MemoryNodeStore(), Paths.get("/tmp/store"), blobStore);
        handler.handleBlobStream(mock(HttpServletRequest.class), response, "blob-404");

        verify(response).setStatus(HttpServletResponse.SC_NOT_FOUND);
        assertTrue(body.toString().contains("Blob not found: blob-404"));
    }

    @Test
    public void testHandleBlobStreamWritesResponseBodyAndHeaders() throws Exception {
        BlobStore blobStore = mock(BlobStore.class);
        byte[] payload = "hello-image".getBytes(StandardCharsets.UTF_8);
        when(blobStore.getInputStream("ed06f9cb-demo")).thenReturn(new ByteArrayInputStream(payload));

        RecordingServletOutputStream output = new RecordingServletOutputStream();
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getOutputStream()).thenReturn(output);

        ExplorerApiHandler handler = new ExplorerApiHandler(new MemoryNodeStore(), Paths.get("/tmp/store"), blobStore);
        handler.handleBlobStream(mock(HttpServletRequest.class), response, "ed06f9cb-demo");

        verify(response).setStatus(HttpServletResponse.SC_OK);
        verify(response).setContentType("image/jpeg");
        verify(response).setHeader("X-Blob-Id", "ed06f9cb-demo");
        assertArrayEquals(payload, output.toByteArray());
    }

    @Test
    public void testHandleBlobStreamUsesLateBoundBlobStoreSupplier() throws Exception {
        final BlobStore[] blobStoreRef = new BlobStore[1];
        BlobStore blobStore = mock(BlobStore.class);
        byte[] payload = "late-bound".getBytes(StandardCharsets.UTF_8);
        when(blobStore.getInputStream("blob-late")).thenReturn(new ByteArrayInputStream(payload));

        RecordingServletOutputStream output = new RecordingServletOutputStream();
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getOutputStream()).thenReturn(output);

        ExplorerApiHandler handler = ExplorerApiHandler.withBlobStoreSupplier(
            new MemoryNodeStore(),
            Paths.get("/tmp/store"),
            () -> blobStoreRef[0]
        );
        blobStoreRef[0] = blobStore;

        handler.handleBlobStream(mock(HttpServletRequest.class), response, "blob-late");

        verify(response).setStatus(HttpServletResponse.SC_OK);
        verify(response).setHeader("X-Blob-Id", "blob-late");
        assertArrayEquals(payload, output.toByteArray());
    }

    private static HttpServletResponse responseWithBody(StringWriter body) throws Exception {
        HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getWriter()).thenReturn(new PrintWriter(body));
        return response;
    }

    private static void deleteRecursively(Path dir) throws IOException {
        if (dir == null || !Files.exists(dir)) {
            return;
        }
        try (java.util.stream.Stream<Path> stream = Files.walk(dir)) {
            stream.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private static final class RecordingServletOutputStream extends ServletOutputStream {
        private final ByteArrayOutputStream output = new ByteArrayOutputStream();

        @Override
        public void write(int b) {
            output.write(b);
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setWriteListener(WriteListener writeListener) {
        }

        private byte[] toByteArray() {
            return output.toByteArray();
        }
    }
}
