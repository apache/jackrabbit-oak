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

package org.apache.jackrabbit.oak.plugins.tika;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.plugins.blob.BlobStoreBlob;
import org.apache.jackrabbit.oak.plugins.memory.ArrayBasedBlob;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.jackrabbit.JcrConstants.JCR_CONTENT;
import static org.apache.jackrabbit.oak.InitialContent.INITIAL_CONTENT;
import static org.junit.Assert.assertEquals;

public class NodeStoreBinaryResourceProviderTest {
    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));

    private NodeState root = INITIAL_CONTENT;

    @Test
    public void countBinaries() throws Exception {
        NodeBuilder builder = root.builder();
        createFileNode(builder, "a", new IdBlob("hello", null), "text/plain");
        createFileNode(builder, "b", new IdBlob("hello", "id1"), "text/plain");

        createFileNode(builder.child("a2"), "c", new IdBlob("hello", "id2"), "text/foo")
                .setProperty(JcrConstants.JCR_ENCODING, "bar");

        NodeStore store = new MemoryNodeStore(builder.getNodeState());
        BlobStore blobStore = new MemoryBlobStore();
        NodeStoreBinaryResourceProvider extractor = new NodeStoreBinaryResourceProvider(store, blobStore);

        assertEquals(2, extractor.getBinaries("/").size());
        assertEquals(1, extractor.getBinaries("/a2").size());

        BinaryResource bs = extractor.getBinaries("/a2").first().get();
        assertEquals("text/foo", bs.getMimeType());
        assertEquals("bar", bs.getEncoding());
        assertEquals("id2", bs.getBlobId());
    }

    @Test
    public void csvGenerator() throws Exception {
        File csv = new File(temporaryFolder.getRoot(), "test.csv");
        BlobStore blobStore = new MemoryBlobStore();
        NodeBuilder builder = root.builder();
        createFileNode(builder, "a", blobOf("foo", blobStore), "text/plain");
        createFileNode(builder, "b", blobOf("hello", blobStore), "text/plain");

        NodeStore store = new MemoryNodeStore(builder.getNodeState());

        NodeStoreBinaryResourceProvider extractor = new NodeStoreBinaryResourceProvider(store, blobStore);
        CSVFileGenerator generator = new CSVFileGenerator(csv);
        generator.generate(extractor.getBinaries("/"));

        CSVFileBinaryResourceProvider csvbrp = new CSVFileBinaryResourceProvider(csv, blobStore);
        assertEquals(2, csvbrp.getBinaries("/").size());

        csvbrp.close();
    }

    private NodeBuilder createFileNode(NodeBuilder base, String name, Blob content, String mimeType) {
        NodeBuilder jcrContent = base.child(name).child(JCR_CONTENT);
        jcrContent.setProperty(JcrConstants.JCR_DATA, content);
        jcrContent.setProperty(JcrConstants.JCR_MIMETYPE, mimeType);
        return jcrContent;
    }

    private Blob blobOf(String content, BlobStore bs) throws IOException {
        String id = bs.writeBlob(new ByteArrayInputStream(content.getBytes()));
        return new BlobStoreBlob(bs, id);
    }

    private static class IdBlob extends ArrayBasedBlob {
        final String id;

        public IdBlob(String value, String id) {
            super(value.getBytes());
            this.id = id;
        }

        @Override
        public String getReference() {
            return id;
        }

        @Override
        public String getContentIdentity() {
            return id;
        }
    }
}
