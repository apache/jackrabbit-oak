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
package org.apache.jackrabbit.oak.plugins.document;

import java.io.*;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.apache.jackrabbit.guava.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.guava.common.collect.Sets.newHashSet;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class DocumentPropertyStateTest {

    private static final int BLOB_SIZE = 16 * 1024;
    private static final String TEST_NODE = "test";
    private static final String STRING_HUGEVALUE = RandomStringUtils.random(1050, "dummytest");

    @Rule
    public DocumentMKBuilderProvider builderProvider = new DocumentMKBuilderProvider();

    private Set<String> reads = newHashSet();

    private BlobStore bs = new MemoryBlobStore() {
        @Override
        public InputStream getInputStream(String blobId)
                throws IOException {
            reads.add(blobId);
            return super.getInputStream(blobId);
        }
    };

    private DocumentNodeStore ns;

    @Before
    public void before() throws Exception {
        ns = builderProvider.newBuilder().setBlobStore(bs).getNodeStore();
    }

    // OAK-5462
    @Test
    public void multiValuedBinarySize() throws Exception {
        NodeBuilder builder = ns.getRoot().builder();
        List<Blob> blobs = newArrayList();
        for (int i = 0; i < 3; i++) {
            blobs.add(builder.createBlob(new RandomStream(BLOB_SIZE, i)));
        }
        builder.child("test").setProperty("p", blobs, Type.BINARIES);
        TestUtils.merge(ns, builder);

        PropertyState p = ns.getRoot().getChildNode("test").getProperty("p");
        assertEquals(Type.BINARIES, p.getType());
        assertEquals(3, p.count());

        reads.clear();
        assertEquals(BLOB_SIZE, p.size(0));
        // must not read the blob via stream
        assertEquals(0, reads.size());
    }

    @Test
    public void multiValuedAboveThresholdSize() throws Exception {
        NodeBuilder builder = ns.getRoot().builder();
        List<Blob> blobs = newArrayList();
        for (int i = 0; i < 13; i++) {
            blobs.add(builder.createBlob(new RandomStream(BLOB_SIZE, i)));
        }
        builder.child(TEST_NODE).setProperty("p", blobs, Type.BINARIES);
        TestUtils.merge(ns, builder);

        PropertyState p = ns.getRoot().getChildNode(TEST_NODE).getProperty("p");
        assertEquals(Type.BINARIES, Objects.requireNonNull(p).getType());
        assertEquals(13, p.count());

        reads.clear();
        assertEquals(BLOB_SIZE, p.size(0));
        // must not read the blob via stream
        assertEquals(0, reads.size());
    }

    @Test
    public void stringBelowThresholdSize() throws Exception {
        NodeBuilder builder = ns.getRoot().builder();
        builder.child(TEST_NODE).setProperty("p", "dummy", Type.STRING);
        TestUtils.merge(ns, builder);

        PropertyState p = ns.getRoot().getChildNode(TEST_NODE).getProperty("p");
        assertEquals(Type.STRING, Objects.requireNonNull(p).getType());
        assertEquals(1, p.count());

        reads.clear();
        assertEquals(5, p.size(0));
        // must not read the string via stream
        assertEquals(0, reads.size());
    }

    @Test
    public void stringAboveThresholdSize() throws Exception {
        NodeBuilder builder = ns.getRoot().builder();
        builder.child(TEST_NODE).setProperty("p", STRING_HUGEVALUE, Type.STRING);
        TestUtils.merge(ns, builder);

        PropertyState p = ns.getRoot().getChildNode(TEST_NODE).getProperty("p");
        assertEquals(Type.STRING, Objects.requireNonNull(p).getType());
        assertEquals(1, p.count());

        reads.clear();
        assertEquals(1050, p.size(0));
        // must not read the string via streams
        assertEquals(0, reads.size());
    }

    @Test
    public void compressValueThrowsException() throws CommitFailedException, IOException {
        DocumentNodeStore mockDocumentStore = mock(DocumentNodeStore.class);
        Compression mockCompression = mock(Compression.class);
        when(mockCompression.getOutputStream(any(OutputStream.class))).thenThrow(new IOException("Compression failed"));

        DocumentPropertyState documentPropertyState = new DocumentPropertyState(mockDocumentStore, "p", STRING_HUGEVALUE, mockCompression);

        verify(mockCompression, times(1)).getOutputStream(any(OutputStream.class));

    }

    @Test
    public void uncompressValueThrowsException() throws CommitFailedException, IOException {

        DocumentNodeStore mockDocumentStore = mock(DocumentNodeStore.class);
        Compression mockCompression = mock(Compression.class);
        OutputStream mockOutputStream= mock(OutputStream.class);
        when(mockCompression.getOutputStream(any(OutputStream.class))).thenReturn(mockOutputStream);
        when(mockCompression.getInputStream(any(InputStream.class))).thenThrow(new IOException("Compression failed"));

        DocumentPropertyState documentPropertyState = new DocumentPropertyState(mockDocumentStore, "p", STRING_HUGEVALUE, mockCompression);
        documentPropertyState.getValue(Type.STRING);

        verify(mockCompression, times(1)).getInputStream(any(InputStream.class));
    }

}
