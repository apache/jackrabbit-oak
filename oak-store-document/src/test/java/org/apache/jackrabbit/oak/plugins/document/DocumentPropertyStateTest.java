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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.mongodb.ReadPreference;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.mongo.MongoTestUtils;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.jackrabbit.guava.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.guava.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture.*;
import static org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture.MEMORY;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class DocumentPropertyStateTest {

    private static final int BLOB_SIZE = 16 * 1024;
    private static final String TEST_NODE = "test";
    private static final String STRING_HUGEVALUE = RandomStringUtils.random(10050, "dummytest");
    private static final int DEFAULT_COMPRESSION_THRESHOLD = 1024;
    private static final int DISABLED_COMPRESSION = -1;

    private DocumentStoreFixture fixture;
    private DocumentStore store;
    private DocumentPropertyState documentPropertyState;

    @Parameterized.Parameters(name="{0}")
    public static java.util.Collection<DocumentStoreFixture> fixtures() {
        List<DocumentStoreFixture> fixtures = new ArrayList<>();
        if (RDB_H2.isAvailable()) {
            fixtures.add(RDB_H2);
        }
        if (MONGO.isAvailable()) {
            fixtures.add(MONGO);
        }
        if (MEMORY.isAvailable()) {
            fixtures.add(MEMORY);
        }
        return fixtures;
    }

    public DocumentPropertyStateTest(DocumentStoreFixture fixture) {
        this.fixture = fixture;
        this.store = fixture.createDocumentStore();
        if (this.store instanceof MongoDocumentStore) {
            // Enforce primary read preference, otherwise tests may fail on a
            // replica set with a read preference configured to secondary.
            // Revision GC usually runs with a modified range way in the past,
            // which means changes made it to the secondary, but not in this
            // test using a virtual clock
            MongoTestUtils.setReadPreference(store, ReadPreference.primary());
        }
        if (this.store instanceof MongoDocumentStore) {
            this.documentPropertyState = new DocumentPropertyState(ns, "p", "value", Compression.GZIP);
        } else if (this.store instanceof RDBDocumentStore) {
            this.documentPropertyState = new DocumentPropertyState(ns, "p", "value", Compression.GZIP);
        } else {
            this.documentPropertyState = new DocumentPropertyState(ns, "p", "value", Compression.GZIP);
        }
    }

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

    @After
    public void tearDown() {
        try {
            ns.dispose();
        } finally {
            DocumentPropertyState.setCompressionThreshold(DISABLED_COMPRESSION);
        }
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
        assertEquals(10050, p.size(0));
        // must not read the string via streams
        assertEquals(0, reads.size());
    }

    @Test
    public void compressValueThrowsException() throws IOException, NoSuchFieldException, IllegalAccessException {
        DocumentNodeStore mockDocumentStore = mock(DocumentNodeStore.class);
        Compression mockCompression = mock(Compression.class);
        when(mockCompression.getOutputStream(any(OutputStream.class))).thenThrow(new IOException("Compression failed"));

        DocumentPropertyState.setCompressionThreshold(DEFAULT_COMPRESSION_THRESHOLD);
        DocumentPropertyState documentPropertyState = new DocumentPropertyState(mockDocumentStore, "p", "\"" + STRING_HUGEVALUE + "\"", mockCompression);

        assertEquals(documentPropertyState.getValue(Type.STRING), STRING_HUGEVALUE);

        verify(mockCompression, times(1)).getOutputStream(any(OutputStream.class));
    }

    @Test
    public void uncompressValueThrowsException() throws IOException, NoSuchFieldException, IllegalAccessException {

        DocumentNodeStore mockDocumentStore = mock(DocumentNodeStore.class);
        Compression mockCompression = mock(Compression.class);
        OutputStream mockOutputStream= mock(OutputStream.class);
        when(mockCompression.getOutputStream(any(OutputStream.class))).thenReturn(mockOutputStream);
        when(mockCompression.getInputStream(any(InputStream.class))).thenThrow(new IOException("Compression failed"));

        DocumentPropertyState.setCompressionThreshold(DEFAULT_COMPRESSION_THRESHOLD);
        DocumentPropertyState documentPropertyState = new DocumentPropertyState(mockDocumentStore, "p", STRING_HUGEVALUE, mockCompression);

        assertEquals(documentPropertyState.getValue(Type.STRING), "{}");

        verify(mockCompression, times(1)).getInputStream(any(InputStream.class));
    }

    @Test
    public void defaultValueSetToMinusOne() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, NoSuchFieldException {
        DocumentNodeStore store = mock(DocumentNodeStore.class);

        DocumentPropertyState.setCompressionThreshold(-1);
        DocumentPropertyState state = new DocumentPropertyState(store, "propertyNameNew", "\"" + STRING_HUGEVALUE + "\"", Compression.GZIP);

        byte[] result = state.getCompressedValue();

        assertNull(result);
        assertEquals(state.getValue(Type.STRING), STRING_HUGEVALUE);
    }

    @Test
    public void stringAboveThresholdSizeNoCompression() {
        DocumentNodeStore store = mock(DocumentNodeStore.class);

        DocumentPropertyState.setCompressionThreshold(DEFAULT_COMPRESSION_THRESHOLD);

        DocumentPropertyState state = new DocumentPropertyState(store, "propertyName", "\"" + STRING_HUGEVALUE + "\"", Compression.NONE);

        byte[] result = state.getCompressedValue();

        assertEquals(result.length, STRING_HUGEVALUE.length() + 2 );

        assertEquals(state.getValue(Type.STRING), STRING_HUGEVALUE);
        assertEquals(STRING_HUGEVALUE, state.getValue(Type.STRING));
    }

    @Test
    public void testInterestingStringsWithoutCompression() {
        DocumentNodeStore store = mock(DocumentNodeStore.class);
        String[] tests = new String[] { "simple:foo", "cr:a\n\b", "dquote:a\"b", "bs:a\\b", "euro:a\u201c", "gclef:\uD834\uDD1E",
                "tab:a\tb", "nul:a\u0000b"};

        for (String test : tests) {
                DocumentPropertyState state = new DocumentPropertyState(store, "propertyName", test, Compression.GZIP);
                assertEquals(test, state.getValue());
        }
    }

    @Test
    public void testInterestingStringsWithCompression() {
        DocumentNodeStore store = mock(DocumentNodeStore.class);
        String[] tests = new String[]{"simple:foo", "cr:a\n\b", "dquote:a\"b", "bs:a\\b", "euro:a\u201c", "gclef:\uD834\uDD1E",
                "tab:a\tb", "nul:a\u0000b"};

        DocumentPropertyState.setCompressionThreshold(1);
        for (String test : tests) {
            DocumentPropertyState state = new DocumentPropertyState(store, "propertyName", test, Compression.GZIP);
            assertEquals(test, state.getValue());
        }
    }

    @Test
    public void testBrokenSurrogateWithoutCompression() {
        DocumentNodeStore store = mock(DocumentNodeStore.class);
        String test = "brokensurrogate:dfsa\ud800";

        DocumentPropertyState state = new DocumentPropertyState(store, "propertyName", test, Compression.GZIP);
        assertEquals(test, state.getValue());
    }

    @Test
    public void testBrokenSurrogateWithCompression() {
        DocumentNodeStore store = mock(DocumentNodeStore.class);
        String test = "brokensurrogate:dfsa\ud800";

        DocumentPropertyState.setCompressionThreshold(1);
        DocumentPropertyState state = new DocumentPropertyState(store, "propertyName", test, Compression.GZIP);
        assertEquals(test, state.getValue());

    }

    @Test
    public void testEqualsWithoutCompression() {
        DocumentNodeStore store = mock(DocumentNodeStore.class);
        String name = "propertyName";
        String value = "testValue";
        Compression compression = Compression.GZIP;

        DocumentPropertyState state1 = new DocumentPropertyState(store, name, value, compression);
        DocumentPropertyState state2 = new DocumentPropertyState(store, name, value, compression);

        assertEquals(state1, state2);

        // Test for inequality
        DocumentPropertyState state4 = new DocumentPropertyState(store, "differentName", value, compression);
        assertNotEquals(state1, state4);
    }

    @Test
    public void testEqualsWithCompression() throws IOException {
        DocumentNodeStore store = mock(DocumentNodeStore.class);
        String name = "propertyName";
        String value = "testValue";
        Compression compression = Compression.GZIP;

        DocumentPropertyState.setCompressionThreshold(1);
        // Create two DocumentPropertyState instances with the same properties
        DocumentPropertyState state1 = new DocumentPropertyState(store, name, value, compression);
        DocumentPropertyState state2 = new DocumentPropertyState(store, name, value, compression);

        // Check that the compressed values are not null
        assertNotNull(state1.getCompressedValue());
        assertNotNull(state2.getCompressedValue());

        // Check that the equals method returns true
        assertEquals(state1, state2);

        // Decompress the values
        String decompressedValue1 = state1.getValue();
        String decompressedValue2 = state2.getValue();

        // Check that the decompressed values are equal to the original value
        assertEquals(value, decompressedValue1);
        assertEquals(value, decompressedValue2);

        // Check that the equals method still returns true after decompression
        assertEquals(state1, state2);
    }

    @Test
    public void testOneCompressOtherUncompressInEquals() throws IOException {
        DocumentNodeStore store = mock(DocumentNodeStore.class);
        String name = "propertyName";
        String value = "testValue";
        Compression compression = Compression.GZIP;

        // Create a DocumentPropertyState instance with a compressed value
        DocumentPropertyState.setCompressionThreshold(1);
        DocumentPropertyState state1 = new DocumentPropertyState(store, name, value, compression);

        // Create a DocumentPropertyState instance with an uncompressed value
        DocumentPropertyState.setCompressionThreshold(-1);
        DocumentPropertyState state2 = new DocumentPropertyState(store, name, value, compression);

        assertNotEquals(state1, state2);

        // Decompress the value of the first instance
        String decompressedValue1 = state1.getValue();

        // Check that the decompressed value is equal to the original value
        assertEquals(value, decompressedValue1);
    }
}