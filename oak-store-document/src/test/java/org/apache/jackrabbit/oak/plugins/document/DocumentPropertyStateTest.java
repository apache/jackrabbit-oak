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
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.*;
import org.junit.runners.MethodSorters;

import static org.apache.jackrabbit.guava.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.guava.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DocumentPropertyStateTest {

    private static final int BLOB_SIZE = 16 * 1024;
    public static final int COMPRESSED_SIZE = 543;
    private static final String TEST_NODE = "test";
    private static final String STRING_HUGEVALUE = RandomStringUtils.random(10050, "dummytest");
    private static final int DEFAULT_COMPRESSION_THRESHOLD = 1024;

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
        assertEquals(10050, p.size(0));
        // must not read the string via streams
        assertEquals(0, reads.size());
    }

    @Test
    public void compressValueThrowsException() throws IOException, NoSuchFieldException, IllegalAccessException {
        DocumentNodeStore mockDocumentStore = mock(DocumentNodeStore.class);
        Compression mockCompression = mock(Compression.class);
        when(mockCompression.getOutputStream(any(OutputStream.class))).thenThrow(new IOException("Compression failed"));

        Field compressionThreshold = DocumentPropertyState.class.getDeclaredField("DEFAULT_COMPRESSION_THRESHOLD");
        compressionThreshold.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(compressionThreshold, compressionThreshold.getModifiers() & ~Modifier.FINAL);

        compressionThreshold.set(null, DEFAULT_COMPRESSION_THRESHOLD);

        DocumentPropertyState documentPropertyState = new DocumentPropertyState(mockDocumentStore, "p", "\"" + STRING_HUGEVALUE + "\"", mockCompression);

        assertEquals(documentPropertyState.getValue(Type.STRING), STRING_HUGEVALUE);

        verify(mockCompression, times(1)).getOutputStream(any(OutputStream.class));

        compressionThreshold.set(null, -1);

    }

    @Test
    public void uncompressValueThrowsException() throws IOException, NoSuchFieldException, IllegalAccessException {

        DocumentNodeStore mockDocumentStore = mock(DocumentNodeStore.class);
        Compression mockCompression = mock(Compression.class);
        OutputStream mockOutputStream= mock(OutputStream.class);
        when(mockCompression.getOutputStream(any(OutputStream.class))).thenReturn(mockOutputStream);
        when(mockCompression.getInputStream(any(InputStream.class))).thenThrow(new IOException("Compression failed"));

        Field compressionThreshold = DocumentPropertyState.class.getDeclaredField("DEFAULT_COMPRESSION_THRESHOLD");
        compressionThreshold.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(compressionThreshold, compressionThreshold.getModifiers() & ~Modifier.FINAL);

        compressionThreshold.set(null, DEFAULT_COMPRESSION_THRESHOLD);

        DocumentPropertyState documentPropertyState = new DocumentPropertyState(mockDocumentStore, "p", STRING_HUGEVALUE, mockCompression);

        assertEquals(documentPropertyState.getValue(Type.STRING), "{}");

        verify(mockCompression, times(1)).getInputStream(any(InputStream.class));

        compressionThreshold.set(null, -1);
    }

    @Test
    public void adefaultValueSetToMinusOne() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, NoSuchFieldException {
        DocumentNodeStore store = mock(DocumentNodeStore.class);

        DocumentPropertyState state = new DocumentPropertyState(store, "propertyNameNew", "\"" + STRING_HUGEVALUE + "\"", Compression.GZIP);

        // Get the private method
        Method getCompressedValueMethod = DocumentPropertyState.class.getDeclaredMethod("getCompressedValue");

        // Make the method accessible
        getCompressedValueMethod.setAccessible(true);
        byte[] result = (byte[]) getCompressedValueMethod.invoke(state);

        assertNull(result);
        assertEquals(state.getValue(Type.STRING), STRING_HUGEVALUE);
    }

    @Test
    public void stringAboveThresholdSizeNoCompression() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, NoSuchFieldException {
        DocumentNodeStore store = mock(DocumentNodeStore.class);

        // Get the private method
        Method getCompressedValueMethod = DocumentPropertyState.class.getDeclaredMethod("getCompressedValue");

        // Make the method accessible
        getCompressedValueMethod.setAccessible(true);

        Field compressionThreshold = DocumentPropertyState.class.getDeclaredField("DEFAULT_COMPRESSION_THRESHOLD");
        compressionThreshold.setAccessible(true);
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(compressionThreshold, compressionThreshold.getModifiers() & ~Modifier.FINAL);

        compressionThreshold.set(null, DEFAULT_COMPRESSION_THRESHOLD);

        DocumentPropertyState state = new DocumentPropertyState(store, "propertyName", "\"" + STRING_HUGEVALUE + "\"", Compression.NONE);

        byte[] result = (byte[]) getCompressedValueMethod.invoke(state);

        assertEquals(result.length, STRING_HUGEVALUE.length() + 2 );

        assertEquals(state.getValue(Type.STRING), STRING_HUGEVALUE);
        assertEquals(STRING_HUGEVALUE, state.getValue(Type.STRING));

        // Reset the value of the field
        compressionThreshold.set(null, -1);
    }

    @Test(expected = ComparisonFailure.class)
    public void testInterestingStrings() {

        DocumentNodeStore store = mock(DocumentNodeStore.class);
        String testString = "\"\"simple:foo\", \"cr:a\\n\\b\", \"dquote:a\\\"b\", \"bs:a\\\\b\", \"euro:a\\u201c\", \"gclef:\\uD834\\uDD1E\",\n" +
                "                \"tab:a\\tb\", \"nul:a\\u0000b\", \"brokensurrogate:\\ud800\"";
        DocumentPropertyState state = new DocumentPropertyState(store, "propertyName", testString, Compression.GZIP);

        String value = state.getValue(Type.STRING);

        assertEquals(testString, value);

    }



    @Test
    public void performanceTest() {
        String testString = RandomStringUtils.random(10050, "dummytest");
        DocumentNodeStore store = mock(DocumentNodeStore.class);
        DocumentPropertyState state = new DocumentPropertyState(store, "propertyName", "\"" + testString + "\"", Compression.GZIP);

        Runtime runtime = Runtime.getRuntime();

        long startMemory = runtime.totalMemory() - runtime.freeMemory(); // Get initial memory usage
        long startTime = System.nanoTime();

        String compressedValue = state.getValue(Type.STRING);

        long endTime = System.nanoTime();
        long endMemory = runtime.totalMemory() - runtime.freeMemory(); // Get final memory usage

        long duration = (endTime - startTime);  // divide by 1000000 to get milliseconds.
        long usedMemory = endMemory - startMemory; // Calculate the used memory during the operation

        System.out.println("Time taken for compression: " + duration + " nanoseconds");
        System.out.println("Memory used for compression: " + usedMemory + " bytes");

        // Assert that the compressed value is not null or empty
        assertTrue(compressedValue != null && !compressedValue.isEmpty());
    }
}