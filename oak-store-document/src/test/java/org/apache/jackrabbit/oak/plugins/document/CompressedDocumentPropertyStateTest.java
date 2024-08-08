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

import static org.apache.jackrabbit.guava.common.collect.Sets.newHashSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.Compression;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.blob.MemoryBlobStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class CompressedDocumentPropertyStateTest {

    private static final String STRING_HUGEVALUE = RandomStringUtils.random(10050, "dummytest");
    private static final int DEFAULT_COMPRESSION_THRESHOLD = 1024;
    private static final int DISABLED_COMPRESSION = -1;

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
            CompressedDocumentPropertyState.setCompressionThreshold(DISABLED_COMPRESSION);
        }
    }

    @Test
    public void stringBelowThresholdSize() throws Exception {
        DocumentNodeStore store = mock(DocumentNodeStore.class);
        CompressedDocumentPropertyState.setCompressionThreshold(10000);

        CompressedDocumentPropertyState state = new CompressedDocumentPropertyState(store, "p", "\"" + STRING_HUGEVALUE + "\"", Compression.GZIP);

        assertEquals(Type.STRING, state.getType());
        assertEquals(1, state.count());

        reads.clear();
        assertEquals(10050, state.size(0));
        // must not read the string via streams
        assertEquals(0, reads.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void compressValueThrowsException() throws IOException {
        DocumentNodeStore mockDocumentStore = mock(DocumentNodeStore.class);
        Compression mockCompression = mock(Compression.class);
        when(mockCompression.getOutputStream(any(OutputStream.class))).thenThrow(new IllegalArgumentException("Compression failed"));

        CompressedDocumentPropertyState.setCompressionThreshold(DEFAULT_COMPRESSION_THRESHOLD);
        CompressedDocumentPropertyState documentPropertyState = new CompressedDocumentPropertyState(mockDocumentStore, "p", "\"" + STRING_HUGEVALUE + "\"", mockCompression);

        assertEquals(documentPropertyState.getValue(Type.STRING), STRING_HUGEVALUE);

        verify(mockCompression, times(1)).getOutputStream(any(OutputStream.class));
    }

    @Test
    public void stringAboveThresholdSize() {
        DocumentNodeStore store = mock(DocumentNodeStore.class);

        CompressedDocumentPropertyState.setCompressionThreshold(DEFAULT_COMPRESSION_THRESHOLD);

        CompressedDocumentPropertyState state = new CompressedDocumentPropertyState(store, "propertyName", "\"" + STRING_HUGEVALUE + "\"", Compression.NONE);

        byte[] result = state.getCompressedValue();

        assertEquals(result.length, STRING_HUGEVALUE.length() + 2 );

        assertEquals(state.getValue(Type.STRING), STRING_HUGEVALUE);
        assertEquals(STRING_HUGEVALUE, state.getValue(Type.STRING));
    }

    @Test
    public void testInterestingStringsWithCompression() {
        DocumentNodeStore store = mock(DocumentNodeStore.class);
        String[] tests = new String[]{"simple:foo", "cr:a\n\b", "dquote:a\"b", "bs:a\\b", "euro:a\u201c", "gclef:\uD834\uDD1E",
                "tab:a\tb", "nul:a\u0000b"};

        CompressedDocumentPropertyState.setCompressionThreshold(1);
        for (String test : tests) {
            CompressedDocumentPropertyState state = new CompressedDocumentPropertyState(store, "propertyName", test, Compression.GZIP);
            assertEquals(test, state.getValue());
        }
    }

    @Test
    public void testEqualsWithCompression() throws IOException {
        DocumentNodeStore store = mock(DocumentNodeStore.class);
        String name = "propertyName";
        String value = "testValue";
        Compression compression = Compression.GZIP;

        CompressedDocumentPropertyState.setCompressionThreshold(1);
        // Create two DocumentPropertyState instances with the same properties
        CompressedDocumentPropertyState state1 = new CompressedDocumentPropertyState(store, name, value, compression);
        CompressedDocumentPropertyState state2 = new CompressedDocumentPropertyState(store, name, value, compression);

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
}
