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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.Compression;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.OutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;

public class DocumentPropertyStateFactoryTest {

    private static final int COMPRESSION_THRESHOLD = 5;
    private static final int DISABLED_COMPRESSION = -1;
    private static final String STRING_HUGEVALUE = RandomStringUtils.random(10050, "dummytest");

    @Before
    public void before() throws Exception {
        CompressedDocumentPropertyState.setCompressionThreshold(COMPRESSION_THRESHOLD);
    }

    @After
    public void tearDown() {
        CompressedDocumentPropertyState.setCompressionThreshold(DISABLED_COMPRESSION);
    }

    @Test
    public void createPropertyStateWithCompressionNone() {
        DocumentNodeStore store = Mockito.mock(DocumentNodeStore.class);
        String name = "testName";
        String value = "testValue";

        PropertyState propertyState = DocumentPropertyStateFactory.createPropertyState(store, name, "\"" + value + "\"", Compression.NONE);

        assertTrue(propertyState instanceof DocumentPropertyState);
        assertEquals(name, propertyState.getName());
        assertEquals(value, propertyState.getValue(Type.STRING));
    }

    @Test
    public void createPropertyStateWithCompressionGzip() {
        DocumentNodeStore store = Mockito.mock(DocumentNodeStore.class);
        String name = "testName";
        String value = "testValue";

        PropertyState propertyState = DocumentPropertyStateFactory.createPropertyState(store, name, "\"" + value + "\"", Compression.GZIP);

        assertTrue(propertyState instanceof CompressedDocumentPropertyState);
        assertEquals(name, propertyState.getName());
        assertEquals(value, propertyState.getValue(Type.STRING));
    }

    @Test
    public void createPropertyStateWithDefaultCompression() {
        DocumentNodeStore store = Mockito.mock(DocumentNodeStore.class);
        String name = "testName";
        String value = "testValue";

        assertEquals(CompressedDocumentPropertyState.getCompressionThreshold(), COMPRESSION_THRESHOLD);
        PropertyState propertyState = DocumentPropertyStateFactory.createPropertyState(store, name, "\"" + value + "\"");

        assertTrue(propertyState instanceof CompressedDocumentPropertyState);
        assertEquals(name, propertyState.getName());
        assertEquals(value, propertyState.getValue(Type.STRING));
    }

    @Test
    public void createPropertyStateWithCompressionThresholdExceeded() {
        DocumentNodeStore store = Mockito.mock(DocumentNodeStore.class);
        String name = "testName";
        String value = "testValue";

        PropertyState propertyState = DocumentPropertyStateFactory.createPropertyState(store, name, "\"" + value + "\"", Compression.GZIP);

        assertTrue(propertyState instanceof CompressedDocumentPropertyState);
        assertEquals(name, propertyState.getName());
        assertEquals(value, propertyState.getValue(Type.STRING));
    }

    @Test
    public void createPropertyStateWithCompressionThresholdNotExceeded() {
        DocumentNodeStore store = Mockito.mock(DocumentNodeStore.class);
        String name = "testName";
        String value = "testValue";
        CompressedDocumentPropertyState.setCompressionThreshold(15);

        PropertyState propertyState = DocumentPropertyStateFactory.createPropertyState(store, name, "\"" + value + "\"", Compression.GZIP);

        assertTrue(propertyState instanceof DocumentPropertyState);
        assertEquals(name, propertyState.getName());
        assertEquals(value, propertyState.getValue(Type.STRING));
    }

    @Test
    public void defaultValueSetToMinusOne() {
        DocumentNodeStore store = mock(DocumentNodeStore.class);
        String name = "propertyNameNew";

        CompressedDocumentPropertyState.setCompressionThreshold(-1);
        PropertyState state = DocumentPropertyStateFactory.createPropertyState(store, name, "\"" + STRING_HUGEVALUE + "\"");

        assertTrue(state instanceof DocumentPropertyState);
        assertEquals(name, state.getName());
        assertEquals(STRING_HUGEVALUE, state.getValue(Type.STRING));
    }

    @Test
    public void createPropertyStateWithCompressionThrowsException() throws IOException {
        DocumentNodeStore mockDocumentStore = mock(DocumentNodeStore.class);
        Compression mockCompression = mock(Compression.class);
        when(mockCompression.getOutputStream(any(OutputStream.class))).thenThrow(new IllegalArgumentException("Compression failed"));

        CompressedDocumentPropertyState.setCompressionThreshold(5);
        PropertyState documentPropertyState = DocumentPropertyStateFactory.createPropertyState(mockDocumentStore, "p", "\"" + "testValue" + "\"", mockCompression);

        assertEquals(documentPropertyState.getValue(Type.STRING), "testValue");

        verify(mockCompression, times(1)).getOutputStream(any(OutputStream.class));
    }
}
