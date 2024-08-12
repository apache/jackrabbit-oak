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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.Compression;
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

        PropertyState propertyState = DocumentPropertyStateFactory.createPropertyState(store, name, "\"" + value + "\"");

        assertTrue(propertyState instanceof DocumentPropertyState);
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
        CompressedDocumentPropertyState.setCompressionThreshold(15);
        String value = "testValue";

        PropertyState propertyState = DocumentPropertyStateFactory.createPropertyState(store, name, "\"" + value + "\"", Compression.GZIP);

        assertTrue(propertyState instanceof DocumentPropertyState);
        assertEquals(name, propertyState.getName());
        assertEquals(value, propertyState.getValue(Type.STRING));
    }

    @Test(expected = IllegalArgumentException.class)
    public void createPropertyStateWithCompressionThresholdExceededFail() throws IOException {
        DocumentNodeStore mockDocumentStore = mock(DocumentNodeStore.class);
        Compression mockCompression = mock(Compression.class);
        when(mockCompression.getOutputStream(any(OutputStream.class))).thenThrow(new IOException("Compression failed"));

        CompressedDocumentPropertyState.setCompressionThreshold(5);
        CompressedDocumentPropertyState documentPropertyState = new CompressedDocumentPropertyState(mockDocumentStore, "p", "\"" + "testValue" + "\"", mockCompression);

        assertEquals(documentPropertyState.getValue(Type.STRING), "tesValue");

        verify(mockCompression, times(1)).getOutputStream(any(OutputStream.class));
    }
}
