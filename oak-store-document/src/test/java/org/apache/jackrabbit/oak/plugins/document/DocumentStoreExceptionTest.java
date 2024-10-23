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
import java.util.List;

import org.apache.jackrabbit.guava.common.collect.Lists;

import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException.Type;
import org.junit.Test;

import static org.apache.jackrabbit.oak.plugins.document.DocumentStoreException.Type.GENERIC;
import static org.apache.jackrabbit.oak.plugins.document.DocumentStoreException.Type.TRANSIENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class DocumentStoreExceptionTest {

    @Test
    public void singleParamMessage() {
        String msg = "foo";
        DocumentStoreException dse = new DocumentStoreException(msg);
        assertNull(dse.getCause());
        assertEquals(msg, dse.getMessage());
        assertEquals(GENERIC, dse.getType());
    }

    @Test
    public void singleParamMessageNull() {
        DocumentStoreException dse = new DocumentStoreException((String) null);
        assertNull(dse.getCause());
        assertNull(dse.getMessage());
        assertEquals(GENERIC, dse.getType());
    }

    @Test
    public void singleParamCause() {
        String msg = "foo";
        Exception cause = new IllegalArgumentException(msg);
        DocumentStoreException dse = new DocumentStoreException(cause);
        assertSame(cause, dse.getCause());
        assertTrue(dse.getMessage().contains(IllegalArgumentException.class.getSimpleName()));
        assertTrue(dse.getMessage().contains(msg));
        assertEquals(GENERIC, dse.getType());
    }

    @Test
    public void singleParamCauseNull() {
        DocumentStoreException dse = new DocumentStoreException((Throwable) null);
        assertNull(dse.getCause());
        assertNull(dse.getMessage());
        assertEquals(GENERIC, dse.getType());
    }

    @Test
    public void dualParamMessageCause() {
        String msg = "foo";
        Exception cause = new IllegalArgumentException(msg);
        DocumentStoreException dse = new DocumentStoreException(msg, cause);
        assertSame(cause, dse.getCause());
        assertEquals(msg, dse.getMessage());
        assertEquals(GENERIC, dse.getType());
    }

    @Test
    public void dualParamMessageThrowableNull() {
        String msg = "foo";
        DocumentStoreException dse = new DocumentStoreException(msg, null);
        assertNull(dse.getCause());
        assertEquals(msg, dse.getMessage());
        assertEquals(GENERIC, dse.getType());
    }

    @Test
    public void tripleParamMessageCauseType() {
        String msg = "foo";
        Exception cause = new IOException(msg);
        Type type = TRANSIENT;
        DocumentStoreException dse = new DocumentStoreException(msg, cause, type);
        assertEquals(msg, dse.getMessage());
        assertSame(cause, dse.getCause());
        assertEquals(type, dse.getType());
    }

    @Test
    public void convertDocumentStoreException() {
        Exception cause = new DocumentStoreException("foo");
        DocumentStoreException dse = DocumentStoreException.convert(cause);
        assertSame(cause, dse);
    }

    @Test
    public void convertIOException() {
        String msg = "foo";
        Exception cause = new IOException(msg);
        DocumentStoreException dse = DocumentStoreException.convert(cause);
        assertEquals(msg, dse.getMessage());
        assertSame(cause, dse.getCause());
        assertEquals(GENERIC, dse.getType());
    }

    @Test
    public void convertIOExceptionWithMessage() {
        String msg = "foo";
        Exception cause = new IOException();
        DocumentStoreException dse = DocumentStoreException.convert(cause, msg);
        assertEquals(msg, dse.getMessage());
        assertSame(cause, dse.getCause());
        assertEquals(GENERIC, dse.getType());
    }

    @Test
    public void convertIOExceptionWithIDs() {
        String msg = "foo";
        List<String> ids = List.of("one", "two", "three");
        Exception cause = new IOException(msg);
        DocumentStoreException dse = DocumentStoreException.convert(cause, ids);
        assertTrue(dse.getMessage().contains(msg));
        for (String id : ids) {
            assertTrue(dse.getMessage().contains(id));
        }
        assertSame(cause, dse.getCause());
        assertEquals(GENERIC, dse.getType());
    }

    @Test
    public void asDocumentStoreException() {
        String msg = "foo";
        Exception cause = new IOException();
        Type type = TRANSIENT;
        List<String> ids = List.of("one", "two", "three");
        DocumentStoreException dse = DocumentStoreException.asDocumentStoreException(msg, cause, type, ids);
        assertTrue(dse.getMessage().contains(msg));
        for (String id : ids) {
            assertTrue(dse.getMessage().contains(id));
        }
        assertSame(cause, dse.getCause());
        assertEquals(type, dse.getType());
    }
}
