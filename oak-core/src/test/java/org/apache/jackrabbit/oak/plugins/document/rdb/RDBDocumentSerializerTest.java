/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.document.rdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.UnsupportedEncodingException;
import java.util.Collections;

import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RDBDocumentSerializerTest  {

    private DocumentStoreFixture fixture = DocumentStoreFixture.RDB_H2;
    private DocumentStore store;
    private RDBDocumentSerializer ser;

    @Before
    public void setUp() throws Exception {
        store = fixture.createDocumentStore();
        ser = new RDBDocumentSerializer(store, Collections.singleton("_id"));
    }

    @After
    public void tearDown() throws Exception {
        fixture.dispose();
    }

    @Test
    public void testSimpleString() {
        RDBRow row = new RDBRow("_foo", true, true, 1, 2, 3, "{}", null);
        NodeDocument doc = this.ser.fromRow(Collection.NODES, row);
        assertEquals("_foo", doc.getId());
        assertEquals(true, doc.hasBinary());
        assertEquals(true, doc.get(NodeDocument.DELETED_ONCE));
        assertEquals(2L, doc.getModCount().longValue());
    }

    @Test
    public void testSimpleBlob() throws UnsupportedEncodingException {
        RDBRow row = new RDBRow("_foo", false, false, 1, 2, 3, "\"blob\"", "{}".getBytes("UTF-8"));
        NodeDocument doc = this.ser.fromRow(Collection.NODES, row);
        assertEquals("_foo", doc.getId());
        assertEquals(false, doc.hasBinary());
        assertEquals(2L, doc.getModCount().longValue());
    }

    @Test
    public void testSimpleBlob2() throws UnsupportedEncodingException {
        RDBRow row = new RDBRow("_foo", false, false, 1, 2, 3, "\"blob\"", "{\"s\":\"string\", \"b\":true, \"i\":1}".getBytes("UTF-8"));
        NodeDocument doc = this.ser.fromRow(Collection.NODES, row);
        assertEquals("_foo", doc.getId());
        assertEquals(false, doc.hasBinary());
        assertEquals(2L, doc.getModCount().longValue());
        assertEquals("string", doc.get("s"));
        assertEquals(Boolean.TRUE, doc.get("b"));
        assertEquals(1L, doc.get("i"));
    }

    @Test
    public void testSimpleBoth() throws UnsupportedEncodingException {
        try {
            RDBRow row = new RDBRow("_foo", true, false, 1, 2, 3, "{}", "{}".getBytes("UTF-8"));
            this.ser.fromRow(Collection.NODES, row);
            fail("should fail");
        }
        catch (DocumentStoreException expected) {
        }
    }

    @Test
    public void testBlobAndDiff() throws UnsupportedEncodingException {
        RDBRow row = new RDBRow("_foo", true, false, 1, 2, 3, "\"blob\", [[\"=\", \"foo\", \"bar\"],[\"M\", \"m1\", 1],[\"M\", \"m2\", 3]]", "{\"m1\":2, \"m2\":2}".getBytes("UTF-8"));
        NodeDocument doc = this.ser.fromRow(Collection.NODES, row);
        assertEquals("bar", doc.get("foo"));
        assertEquals(2L, doc.get("m1"));
        assertEquals(3L, doc.get("m2"));
    }

    @Test
    public void testBlobAndDiffBorked() throws UnsupportedEncodingException {
        try {
            RDBRow row = new RDBRow("_foo", true, false, 1, 2, 3, "[[\"\", \"\", \"\"]]", "{}".getBytes("UTF-8"));
            this.ser.fromRow(Collection.NODES, row);
            fail("should fail");
        }
        catch (DocumentStoreException expected) {
        }
    }

    @Test
    public void testBrokenJSONTrailingComma() throws UnsupportedEncodingException {
        try {
            RDBRow row = new RDBRow("_foo", true, false, 1, 2, 3, "{ \"x\" : 1, }", null);
            this.ser.fromRow(Collection.NODES, row);
            fail("should fail");
        }
        catch (DocumentStoreException expected) {
        }
    }

    @Test
    public void testBrokenJSONUnquotedIdentifier() throws UnsupportedEncodingException {
        try {
            RDBRow row = new RDBRow("_foo", true, false, 1, 2, 3, "{ x : 1, }", null);
            this.ser.fromRow(Collection.NODES, row);
            fail("should fail");
        }
        catch (DocumentStoreException expected) {
        }
    }

    @Test
    public void testSimpleStringNonAscii() {
        RDBRow row = new RDBRow("_foo", true, false, 1, 2, 3, "{\"x\":\"\u20ac\uD834\uDD1E\"}", null);
        NodeDocument doc = this.ser.fromRow(Collection.NODES, row);
        assertEquals("_foo", doc.getId());
        assertEquals("\u20ac\uD834\uDD1E", doc.get("x"));
    }
}
