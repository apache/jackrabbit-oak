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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreFixture;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RDBDocumentSerializerTest {

    private DocumentStoreFixture fixture = DocumentStoreFixture.RDB_H2;
    private DocumentStore store;
    private RDBDocumentSerializer ser;

    @Before
    public void setUp() throws Exception {
        store = fixture.createDocumentStore();
        ser = new RDBDocumentSerializer(store);
    }

    @After
    public void tearDown() throws Exception {
        fixture.dispose();
    }

    @Test
    public void testSimpleString() {
        RDBRow row = new RDBRow("_foo", 1L, true, 1l, 2l, 3l, 0L, 0L, 0L, "{}", null);
        NodeDocument doc = this.ser.fromRow(Collection.NODES, row);
        assertEquals("_foo", doc.getId());
        assertEquals(true, doc.hasBinary());
        assertEquals(true, doc.get(NodeDocument.DELETED_ONCE));
        assertEquals(2L, doc.getModCount().longValue());
    }

    @Test
    public void testNoSysprops() {
        RDBRow row = new RDBRow("_foo", null, null, 1l, 2l, 3l, 0L, 0L, 0L, "{}", null);
        NodeDocument doc = this.ser.fromRow(Collection.NODES, row);
        assertEquals("_foo", doc.getId());
        assertEquals(false, doc.hasBinary());
        assertNull(null, doc.get(NodeDocument.HAS_BINARY_FLAG));
        assertEquals(false, doc.wasDeletedOnce());
        assertNull(null, doc.get(NodeDocument.DELETED_ONCE));
        assertEquals(2L, doc.getModCount().longValue());
    }

    @Test
    public void testSimpleBlob() throws UnsupportedEncodingException {
        RDBRow row = new RDBRow("_foo", 0L, false, 1l, 2l, 3l, 0L, 0L, 0L, "\"blob\"", "{}".getBytes("UTF-8"));
        NodeDocument doc = this.ser.fromRow(Collection.NODES, row);
        assertEquals("_foo", doc.getId());
        assertEquals(false, doc.hasBinary());
        assertEquals(2L, doc.getModCount().longValue());
    }

    @Test
    public void testSimpleBlob2() throws UnsupportedEncodingException {
        RDBRow row = new RDBRow("_foo", 0L, false, 1l, 2l, 3l, 0L, 0L, 0L, "\"blob\"",
                "{\"s\":\"string\", \"b\":true, \"i\":1}".getBytes("UTF-8"));
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
            RDBRow row = new RDBRow("_foo", 1L, false, 1l, 2l, 3l, 0L, 0L, 0L, "{}", "{}".getBytes("UTF-8"));
            this.ser.fromRow(Collection.NODES, row);
            fail("should fail");
        } catch (DocumentStoreException expected) {
        }
    }

    @Test
    public void testBlobAndDiff() throws UnsupportedEncodingException {
        RDBRow row = new RDBRow("_foo", 1L, false, 1l, 2l, 3l, 0L, 0L, 0L,
                "\"blob\", [[\"=\", \"foo\", \"bar\"],[\"M\", \"m1\", 1],[\"M\", \"m2\", 3]]",
                "{\"m1\":2, \"m2\":2}".getBytes("UTF-8"));
        NodeDocument doc = this.ser.fromRow(Collection.NODES, row);
        assertEquals("bar", doc.get("foo"));
        assertEquals(2L, doc.get("m1"));
        assertEquals(3L, doc.get("m2"));
    }

    @Test
    public void testBlobAndDiffBorked() throws UnsupportedEncodingException {
        try {
            RDBRow row = new RDBRow("_foo", 1L, false, 1l, 2l, 3l, 0L, 0L, 0L, "[[\"\", \"\", \"\"]]", "{}".getBytes("UTF-8"));
            this.ser.fromRow(Collection.NODES, row);
            fail("should fail");
        } catch (DocumentStoreException expected) {
        }
    }

    @Test
    public void testNullModified() throws UnsupportedEncodingException {
        RDBRow row = new RDBRow("_foo", 1L, true, null, 2l, 3l, 0L, 0L, 0L, "{}", null);
        NodeDocument doc = this.ser.fromRow(Collection.NODES, row);
        assertNull(doc.getModified());
    }

    @Test
    public void testBrokenJSONTrailingComma() throws UnsupportedEncodingException {
        try {
            RDBRow row = new RDBRow("_foo", 1L, false, 1l, 2l, 3l, 0L, 0L, 0L, "{ \"x\" : 1, }", null);
            this.ser.fromRow(Collection.NODES, row);
            fail("should fail");
        } catch (DocumentStoreException expected) {
        }
    }

    @Test
    public void testBrokenJSONUnquotedIdentifier() throws UnsupportedEncodingException {
        try {
            RDBRow row = new RDBRow("_foo", 1L, false, 1l, 2l, 3l, 0L, 0L, 0L, "{ x : 1, }", null);
            this.ser.fromRow(Collection.NODES, row);
            fail("should fail");
        } catch (DocumentStoreException expected) {
        }
    }

    @Test
    public void testSimpleStringNonAscii() {
        RDBRow row = new RDBRow("_foo", 1L, false, 1l, 2l, 3l, 0L, 0L, 0L, "{\"x\":\"\u20ac\uD834\uDD1E\"}", null);
        NodeDocument doc = this.ser.fromRow(Collection.NODES, row);
        assertEquals("_foo", doc.getId());
        assertEquals("\u20ac\uD834\uDD1E", doc.get("x"));
    }

    @Test
    public void testValidJsonSimple() {
        RDBJSONSupport json = new RDBJSONSupport(false);
        assertNull(json.parse("null"));
        assertTrue((Boolean) json.parse("true"));
        assertFalse((Boolean) json.parse("false"));
        assertEquals(123.45, (Number) json.parse("123.45"));
        assertEquals("\r\n\t\u00e0", (String) json.parse("\"\r\n\t\u00e0\""));
    }

    @Test
    public void testValidJsonArray() {
        RDBJSONSupport json = new RDBJSONSupport(false);
        assertArrayEquals("", new Object[] { Boolean.TRUE }, ((List<Object>) json.parse("[true]")).toArray());
        assertArrayEquals("", new Object[] { Boolean.TRUE, null, 123L, "foobar" },
                ((List<Object>) json.parse("[true, null, 123, \"foobar\"]")).toArray());
    }

    @Test
    public void testValidJsonMap() {
        RDBJSONSupport json = new RDBJSONSupport(false);
        Map<String, Object> map = (Map<String, Object>)json.parse("{\"a\":true,\"b\":null,\"c\":123,\"d\":\"foobar\"}");
        assertEquals(4, map.size());
        assertTrue((Boolean)map.get("a"));
        assertTrue(map.containsKey("b"));
        assertNull(map.get("b"));
        assertEquals(123l, ((Long)map.get("c")).longValue());
        assertEquals("foobar", (String)map.get("d"));
    }

    @Test
    public void testInvalidJson() {
        RDBJSONSupport json = new RDBJSONSupport(false);
        String tests[] = new String[] { "x", "\"", "{a:1}", "[false,]" };
        for (String test : tests) {
            try {
                json.parse(test);
            } catch (IllegalArgumentException expected) {
            }
        }
    }
}
