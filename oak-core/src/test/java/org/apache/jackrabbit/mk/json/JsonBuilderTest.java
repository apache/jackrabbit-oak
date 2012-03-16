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
package org.apache.jackrabbit.mk.json;

import junit.framework.Assert;
import org.apache.jackrabbit.mk.json.JsonBuilder.JsonArrayBuilder;
import org.apache.jackrabbit.mk.json.JsonBuilder.JsonObjectBuilder;
import org.json.simple.parser.ContentHandler;
import org.json.simple.parser.JSONParser;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.math.BigDecimal;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;

public class JsonBuilderTest {

    @Test
    public void jsonBuilder() throws IOException {

        StringWriter sw = new StringWriter();
        JsonBuilder.create(sw)
                .value("foo", "bar")
                .value("int", 3)
                .value("float", 3f)
                .object("obj")
                .value("boolean", true)
                .nil("nil")
                .array("arr")
                .value(1)
                .value(2.0f)
                .value(2.0d)
                .value("42")
                .build()
                .build()
                .array("string array", new String[]{"", "1", "foo"})
                .array("int array", new int[]{1, 2, 3})
                .array("long array", new long[]{1, 2, 3})
                .array("float array", new float[]{1, 2, 3})
                .array("double array", new double[]{1, 2, 3})
                .array("boolean array", new boolean[]{true, false})
                .array("number array", new BigDecimal[]{new BigDecimal(21), new BigDecimal(42)})
                .value("some", "more")
                .build();

        String json = sw.toString();
        assertEquals("{\"foo\":\"bar\",\"int\":3,\"float\":3.0,\"obj\":{\"boolean\":true,\"nil\":null," +
                "\"arr\":[1,2.0,2.0,\"42\"]},\"string array\":[\"\",\"1\",\"foo\"],\"int array\":[1,2,3]," +
                "\"long array\":[1,2,3],\"float array\":[1.0,2.0,3.0],\"double array\":[1.0,2.0,3.0]," +
                "\"boolean array\":[true,false],\"number array\":[21,42],\"some\":\"more\"}", json);
    }

    @Test
    public void escape() throws IOException {
        StringWriter sw = new StringWriter();
        JsonBuilder.create(sw)
                .value("back\\slash", "\\")
                .value("back\\\\slash", "\\\\")
                .build();

        String json = sw.toString();
        assertEquals("{\"back\\\\slash\":\"\\\\\",\"back\\\\\\\\slash\":\"\\\\\\\\\"}", json);
    }

    @Test
    public void fixedPoint() {
        InputStream one = JsonBuilderTest.class.getResourceAsStream("test.json");
        assertNotNull(one);
        InputStreamReader isr = new InputStreamReader(one);

        String s1 = fix(isr);
        String s2 = fix(s1);

        // fix == fix fix
        assertEquals(s1, s2);
    }

    //------------------------------------------< private >---

    private static String fix(Reader reader) {
        StringWriter sw = new StringWriter();
        try {
            new JSONParser().parse(reader, new JsonHandler(JsonBuilder.create(sw)));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return sw.toString();
    }

    private static String fix(String string) {
        return fix(new StringReader(string));
    }

    private static class JsonHandler implements ContentHandler {
        private JsonObjectBuilder objectBuilder;
        private JsonArrayBuilder arrayBuilder;
        private String currentKey;

        public JsonHandler(JsonObjectBuilder objectBuilder) {
            this.objectBuilder = objectBuilder;
        }

        public void startJSON() {
            // ignore
        }

        public void endJSON() {
            // ignore
        }

        public boolean startObject() throws IOException {
            if (currentKey != null) {
                objectBuilder = objectBuilder.object(currentKey);
            }
            return true;
        }

        public boolean endObject() throws IOException {
            objectBuilder = objectBuilder.build();
            return true;
        }

        public boolean startObjectEntry(String key) throws IOException {
            currentKey = key;
            return true;
        }

        public boolean endObjectEntry() throws IOException {
            return true;
        }

        public boolean startArray() throws IOException {
            arrayBuilder = objectBuilder.array(currentKey);
            return true;
        }

        public boolean endArray() throws IOException {
            objectBuilder = arrayBuilder.build();
            arrayBuilder = null;
            return true;
        }

        public boolean primitive(Object value) throws IOException {
            if (arrayBuilder == null) {
                if (value == null) {
                    objectBuilder.nil(currentKey);
                } else if (value instanceof String) {
                    objectBuilder.value(currentKey, (String) value);
                } else if (value instanceof Integer) {
                    objectBuilder.value(currentKey, ((Integer) value).intValue());
                } else if (value instanceof Long) {
                    objectBuilder.value(currentKey, ((Long) value).longValue());
                } else if (value instanceof Double) {
                    objectBuilder.value(currentKey, ((Double) value).doubleValue());
                } else if (value instanceof Float) {
                    objectBuilder.value(currentKey, ((Float) value).floatValue());
                } else if (value instanceof Boolean) {
                    objectBuilder.value(currentKey, (Boolean) value);
                } else {
                    Assert.fail();
                }
            } else {
                if (value == null) {
                    arrayBuilder.nil();
                } else if (value instanceof String) {
                    arrayBuilder.value((String) value);
                } else if (value instanceof Integer) {
                    arrayBuilder.value(((Integer) value).intValue());
                } else if (value instanceof Long) {
                    arrayBuilder.value(((Long) value).longValue());
                } else if (value instanceof Double) {
                    arrayBuilder.value(((Double) value).doubleValue());
                } else if (value instanceof Float) {
                    arrayBuilder.value(((Float) value).floatValue());
                } else if (value instanceof Boolean) {
                    arrayBuilder.value((Boolean) value);
                } else {
                    Assert.fail();
                }
            }
            return true;
        }
    }
}
