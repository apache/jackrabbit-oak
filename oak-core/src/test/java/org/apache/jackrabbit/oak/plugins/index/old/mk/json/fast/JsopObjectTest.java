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
package org.apache.jackrabbit.oak.plugins.index.old.mk.json.fast;

import java.math.BigDecimal;

import junit.framework.TestCase;

/**
 * Test the Jsop class.
 */
public class JsopObjectTest extends TestCase {

    public static void main(String... args) {
        // 1000 times 'x'
        String data = new String(new char[1000]).replace((char) 0, 'x');
        for (int i = 0; i < 6; i++) {
            boolean lengthIndex = i % 2 == 0;
            JsopObject w = new JsopObject();
            w.setLengthIndex(lengthIndex);
            // 100 children with the dummy data: 100'000 characters
            for (int j = 0; j < 100; j++) {
                w.put("child" + j, data);
            }
            String jsop = w.toString();
            long start = System.nanoTime();
            for (int j = 0; j < 10000; j++) {
                JsopObject o = (JsopObject) Jsop.parse(jsop);
                assertEquals(data, o.get("child99"));
            }
            double seconds = (System.nanoTime() - start) / 1.0e9;
            System.out.format(
                    "%.2f seconds lengthIndex=%d%n", seconds, lengthIndex);
        }
    }

    public void testDataType() {
        String s = (String) Jsop.parse("\"Hello\"");
        assertEquals("Hello", s);

        s = (String) Jsop.parse("\"Line 1\\nLine 2\"");
        assertEquals("Line 1\nLine 2", s);

        BigDecimal db = (BigDecimal) Jsop.parse("1");
        assertEquals("1", db.toString());

        db = (BigDecimal) Jsop.parse("1.3e-1");
        assertEquals("0.13", db.toString());

        db = (BigDecimal) Jsop.parse("-1");
        assertEquals("-1", db.toString());

        db = (BigDecimal) Jsop.parse("10.3");
        assertEquals("10.3", db.toString());

        assertNull(Jsop.parse("null"));

        assertEquals(Boolean.TRUE, Jsop.parse("true"));
        assertEquals(Boolean.FALSE, Jsop.parse("false"));

        JsopObject o = (JsopObject) Jsop.parse("{}");
        assertEquals(0, o.size());
        assertTrue(o.isEmpty());
        assertEquals("{}", o.toString());

        JsopArray a = (JsopArray) Jsop.parse("[]");
        assertEquals(0, a.size());
        assertTrue(a.isEmpty());
        assertEquals("[]", a.toString());
    }

    public void testArray() {
        JsopArray a = (JsopArray) Jsop.parse("[1, null, \"Hello\", [], {}]");
        // this will force everything is parsed
        assertEquals(5, a.size());
        assertFalse(a.isEmpty());
        assertEquals("1", a.get(0).toString());
        assertNull(a.get(1));
        assertEquals("Hello", a.get(2));
        JsopArray a1 = (JsopArray) a.get(3);
        assertEquals(0, a1.size());
        JsopObject o1 = (JsopObject) a.get(4);
        assertEquals(0, o1.size());

        try {
            a.get(6);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // expected
        }
        String s = "";
        for (Object o : a) {
            s += o + ";";
        }
        assertEquals("1;null;Hello;[];{};", s);
    }

    public void testArrayLazyInit() {
        JsopArray a = (JsopArray) Jsop.parse("[1, null, \"Hello\", [], {}]");
        assertEquals("1", a.get(0).toString());
        assertNull(a.get(1));
        assertEquals("Hello", a.get(2));
        JsopArray a1 = (JsopArray) a.get(3);
        assertEquals(0, a1.size());
        JsopObject o1 = (JsopObject) a.get(4);
        assertEquals(0, o1.size());
        assertEquals(5, a.size());
        assertFalse(a.isEmpty());
    }

    public void testArrayCreate() {
        JsopArray array = new JsopArray();
        array.add("test");
        array.add(1);
        assertEquals("[\"test\",1]", array.toString());
        array.clear();
        array.add(null);
        array.add(true);
        array.add(false);
        assertEquals("[null,true,false]", array.toString());
    }

    public void testObject() {
        JsopObject o = (JsopObject) Jsop.parse("{\"a\": 1, \"b\": null, \"c\": true, \"d\": {}, \"e\": []}");
        // this will force everything is parsed
        assertEquals(5, o.size());
        assertFalse(o.isEmpty());
        assertEquals("1", o.get("a").toString());
        assertNull(o.get("b"));
        assertEquals(true, o.get("c"));
        JsopObject o1 = (JsopObject) o.get("d");
        assertEquals(0, o1.size());
        JsopArray a1 = (JsopArray) o.get("e");
        assertEquals(0, a1.size());
        assertTrue(o.containsKey("a"));
        assertTrue(o.containsKey("b"));
        assertFalse(o.containsKey("x"));
    }

    public void testObjectLazyInit() {
        JsopObject a = (JsopObject) Jsop.parse("{\"a\": 1, \"b\": null, \"c\": true, \"d\": {}, \"e\": []}");
        assertEquals("1", a.get("a").toString());
        assertNull(a.get("b"));
        assertEquals(true, a.get("c"));
        JsopObject a1 = (JsopObject) a.get("d");
        assertEquals(0, a1.size());
        JsopArray o1 = (JsopArray) a.get("e");
        assertEquals(0, o1.size());
    }

    public void testObjectCreate() {
        JsopObject o = new JsopObject();
        o.setLengthIndex(true);
        o.put("a", true);
        o.put("b", false);
        o.put("c", null);
        o.put("d", "Hello");
        o.put("e", new JsopObject());
        o.put("f", new JsopArray());
        String s = o.toString();
        assertEquals("{\":lengths:\":\"4,5,4,7,2,2\",\"a\":true,\"b\":false,\"c\":null,\"d\":\"Hello\",\"e\":{},\"f\":[]}", s);
        o = (JsopObject) Jsop.parse(s);
        assertEquals(Boolean.TRUE, o.get("a"));
        assertEquals(Boolean.FALSE, o.get("b"));
        assertNull(o.get("c"));
        assertEquals("Hello", o.get("d"));
        assertEquals("{}", o.get("e").toString());
        assertEquals("[]", o.get("f").toString());
    }

    public void testObjectCreateClear() {
        JsopObject o = new JsopObject();
        o.put("a", true);
        o.put("b", false);
        assertEquals(true, o.get("a"));
        assertEquals(false, o.get("b"));
        assertEquals(2, o.size());
        o.clear();
        assertEquals(0, o.size());
    }

}
