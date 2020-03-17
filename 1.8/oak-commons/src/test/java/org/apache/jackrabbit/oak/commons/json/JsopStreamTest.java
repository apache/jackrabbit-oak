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
package org.apache.jackrabbit.oak.commons.json;

import junit.framework.TestCase;
import org.apache.jackrabbit.oak.commons.StopWatch;

public class JsopStreamTest extends TestCase {

    // run the micro-benchmark
    public static void main(String... args) {
        for (int k = 0; k < 5; k++) {
            String s = "Hello \"World\" Hello \"World\" Hello \"World\" Hello \"World\" Hello \"World\" Hello \"World\" ";
            StopWatch timer = new StopWatch();
            JsopWriter w = k % 2 == 1 ? new JsopBuilder() : new JsopStream();
            for (int i = 0; i < 1000000; i++) {
                w.value(s);
                if (i % 100 == 0) {
                    w.resetWriter();
                }
            }
            System.out.println(w.getClass() + ": " + timer.seconds());
        }
        // JsopStream: 20
        // JsopBuilder: 1150
    }

    public void testNested() {
        JsopStream s = new JsopStream().key("x");
        JsopStream nested = new JsopStream().array().value(1).value(null).value(true).value(false).value("Hello").endArray();
        s.append(nested);
        assertEquals("\"x\":[1,null,true,false,\"Hello\"]", s.toString());
    }

    public void testRawValue() {
        JsopStream s = new JsopStream().tag('+').
        key("x").object().
            key("y").array().value(1).array().endArray().value(2).endArray().endObject();
        assertEquals("+\"x\":{\"y\":[1,[],2]}", s.toString());
        testRawValue(s);
        testRawValue(new JsopTokenizer(s.toString()));
    }

    private static void testRawValue(JsopReader s) {
        for (int i = 0; i < 3; i++) {
            assertFalse(s.matches('-'));
            assertTrue(s.matches('+'));
            assertEquals("x", s.read(JsopReader.STRING));
            s.read(':');
            assertEquals("{", s.read('{'));
            assertEquals("y", s.readString());
            s.read(':');
            assertEquals("[1,[],2]", s.readRawValue());
            s.read('}');
            s.read(JsopReader.END);
            s.resetReader();
        }
    }

    public void testJsopReader() {
        JsopStream s = new JsopStream().tag('+').
            key("x").object().
                key("y").value(1).
                key("n").value("").
                key("z").encodedValue("n10").
                endObject();
        s.setLineLength(-1);
        assertEquals("+\"x\":{\"y\":1,\"n\":\"\",\"z\":n10}", s.toString());
        assertFalse(s.matches('-'));
        assertTrue(s.matches('+'));
        assertEquals("x", s.read(JsopReader.STRING));
        try {
            s.read('}');
            fail();
        } catch (Exception e) {
            // expected
        }
        s.read(':');
        assertEquals("{", s.read('{'));
        assertEquals("y", s.readString());
        s.read(':');
        assertEquals("1", s.readRawValue());
        s.read(',');
        assertEquals("n", s.readString());
        s.read(':');
        assertEquals("\"\"", s.readRawValue());
        s.read(',');
        assertEquals("z", s.readString());
        s.read(':');
        assertEquals("n10", s.readRawValue());
        s.read('}');
    }

    public void testTokenizer() {
        test("+ \"x\": {}",
                new JsopStream().tag('+').
                    key("x").object().endObject());
        test("[\"-1\": -1, " +
                "\"true\": true, " +
                "\"false\": false, " +
                "\"null\": null]",
                new JsopStream().array().
                    key("-1").value(-1).
                    key("true").value(true).
                    key("false").value(false).
                    key("null").value(null).
                    endArray());

    }

    static void test(String expected, JsopReader t) {
        String j2 = prettyPrint(t);
        assertEquals(expected, j2);
        j2 = prettyPrint(new JsopTokenizer(j2));
        assertEquals(expected, j2);
    }

    public static String prettyPrint(JsopReader t) {
        StringBuilder buff = new StringBuilder();
        while (true) {
            prettyPrint(buff, t, "  ");
            if (t.getTokenType() == JsopReader.END) {
                return buff.toString();
            }
        }
    }

    static String prettyPrint(StringBuilder buff, JsopReader t, String ident) {
        String space = "";
        boolean inArray = false;
        while (true) {
            int token = t.read();
            switch (token) {
                case JsopReader.END:
                    return buff.toString();
                case JsopReader.STRING:
                    buff.append(JsopBuilder.encode(t.getToken()));
                    break;
                case JsopReader.NUMBER:
                case JsopReader.TRUE:
                case JsopReader.FALSE:
                case JsopReader.NULL:
                case JsopReader.IDENTIFIER:
                case JsopReader.ERROR:
                    buff.append(t.getToken());
                    break;
                case '{':
                    if (t.matches('}')) {
                        buff.append("{}");
                    } else {
                        buff.append("{\n").append(space += ident);
                    }
                    break;
                case '}':
                    space = space.substring(0, space.length() - ident.length());
                    buff.append('\n').append(space).append("}");
                    break;
                case '[':
                    inArray = true;
                    buff.append("[");
                    break;
                case ']':
                    inArray = false;
                    buff.append("]");
                    break;
                case ',':
                    if (!inArray) {
                        buff.append(",\n").append(space);
                    } else {
                        buff.append(", ");
                    }
                    break;
                default:
                    buff.append((char) token).append(' ');
                    break;
            }
        }
    }

    public static void testBuilder() {

        JsopWriter buff = new JsopStream();
        buff.tag('+').object().
                key("foo").value("bar").
                key("int").value(3).
                key("decimal").encodedValue("3.0").
                key("obj").object().
                key("boolean").value(true).
                key("null").value(null).
                key("arr").array().
                array().
                value(1).
                value("\u001f ~ \u007f \u0080").
                value("42").
                endArray().
                array().
                endArray().
                endArray().
                endObject().
                key("some").value("more").
                endObject();

        String json = buff.toString();
        assertEquals("+{\"foo\":\"bar\",\"int\":3,\"decimal\":3.0," +
                "\"obj\":{\"boolean\":true,\"null\":null," +
                "\"arr\":[[1,\"\\u001f ~ \u007f \u0080\",\"42\"],[]]},\"some\":\"more\"}", json);

        buff.resetWriter();
        buff.array().
                object().key("x").value("1").endObject().newline().
                object().key("y").value("2").endObject().newline().
                endArray();
        json = buff.toString();
        assertEquals("[{\"x\":\"1\"}\n,{\"y\":\"2\"}\n]", json);

        buff = new JsopStream();
        buff.tag('+').key("x").value("1").newline();
        buff.tag('+').key("y").value("2").newline();
        buff.tag('+').key("z").value(false).newline();
        json = buff.toString();
        assertEquals("+\"x\":\"1\"\n+\"y\":\"2\"\n+\"z\":false\n", json);

    }

}

