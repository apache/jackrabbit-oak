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

/**
 * Test the Jsop tokenizer and builder.
 */
public class JsopTest extends TestCase {

    // run the micro-benchmark
    public static void main(String... args) {
        for (int k = 0; k < 5; k++) {
            // String s = "Hello World Hello World Hello World Hello World Hello World Hello World ";
            String s = "Hello \"World\" Hello \"World\" Hello \"World\" Hello \"World\" Hello \"World\" Hello \"World\" ";
            StopWatch timer = new StopWatch();
            int t2 = 0;
            for (int i = 0; i < 1000000; i++) {
                t2 += JsopBuilder.encode(s).length();
            }
            System.out.println(timer.seconds() + " dummy: " + t2);
        }
        // old: not escaped: 5691 ms; escaped: 10609 ms
        // new: not escaped: 3931 ms; escaped: 11001 ms
    }

    public void testDataType() {
        String dateString = new JsopBuilder().
                key("string").value("/Date(0)/").
                key("date").encodedValue("\"\\/Date(0)\\/\"").
                toString();
        assertEquals(
                "\"string\":\"/Date(0)/\"," +
                        "\"date\":\"\\/Date(0)\\/\"",
                dateString);
        JsopTokenizer t = new JsopTokenizer(dateString);
        assertEquals("string", t.readString());
        t.read(':');
        assertEquals("/Date(0)/", t.readString());
        assertEquals("/Date(0)/", t.getEscapedToken());
        t.read(',');
        assertEquals("date", t.readString());
        t.read(':');
        assertEquals("/Date(0)/", t.readString());
        assertEquals("\\/Date(0)\\/", t.getEscapedToken());
    }

    public void testNullTrueFalse() {
        JsopTokenizer t;
        t = new JsopTokenizer("null, 1, null, true, false");
        assertEquals(null, t.read(JsopReader.NULL));
        assertEquals(",", t.read(','));
        assertEquals("1", t.read(JsopReader.NUMBER));
        assertEquals(",", t.read(','));
        assertEquals(null, t.read(JsopReader.NULL));
        assertEquals(",", t.read(','));
        assertEquals("true", t.read(JsopReader.TRUE));
        assertEquals(",", t.read(','));
        assertEquals("false", t.read(JsopReader.FALSE));
        t = new JsopTokenizer("true, false");
        assertEquals("true", t.read(JsopReader.TRUE));
        assertEquals(",", t.read(','));
        assertEquals("false", t.read(JsopReader.FALSE));
        t = new JsopTokenizer("false, true");
        assertEquals("false", t.read(JsopReader.FALSE));
        assertEquals(",", t.read(','));
        assertEquals("true", t.read(JsopReader.TRUE));
    }

    public void testLineLength() {
        JsopBuilder buff = new JsopBuilder();
        buff.key("hello").value("world");
        assertEquals("\"hello\":\"world\"", buff.toString());
        assertEquals(15, buff.length());
        buff = new JsopBuilder();

        buff.setLineLength(10);
        buff.key("hello").value("world");
        assertEquals("\"hello\":\n\"world\"", buff.toString());
        assertEquals(16, buff.length());
    }

    public void testNumber() {
        JsopTokenizer t = new JsopTokenizer("9/3:-3-:-/- 3");
        assertEquals("9", t.read(JsopReader.NUMBER));
        t.read('/');
        assertEquals("3", t.read(JsopReader.NUMBER));
        t.read(':');
        assertEquals("-3", t.read(JsopReader.NUMBER));
        t.read('-');
        t.read(':');
        t.read('-');
        t.read('/');
        t.read('-');
        t.read(JsopReader.NUMBER);
    }

    public void testRawValue() {
        JsopTokenizer t;
        t = new JsopTokenizer("");
        try {
            t.readRawValue();
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        t = new JsopTokenizer("[unclosed");
        try {
            t.readRawValue();
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
        t = new JsopTokenizer("{\"x\": [1], null, true, {\"y\": 1}, [1, 2], [], [[1]], +error+}");
        t.read('{');
        assertEquals("x", t.readString());
        t.read(':');
        assertEquals("[1]", t.readRawValue());
        t.read(',');
        assertEquals("null", t.readRawValue());
        t.read(',');
        assertEquals("true", t.readRawValue());
        t.read(',');
        assertEquals("{\"y\": 1}", t.readRawValue());
        t.read(',');
        assertEquals("[1, 2]", t.readRawValue());
        t.read(',');
        assertEquals("[]", t.readRawValue());
        t.read(',');
        assertEquals("[[1]]", t.readRawValue());
        t.read(',');
        try {
            t.readRawValue();
            fail();
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    public void testTokenizer() {
        assertEquals("test", JsopTokenizer.decode("test"));
        assertEquals("test", JsopTokenizer.decodeQuoted("\"test\""));
        assertEquals("hello\n" + "world", JsopTokenizer.decodeQuoted("\"hello\\n" + "world\""));
        try {
            JsopTokenizer.decodeQuoted("test");
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }
        try {
            JsopTokenizer.decode("test\\");
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }
        try {
            JsopTokenizer.decode("wrong\\uxxxx");
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }
        try {
            JsopTokenizer.decode("wrong\\m");
            fail();
        } catch (IllegalArgumentException e) {
            // ok
        }
        test("/error/", "\"\\");
        test("/error/1", ".1");
        assertEquals("x", new JsopTokenizer("x").toString());
        test("/id:truetrue/", "true" + "true");
        test("/id:truer/", "truer");
        test("/id:falsehood/", "falsehood");
        test("/id:nil/", "nil");
        test("/id:nil/1", "nil 1");
        test("/error/", "\"invalid");
        test("- \"test/test\"", "-\"test\\/test\"");
        test(" {\n\"x\": 1,\n\"y\": 2\n}\n", "{\"x\":1, \"y\":2}");
        test("[true, false, null]", "[true, false, null]");
        test("\"\"", "\"\"");
        test("\"\\u0000\"", "\"\\u0000\"");
        test("\"\\u0001\"", "\"\\u0001\"");
        test("\"\\u0002\"", "\"\\u0002\"");
        test("\"\\u0003\"", "\"\\u0003\"");
        test("\"\\u0004\"", "\"\\u0004\"");
        test("\"\\u0005\"", "\"\\u0005\"");
        test("\"\\u0006\"", "\"\\u0006\"");
        test("\"\\u0007\"", "\"\\u0007\"");
        test("\"\\b\"", "\"\\u0008\"");
        test("\"\\t\"", "\"\\u0009\"");
        test("\"\\n\"", "\"\\u000a\"");
        test("\"\\u000b\"", "\"\\u000b\"");
        test("\"\\f\"", "\"\\u000c\"");
        test("\"\\r\"", "\"\\u000d\"");
        test("\"\\u000e\"", "\"\\u000e\"");
        test("\"\\u000f\"", "\"\\u000f\"");
        test("\"\\u0010\"", "\"\\u0010\"");
        test("\"\\u0011\"", "\"\\u0011\"");
        test("\"\\u0012\"", "\"\\u0012\"");
        test("\"\\u0013\"", "\"\\u0013\"");
        test("\"\\u0014\"", "\"\\u0014\"");
        test("\"\\u0015\"", "\"\\u0015\"");
        test("\"\\u0016\"", "\"\\u0016\"");
        test("\"\\u0017\"", "\"\\u0017\"");
        test("\"\\u0018\"", "\"\\u0018\"");
        test("\"\\u0019\"", "\"\\u0019\"");
        test("\"\\u001a\"", "\"\\u001a\"");
        test("\"\\u001b\"", "\"\\u001b\"");
        test("\"\\u001c\"", "\"\\u001c\"");
        test("\"\\u001d\"", "\"\\u001d\"");
        test("\"\\u001e\"", "\"\\u001e\"");
        test("\"\\u001f\"", "\"\\u001f\"");
        test("\"\u0123\"", "\"\\u0123\"");
        test("\"\u1234\"", "\"\\u1234\"");
        test("\"-\\\\-\\\"-\\b-\\f-\\n-\\r-\\t\"", "\"-\\\\-\\\"-\\b-\\f-\\n-\\r-\\t\"");
        test("\"-\\b-\\f-\\n-\\r-\\t\"", "\"-\b-\f-\n-\r-\t\"");
        test("[0, 12, -1, 0.1, -0.1, -2.3e1, 1e+1, 1.e-20]", "[0,12,-1,0.1,-0.1,-2.3e1,1e+1,1.e-20]");
        test("\"Hello\"", "\"Hello\"");
        test("[]", "[]");
        test(" {\n\n}\n", "{}");
        test(" {\n\"a\": /* test */ 10\n}\n", "{ \"a\": /* test */ 10}");
        test("+ - / ^ ", "+ - / ^");
        test("/*/ comment /*/ ", "/*/ comment /*/");
        test("/**/ /id:comment//**/ ", "/**/ comment /**/");

        JsopTokenizer t = new JsopTokenizer("{}123");
        assertFalse(t.matches('+'));
        assertTrue(t.matches('{'));
        t.read('}');

        try {
            t.read('+');
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("{}123[*] expected: '+'", e.getMessage());
        }
        try {
            t.read(JsopReader.STRING);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("{}123[*] expected: string", e.getMessage());
        }

    }

    public void testSurrogates() {
        String[][] tests = { { "surrogate-ok: \uD834\uDD1E", "surrogate-ok: \uD834\uDD1E" },
                { "surrogate-broken: \ud800 ", "surrogate-broken: \\ud800 " },
                { "surrogate-truncated: \ud800", "surrogate-truncated: \\ud800" } };

        for (String[] test : tests) {
            StringBuilder buff = new StringBuilder();
            JsopBuilder.escape(test[0], buff);
            assertEquals(test[1], buff.toString());
            
            String s2 = JsopBuilder.encode(test[0]);
            assertEquals("\"" + test[1] + "\"", s2);
            String s3 = JsopTokenizer.decodeQuoted(s2);
            assertEquals(test[0], s3);
        }
    }

    static void test(String expected, String json) {
        String j2 = prettyPrintWithErrors(json);
        assertEquals(expected, j2);
    }

    static String prettyPrintWithErrors(String jsop) {
        StringBuilder buff = new StringBuilder();
        JsopTokenizer t = new JsopTokenizer(jsop);
        while (true) {
            prettyPrint(buff, t, "");
            if (t.getTokenType() == JsopReader.END) {
                return buff.toString();
            }
        }
    }

    static String prettyPrint(StringBuilder buff, JsopTokenizer t, String ident) {
        String space = "";
        boolean inArray = false;
        while (true) {
            switch (t.read()) {
                case JsopReader.END:
                    return buff.toString();
                case JsopReader.STRING:
                    buff.append(JsopBuilder.encode(t.getToken()));
                    break;
                case JsopReader.NUMBER:
                    buff.append(t.getToken());
                    break;
                case JsopReader.TRUE:
                    buff.append("true");
                    break;
                case JsopReader.FALSE:
                    buff.append("false");
                    break;
                case JsopReader.NULL:
                    buff.append("null");
                    break;
                case JsopReader.ERROR:
                    buff.append("/error/");
                    break;
                case JsopReader.IDENTIFIER:
                    buff.append("/id:").append(t.getToken()).append('/');
                    break;
                case JsopReader.COMMENT:
                    buff.append("/*").append(t.getToken()).append("*/ ");
                    break;
                case '{':
                    buff.append(" {\n").append(space += ident);
                    break;
                case '}':
                    space = space.substring(0, space.length() - ident.length());
                    buff.append('\n').append(space).append("}\n").append(space);
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
                case ':':
                    buff.append(": ");
                    break;
                case '+':
                    buff.append("+ ");
                    break;
                case '-':
                    buff.append("- ");
                    break;
                case '^':
                    buff.append("^ ");
                    break;
                case '/':
                    buff.append("/ ");
                    break;
                default:
                    throw new AssertionError("token type: " + t.getTokenType());
            }
        }
    }

    public void testBuilder() {

        JsopBuilder buff = new JsopBuilder();
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

        buff = new JsopBuilder();
        buff.tag('+').key("x").value("1").newline();
        buff.tag('+').key("y").value("2").newline();
        json = buff.toString();
        assertEquals("+\"x\":\"1\"\n+\"y\":\"2\"\n", json);

    }

    public void testEscape() {
        assertEquals("null", JsopBuilder.encode(null));
        JsopBuilder buff = new JsopBuilder().
                key("back\\slash").value("\\").
                key("back\\\\slash").value("\\\\");
        assertEquals("\"back\\\\slash\":\"\\\\\",\"back\\\\\\\\slash\":\"\\\\\\\\\"", buff.toString());
    }

    public void testPrettyPrint() {
        assertEquals("{}", JsopBuilder.prettyPrint("{}"));
        assertEquals("{\n  \"a\": 1,\n  \"b\": \"Hello\"\n}",
                JsopBuilder.prettyPrint("{\"a\":1,\"b\":\"Hello\"}"));
        assertEquals("{\n  \"a\": [1, 2]\n}",
                JsopBuilder.prettyPrint("{\"a\":[1, 2]}"));
    }

    public static String format(String json) {
        return prettyPrint(new StringBuilder(),
                new JsopTokenizer(json), "    ");
    }

}
