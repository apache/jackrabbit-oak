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
package org.apache.jackrabbit.oak.query.ast;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.text.ParseException;

import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextExpression;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextParser;
import org.apache.jackrabbit.oak.spi.query.fulltext.FullTextTerm;
import org.junit.Test;

/**
 * Test the fulltext parsing and evaluation.
 */
public class FullTextTest {
    
    @Test
    public void fulltextChars() {
        for (char c : "a\u00e4\u00dfzAZ\u00c409+\u3360".toCharArray()) {
            assertTrue("char:" + c, FullTextTerm.isFullTextCharacter(c));
        }
        for (char c : "@-.,;!?\t\n\f".toCharArray()) {
            assertFalse("char:" + c, FullTextTerm.isFullTextCharacter(c));
        }
    }

    @Test
    public void and() throws ParseException {
        assertEquals("\"hello\" \"world\"", convertPattern("hello world"));
        assertEquals("\"hello\" \"world\"", convertPattern("hello  world"));
        assertEquals("\"hello\" \"world\"", convertPattern("hello   world"));
        assertEquals("\"hello\" \"world\"", convertPattern("hello \t  world"));
        assertEquals("\"hello\" \"or\" \"world\"", convertPattern("hello or world"));
        assertFalse(test("hello world", "hello"));
        assertFalse(test("hello world", "world"));
        assertTrue(test("hello world", "world hello"));
        assertTrue(test("hello world ", "hello world"));
        assertEquals("\"hello\"", convertPattern("hello hello"));
    }

    @Test
    public void or() throws ParseException {
        assertEquals("\"hello\" OR \"world\"", convertPattern("hello OR world"));
        assertTrue(test("hello OR world", "hello"));
        assertTrue(test("hello OR world", "world"));
        assertFalse(test("hello OR world", "hi"));
    }

    @Test
    public void minusLiteral() throws ParseException {
        assertEquals("\"-\"", convertPattern("-"));
        assertEquals("\"-\"", convertPattern("- "));
        assertEquals("\"-\"", convertPattern("- -"));
        assertEquals("\"-\" \"hello\"", convertPattern("- hello"));
        assertEquals("\"hello\" \"-\" \"world\"", convertPattern("hello - world"));
        assertEquals("\"hello\" \"-\" \"world\"", convertPattern("hello  -  world"));
        assertEquals("\"hello\" \"-\"", convertPattern("hello -"));
        assertEquals("\"hello\" \"-\"", convertPattern("hello -"));
        assertEquals("\"hello\" \"-\"", convertPattern("hello - hello"));

        assertTrue(test("-", "hello - world"));
        assertTrue(test("- ", "hello - world"));
        assertTrue(test("- -", "hello - world"));
        assertTrue(test("- hello", "hello - world"));
        assertTrue(test("hello - world", "hello - world"));
        assertTrue(test("hello - \"wonderful world\"", "hello - wonderful world"));
        assertTrue(test("hello -", "hello -"));
    }

    @Test
    public void not() throws ParseException {
        assertEquals("\"hello\" -\"wonderful world\"", convertPattern("hello -\"wonderful world\""));
        assertTrue(test("hello -\"wonderful world\"", "hello"));
        assertTrue(test("hello -\"wonderful world\"", "hello wonderful"));
        assertTrue(test("hello -\"wonderful world\"", "hello world"));
        assertFalse(test("hello -\"wonderful world\"", "hello wonderful world"));
        assertFalse(test("hello -\"wonderful world\"", "wonderful world"));
        assertTrue(test("hello \"-wonderful world\"", "hello this beautiful -wonderful world"));
        assertEquals("\"hello\" -\"world\"", convertPattern("hello -world"));
        assertTrue(test("hello -world", "hello"));
        assertFalse(test("hello -world", "hello world"));
    }

    @Test
    public void quoted() throws ParseException {
        assertEquals("\"hello world\"", convertPattern("\"hello world\""));
        assertEquals("\"hello world\" \"world\"", convertPattern("\"hello world\" world"));
        assertTrue(test("\"hello world\"", "hello world"));
        assertFalse(test("\"hello world\"", "world hello"));
        assertTrue(test("\"hello-world\"", "hello-world"));
        assertTrue(test("\"hello\\-world\"", "hello-world"));
        assertTrue(test("\"hello \\\"world\\\"\"", "hello \"world\""));
        assertTrue(test("\"hello world\" -hallo", "hello world"));
        assertFalse(test("\"hello world\" -hallo", "hallo hello world"));
    }

    @Test
    public void escaped() throws ParseException {
        assertEquals("\"\\\"hello world\\\"\"", convertPattern("\"\\\"hello world\\\"\""));
        assertFalse(test("\\\"hello\\\"", "hello"));
        assertTrue(test("\"hello\"", "\"hello\""));
        assertTrue(test("\\\"hello\\\"", "\"hello\""));
        assertFalse(test("\\-1 2 3", "1 2 3"));
        assertTrue(test("\\-1 2 3", "-1 2 3"));
    }

    @Test
    public void boost() throws ParseException {
        assertEquals("\"hello\"^2", convertPattern("hello^2"));
        assertEquals("\"hello world\"^2", convertPattern("\"hello world\"^2"));
        assertTrue(test("hello^2", "hello"));
        assertTrue(test("\"hello\"^0.2", "hello"));
    }

    @Test
    public void invalid() throws ParseException {
        testInvalid("", "(*); expected: term");
        testInvalid("x OR ", "x OR(*); expected: term");
        testInvalid("\"", "(*)\"; expected: double quote");
        testInvalid("\\", "(*)\\; expected: escaped char");
        testInvalid("\"\\", "\"(*)\\; expected: escaped char");
        testInvalid("\"x\"y", "\"x\"(*)y; expected: space");
    }

    private static void testInvalid(String pattern, String expectedMessage) {
        try {
            test(pattern, "");
            fail("Expected exception " + expectedMessage);
        } catch (ParseException e) {
            String msg = e.getMessage();
            assertTrue(msg.startsWith("FullText expression: "));
            msg = msg.substring("FullText expression: ".length());
            assertEquals(expectedMessage, msg);
        }
    }
    
    private static String convertPattern(String pattern) throws ParseException {
        FullTextExpression e = FullTextParser.parse(null, pattern);
        return e.toString();
    }

    private static boolean test(String pattern, String value) throws ParseException {
        FullTextExpression e = FullTextParser.parse(null, pattern);
        return e.evaluate(value);
    }

}
