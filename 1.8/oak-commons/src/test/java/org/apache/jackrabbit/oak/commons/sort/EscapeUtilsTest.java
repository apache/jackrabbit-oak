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

package org.apache.jackrabbit.oak.commons.sort;

import java.util.Random;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EscapeUtilsTest {

    @Test
    public void noOp() throws Exception{
        assertEquals(null, EscapeUtils.escapeLineBreak(null));
        assertEquals("abc", EscapeUtils.escapeLineBreak("abc"));
        assertEquals("", EscapeUtils.escapeLineBreak(""));
        assertEquals("some text with multi byte 田中 characters.",
                EscapeUtils.escapeLineBreak("some text with multi byte 田中 characters."));
    }

    @Test
    public void testEscape() throws Exception{
        assertEquals("ab\\nc\\r", EscapeUtils.escapeLineBreak("ab\nc\r"));
        assertEquals("a\\\\z", EscapeUtils.escapeLineBreak("a\\z"));
        assertEquals("some text with multi \\nbyte 田中 characters.",
                EscapeUtils.escapeLineBreak("some text with multi \nbyte 田中 characters."));
    }

    @Test
    public void noOpUnEscape() throws Exception{
        assertEquals(null, EscapeUtils.unescapeLineBreaks(null));
        assertEquals("abc", EscapeUtils.unescapeLineBreaks("abc"));
        assertEquals("abc\b", EscapeUtils.unescapeLineBreaks("abc\b"));
        assertEquals("", EscapeUtils.unescapeLineBreaks(""));
        assertEquals("some text with multi byte 田中 characters.",
                EscapeUtils.unescapeLineBreaks("some text with multi byte 田中 characters."));
    }

    @Test
    public void testUnEscape() throws Exception{
        assertEquals("ab\nc\r", EscapeUtils.unescapeLineBreaks("ab\\nc\\r"));
        assertEquals("a\\z", EscapeUtils.unescapeLineBreaks("a\\\\z"));
        assertEquals("some text with multi \nbyte 田中 characters.",
                EscapeUtils.unescapeLineBreaks("some text with multi \\nbyte 田中 characters."));
    }

    @Test
    public void testEscapeUnEscape() throws Exception{
        assertEscape("ab\nc\r");
        assertEscape("a\\z");
        assertEscape("a\\\\z\nc");
        assertEscape("some text with multi \nbyte \r田中 characters\\.");
    }

    @Test(expected = IllegalStateException.class)
    public void invalidUnEscape() throws Exception{
        EscapeUtils.unescapeLineBreaks("abc\\");
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidUnEscape2() throws Exception{
        //Pass an unescaped string. In an escaped string a literal '\'
        // would always be escaped
        EscapeUtils.unescapeLineBreaks("abc\\k\\n");
    }

    @Test
    public void randomized() throws Exception {
        Random r = new Random(1);
        for (int i = 0; i < 100000; i++) {
            int len = r.nextInt(10);
            StringBuilder buff = new StringBuilder();
            for (int j = 0; j < len; j++) {
                switch (r.nextInt(3)) {
                    case 0:
                        String s = "\\\r\nrnRN ";
                        buff.append(s.charAt(r.nextInt(s.length())));
                        break;
                    case 1:
                        buff.append(RandomStringUtils.random(4, true, false));
                        break;
                    case 2:
                        buff.append((char) r.nextInt(65000));
                        break;
                }
            }
            String original = buff.toString();
            String escaped = EscapeUtils.escapeLineBreak(original);
            String unescaped = EscapeUtils.unescapeLineBreaks(escaped);
            assertTrue(escaped.indexOf('\n') < 0);
            assertTrue(escaped.indexOf('\r') < 0);
            assertEquals(original, unescaped);
        }
    }

    private static void assertEscape(String text){
        String result = EscapeUtils.unescapeLineBreaks(EscapeUtils.escapeLineBreak(text));
        assertEquals(text, result);
    }
}
