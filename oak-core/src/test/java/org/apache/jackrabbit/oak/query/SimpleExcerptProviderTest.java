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

package org.apache.jackrabbit.oak.query;

import static com.google.common.collect.ImmutableSet.of;
import static org.apache.jackrabbit.oak.query.SimpleExcerptProvider.highlight;
import static org.junit.Assert.assertEquals;

import java.util.Map;
import java.util.Random;

import com.google.common.collect.Maps;
import org.junit.Test;

public class SimpleExcerptProviderTest {

    @Test
    public void simpleTest() throws Exception {
        assertEquals("<div><span><strong>fox</strong> is jumping</span></div>",
                highlight(sb("fox is jumping"), of("fox")));
        assertEquals("<div><span>fox is <strong>jumping</strong></span></div>",
                highlight(sb("fox is jumping"), of("jump*")));

    }

    @Test
    public void highlightWithWildCard() throws Exception {
        assertEquals("<div><span><strong>fox</strong> is jumping</span></div>",
                highlight(sb("fox is jumping"), of("fox *")));
    }

    @Test
    public void highlightIgnoreStar() throws Exception {
        assertEquals("<div><span>10 * 10</span></div>",
                highlight(sb("10 * 10"), of("fox *")));
    }

    @Test
    public void randomized() throws Exception {
        Random r = new Random(1);
        String set = "abc*\'\"<> ";
        for (int i = 0; i < 10000; i++) {
            highlight(sb(randomString(r, set)), of(randomString(r, set)));
        }
    }

    @Test
    public void hightlightCompleteWordOnly() {
        // using 2 non-simple spaces as mentioned in http://jkorpela.fi/chars/spaces.html
        String[] delimiters = new String[] {" ", "\t", "\n", ":", "\u1680", "\u00A0"};
        Map<String, String> simpleCheck = Maps.newHashMap(); // highlight "of"

        // simple ones
        simpleCheck.put("official conflict of interest",
                "<div><span>official conflict <strong>of</strong> interest</span></div>");
        simpleCheck.put("of to new city",
                "<div><span><strong>of</strong> to new city</span></div>");
        simpleCheck.put("out of the roof",
                "<div><span>out <strong>of</strong> the roof</span></div>");
        simpleCheck.put("well this is of",
                "<div><span>well this is <strong>of</strong></span></div>");

        for (Map.Entry<String, String> simple : simpleCheck.entrySet()) {
            for (String delimiter : delimiters) {
                String text = simple.getKey().replaceAll(" ", delimiter);
                String expect = simple.getValue().replaceAll(" ", delimiter);
                assertEquals("highlighting '" + text + "' for 'of' (delimiter - '" + delimiter + "')",
                        expect, highlight(sb(text), of("of")));
            }
        }

        Map<String, String> wildcardCheck = Maps.newHashMap(); // highlight "of*"
        wildcardCheck.put("office room",
                "<div><span><strong>office</strong> room</span></div>");
        wildcardCheck.put("office room off",
                "<div><span><strong>office</strong> room <strong>off</strong></span></div>");
        wildcardCheck.put("big office room",
                "<div><span>big <strong>office</strong> room</span></div>");

        for (Map.Entry<String, String> wildcard : wildcardCheck.entrySet()) {
            for (String delimiter : delimiters) {
                String text = wildcard.getKey().replaceAll(" ", delimiter);
                String expect = wildcard.getValue().replaceAll(" ", delimiter);
                assertEquals("highlighting '" + text + "' for 'of*' (delimiter - '" + delimiter + "')",
                        expect, highlight(sb(text), of("of*")));
            }
        }
    }

    @Test
    public void multipleSearchTokens() {
        String text = "To be, or not to be. That is the question!";
        String expected = "<div><span>To <strong>be</strong>, " +
                "or not to <strong>be</strong>. " +
                "That is the <strong>question</strong>!</span></div>";

        assertEquals(expected, highlight(sb(text), of("question", "be")));
        assertEquals(expected, highlight(sb(text), of("quest*", "be")));
        assertEquals(expected, highlight(sb(text), of("quest*", "b*")));
    }

    private static String randomString(Random r, String set) {
        int len = r.nextInt(10);
        StringBuilder buff = new StringBuilder();
        for (int i = 0; i < len; i++) {
            buff.append(set.charAt(r.nextInt(set.length())));
        }
        return buff.toString();
    }

    private static StringBuilder sb(String text) {
        return new StringBuilder(text);
    }
}