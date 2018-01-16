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
        String[] whitespaces = new String[] {" ", "\t"};
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
            String text = simple.getKey();
            String expect = simple.getValue();
            for (String whitespace : whitespaces) {
                text = text.replaceAll(" ", whitespace);
                expect = expect.replaceAll(" ", whitespace);
                assertEquals("highlighting '" + text + "' for 'of' (whitespace - '" + whitespace + "')",
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
            String text = wildcard.getKey();
            String expect = wildcard.getValue();
            for (String whitespace : whitespaces) {
                text = text.replaceAll(" ", whitespace);
                expect = expect.replaceAll(" ", whitespace);
                assertEquals("highlighting '" + text + "' for 'of*' (whitespace - '" + whitespace + "')",
                        expect, highlight(sb(text), of("of*")));
            }
        }
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