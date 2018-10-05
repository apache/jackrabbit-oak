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
package org.apache.jackrabbit.oak.plugins.index.lucene.util.fv;

import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link TruncateTokenFilter}
 */
public class TruncateTokenFilterTest {

    @Test
    public void testFiltering() throws Exception {
        TokenStream stream = new WhitespaceTokenizer(Version.LUCENE_47, new StringReader("0.10 0.20 0.30 0.40"));
        TruncateTokenFilter filter = new TruncateTokenFilter(stream, 3);
        filter.reset();
        List<String> expectedTokens = new LinkedList<>();
        expectedTokens.add("0.1");
        expectedTokens.add("0.2");
        expectedTokens.add("0.3");
        expectedTokens.add("0.4");
        int i = 0;
        while (filter.incrementToken()) {
            CharTermAttribute charTermAttribute = filter.getAttribute(CharTermAttribute.class);
            String token = new String(charTermAttribute.buffer(), 0, charTermAttribute.length());
            assertEquals(expectedTokens.get(i), token);
            i++;
        }
        filter.close();
    }

}