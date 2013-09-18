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
package org.apache.jackrabbit.oak.plugins.index.lucene;

import java.io.IOException;
import java.io.Reader;

import org.apache.jackrabbit.oak.plugins.index.lucene.util.OakWordTokenFilter;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.Version;

public class OakAnalyzer extends Analyzer {

    /** Default maximum allowed token length */
    public static final int DEFAULT_MAX_TOKEN_LENGTH = 255;

    private int maxTokenLength = DEFAULT_MAX_TOKEN_LENGTH;

    private final Version matchVersion;

    /**
     * Creates a new {@link OakAnalyzer}
     * 
     * @param matchVersion
     *            Lucene version to match See
     *            {@link <a href="#version">above</a>}
     */
    public OakAnalyzer(Version matchVersion) {
        this.matchVersion = matchVersion;
    }

    /**
     * Set maximum allowed token length. If a token is seen that exceeds this
     * length then it is discarded. This setting only takes effect the next time
     * tokenStream or tokenStream is called.
     */
    public void setMaxTokenLength(int length) {
        maxTokenLength = length;
    }

    /**
     * @see #setMaxTokenLength
     */
    public int getMaxTokenLength() {
        return maxTokenLength;
    }

    @Override
    protected TokenStreamComponents createComponents(final String fieldName,
            final Reader reader) {
        final StandardTokenizer src = new StandardTokenizer(matchVersion, reader);
        src.setMaxTokenLength(maxTokenLength);
        TokenStream tok = new StandardFilter(matchVersion, src);
        tok = new LowerCaseFilter(matchVersion, tok);
        tok = new OakWordTokenFilter(matchVersion, tok);
        return new TokenStreamComponents(src, tok) {
            @Override
            protected void setReader(final Reader reader) throws IOException {
                src.setMaxTokenLength(OakAnalyzer.this.maxTokenLength);
                super.setReader(reader);
            }
        };
    }
}
