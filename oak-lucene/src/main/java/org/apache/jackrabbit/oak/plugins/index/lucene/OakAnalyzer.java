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

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterFilter;
import org.apache.lucene.analysis.standard.ClassicTokenizer;
import org.apache.lucene.util.Version;

public class OakAnalyzer extends Analyzer {

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

    @Override
    protected TokenStreamComponents createComponents(final String fieldName,
            final Reader reader) {
        ClassicTokenizer src = new ClassicTokenizer(matchVersion, reader);
        TokenStream tok = new LowerCaseFilter(matchVersion, src);
        tok = new WordDelimiterFilter(tok,
                WordDelimiterFilter.GENERATE_WORD_PARTS
                        | WordDelimiterFilter.STEM_ENGLISH_POSSESSIVE
                        | WordDelimiterFilter.GENERATE_NUMBER_PARTS, null);
        return new TokenStreamComponents(src, tok);
    }
}
