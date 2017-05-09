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
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.util.Version;

public class OakAnalyzer extends Analyzer {

    private final Version matchVersion;

    private final int INDEX_ORIGINAL_TERM;

    /**
     * Creates a new {@link OakAnalyzer}
     * 
     * @param matchVersion
     *            Lucene version to match See
     *            {@link #matchVersion above}
     */
    public OakAnalyzer(Version matchVersion) {
        this(matchVersion, false);
    }

    /**
     * Create a new {@link OakAnalyzer} with configurable flag to preserve
     * original term being analyzed too.
     * @param matchVersion Lucene version to match See {@link #matchVersion above}
     * @param indexOriginalTerm flag to setup analyzer such that
     *                              {@link WordDelimiterFilter#PRESERVE_ORIGINAL}
     *                              is set to oonfigure word delimeter
     */
    public OakAnalyzer(Version matchVersion, boolean indexOriginalTerm) {
        this.matchVersion = matchVersion;
        INDEX_ORIGINAL_TERM = indexOriginalTerm?WordDelimiterFilter.PRESERVE_ORIGINAL:0;
    }

    @Override
    protected TokenStreamComponents createComponents(final String fieldName,
            final Reader reader) {
        StandardTokenizer src = new StandardTokenizer(matchVersion, reader);
        TokenStream tok = new LowerCaseFilter(matchVersion, src);
        tok = new WordDelimiterFilter(tok,
                WordDelimiterFilter.GENERATE_WORD_PARTS
                        | WordDelimiterFilter.STEM_ENGLISH_POSSESSIVE
                        | this.INDEX_ORIGINAL_TERM
                        | WordDelimiterFilter.GENERATE_NUMBER_PARTS, null);
        return new TokenStreamComponents(src, tok);
    }
}
