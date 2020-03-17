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

package org.apache.jackrabbit.oak.plugins.index.lucene.util;

import java.io.Reader;
import java.util.Arrays;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.CharFilterFactory;
import org.apache.lucene.analysis.util.TokenFilterFactory;
import org.apache.lucene.analysis.util.TokenizerFactory;

/**
 * An analyzer that uses a tokenizer and a list of token filters to
 * create a TokenStream. Taken from org.apache.solr.analysis.TokenizerChain
 */
public final class TokenizerChain extends Analyzer {
    private final CharFilterFactory[] charFilters;
    private final TokenizerFactory tokenizer;
    private final TokenFilterFactory[] filters;

    public TokenizerChain(TokenizerFactory tokenizer) {
        this(null, tokenizer, null);
    }

    public TokenizerChain(TokenizerFactory tokenizer, TokenFilterFactory[] filters) {
        this(null, tokenizer, filters);
    }

    public TokenizerChain(CharFilterFactory[] charFilters, TokenizerFactory tokenizer, TokenFilterFactory[] filters) {
        this.charFilters = charFilters;
        this.tokenizer = tokenizer;
        this.filters = filters == null ? new TokenFilterFactory[0] : filters;
    }

    @Override
    public Reader initReader(String fieldName, Reader reader) {
        if (charFilters != null && charFilters.length > 0) {
            Reader cs = reader;
            for (CharFilterFactory charFilter : charFilters) {
                cs = charFilter.create(cs);
            }
            reader = cs;
        }
        return reader;
    }

    //Mostly required for testing purpose

    public CharFilterFactory[] getCharFilters() {
        return Arrays.copyOf(charFilters, charFilters.length);
    }

    public TokenizerFactory getTokenizer() {
        return tokenizer;
    }

    public TokenFilterFactory[] getFilters() {
        return Arrays.copyOf(filters, filters.length);
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
        Tokenizer tk = tokenizer.create(reader);
        TokenStream ts = tk;
        for (TokenFilterFactory filter : filters) {
            ts = filter.create(ts);
        }
        return new TokenStreamComponents(tk, ts);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("TokenizerChain(");
        for (CharFilterFactory filter : charFilters) {
            sb.append(filter);
            sb.append(", ");
        }
        sb.append(tokenizer);
        for (TokenFilterFactory filter : filters) {
            sb.append(", ");
            sb.append(filter);
        }
        sb.append(')');
        return sb.toString();
    }

}
