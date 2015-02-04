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
import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.suggest.DocumentDictionary;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.analyzing.FreeTextSuggester;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for getting suggest results for a given term, calling a {@link org.apache.lucene.search.suggest.Lookup}
 * implementation under the hood.
 */
public class SuggestHelper {

    private static final Logger log = LoggerFactory.getLogger(SuggestHelper.class);

    private static final Analyzer analyzer = new Analyzer() {
        @Override
        protected Analyzer.TokenStreamComponents createComponents(String fieldName, Reader reader) {
            return new Analyzer.TokenStreamComponents(new CRTokenizer(Version.LUCENE_47, reader));
        }
    };

    public static List<Lookup.LookupResult> getSuggestions(SuggestQuery suggestQuery) {
        FreeTextSuggester suggester = new FreeTextSuggester(analyzer);
        try {
            DocumentDictionary dictionary = suggestQuery.getDictionary();
            suggester.build(dictionary); // TODO : it should be possible to avoid rebuilding the index every time
        } catch (Exception e) {
            log.warn("could not build suggester from the passed dictionary ", e);
        }
        try {
            long count = suggester.getCount();
            if (count > 0) {
                return suggester.lookup(suggestQuery.getText(), false, 10);
            } else {
                return Collections.emptyList();
            }
        } catch (Exception e) {
            throw new RuntimeException("could not handle Suggest query " + suggestQuery, e);
        }
    }

    public static SuggestQuery getSuggestQuery(String suggestQueryString, IndexReader reader) {
        try {
            String text = null;
            for (String param : suggestQueryString.split("&")) {
                String[] keyValuePair = param.split("=");
                if (keyValuePair.length != 2 || keyValuePair[0] == null || keyValuePair[1] == null) {
                    throw new RuntimeException("Unparsable native Lucene Suggest query: " + suggestQueryString);
                } else {
                    if ("term".equals(keyValuePair[0])) {
                        text = keyValuePair[1];
                    }
                }
            }
            if (text != null) {
                return new SuggestQuery(new DocumentDictionary(reader, FieldNames.SUGGEST, FieldNames.PATH_DEPTH), text, analyzer);
            } else {
                return null;
            }

        } catch (Exception e) {
            throw new RuntimeException("could not build SuggestQuery " + suggestQueryString, e);
        }
    }

    public static class SuggestQuery {

        private final DocumentDictionary dictionary;
        private final String text;
        private final Analyzer analyzer;

        public SuggestQuery(DocumentDictionary dictionary, String text, Analyzer analyzer) {
            this.dictionary = dictionary;
            this.text = text;
            this.analyzer = analyzer;
        }

        public DocumentDictionary getDictionary() {
            return dictionary;
        }

        public String getText() {
            return text;
        }

        public Analyzer getAnalyzer() {
            return analyzer;
        }

        @Override
        public String toString() {
            return "SuggestQuery{" +
                    "dictionary=" + dictionary +
                    ", text='" + text + '\'' +
                    '}';
        }
    }
}
