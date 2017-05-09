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

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.util.Collections;
import java.util.List;

import javax.annotation.Nullable;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.FieldNames;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.search.spell.LuceneDictionary;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
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

    public static void updateSuggester(Directory directory, Analyzer analyzer, IndexReader reader) throws IOException {
        File tempDir = null;
        try {
            //Analyzing infix suggester takes a file parameter. It uses its path to getDirectory()
            //for actual storage of suggester data. BUT, while building it also does getDirectory() to
            //a temporary location (original path + ".tmp"). So, instead we create a temp dir and also
            //create a placeholder non-existing-sub-child which would mark the location when we want to return
            //our internal suggestion OakDirectory. After build is done, we'd delete the temp directory
            //thereby removing any temp stuff that suggester created in the interim.
            tempDir = Files.createTempDir();
            File tempSubChild = new File(tempDir, "non-existing-sub-child");

            if (reader.getDocCount(FieldNames.SUGGEST) > 0) {
                Dictionary dictionary = new LuceneDictionary(reader, FieldNames.SUGGEST);
                getLookup(directory, analyzer, tempSubChild).build(dictionary);
            }
        } catch (RuntimeException e) {
            log.debug("could not update the suggester", e);
        } finally {
            //cleanup temp dir
            if (tempDir != null && !FileUtils.deleteQuietly(tempDir)) {
                log.error("Cleanup failed for temp dir {}", tempDir.getAbsolutePath());
            }
        }
    }

    public static List<Lookup.LookupResult> getSuggestions(AnalyzingInfixSuggester suggester, @Nullable SuggestQuery suggestQuery) {
        try {
            if (suggester != null && suggester.getCount() > 0) {
                return suggester.lookup(suggestQuery.getText(), 10, true, false);
            } else {
                return Collections.emptyList();
            }
        } catch (Exception e) {
            throw new RuntimeException("could not handle Suggest query " + suggestQuery, e);
        }
    }

    public static SuggestQuery getSuggestQuery(String suggestQueryString) {
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
                return new SuggestQuery(text);
            } else {
                return null;
            }

        } catch (Exception e) {
            throw new RuntimeException("could not build SuggestQuery " + suggestQueryString, e);
        }
    }

    public static AnalyzingInfixSuggester getLookup(final Directory suggestDirectory) throws IOException {
        return getLookup(suggestDirectory, SuggestHelper.analyzer);
    }

    public static AnalyzingInfixSuggester getLookup(final Directory suggestDirectory, Analyzer analyzer) throws IOException {
        return getLookup(suggestDirectory, analyzer, null);
    }
    public static AnalyzingInfixSuggester getLookup(final Directory suggestDirectory, Analyzer analyzer,
                                                    final File tempDir) throws IOException {
        return new AnalyzingInfixSuggester(Version.LUCENE_47, tempDir, analyzer, analyzer, 3) {
            @Override
            protected Directory getDirectory(File path) throws IOException {
                if (tempDir == null || tempDir.getAbsolutePath().equals(path.getAbsolutePath())) {
                    return suggestDirectory; // use oak directory for writing suggest index
                } else {
                    return FSDirectory.open(path); // use FS for temp index used at build time
                }
            }
        };
    }

    public static Analyzer getAnalyzer() {
        return analyzer;
    }

    public static class SuggestQuery {

        private final String text;

        public SuggestQuery(String text) {
            this.text = text;
        }

        public String getText() {
            return text;
        }

        @Override
        public String toString() {
            return "SuggestQuery{" +
                    "text='" + text + '\'' +
                    '}';
        }
    }
}
