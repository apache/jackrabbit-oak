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
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.commons.pio.Closer;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.spell.Dictionary;
import org.apache.lucene.search.spell.LuceneDictionary;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for getting suggest results for a given term, calling a {@link Lookup}
 * implementation under the hood.
 */
public class SuggestHelper {

    private static final Logger log = LoggerFactory.getLogger(SuggestHelper.class);

    private static final Analyzer analyzer = new Analyzer() {
        /**
         * Creates the TokenStreamComponents for this analyzer.
         * In Lucene 5.x, createComponents no longer takes a Reader parameter.
         */
        @Override
        protected TokenStreamComponents createComponents(String fieldName) {
            CRTokenizer tokenizer = new CRTokenizer();
            return new TokenStreamComponents(tokenizer);
        }
    };

    public static void updateSuggester(Directory directory, Analyzer analyzer, IndexReader reader, final Closer closer)
            throws IOException {
        File tempDir = null;
        boolean shouldCloseDirectory = true;
        try {
            //Analyzing infix suggester takes a file parameter. It uses its path to getDirectory()
            //for actual storage of suggester data. BUT, while building it also does getDirectory() to
            //a temporary location (original path + ".tmp"). So, instead we create a temp dir and also
            //create a placeholder non-existing-sub-child which would mark the location when we want to return
            //our internal suggestion OakDirectory. After build is done, we'd delete the temp directory
            //thereby removing any temp stuff that suggester created in the interim.
            tempDir = Files.createTempDirectory(SuggestHelper.class.getSimpleName() + "-").toFile();
            File tempSubChild = new File(tempDir, "non-existing-sub-child");

            int suggestDocCount = reader.getDocCount(FieldNames.SUGGEST);
            log.debug("updateSuggester: reader.getDocCount(SUGGEST) = {}", suggestDocCount);
            if (suggestDocCount > 0) {
                Dictionary dictionary = new LuceneDictionary(reader, FieldNames.SUGGEST);
                AnalyzingInfixSuggester suggester = closer.register(getLookup(directory, analyzer, tempSubChild));
                shouldCloseDirectory = false;
                suggester.build(dictionary);
                log.debug("updateSuggester: suggester.build() completed, getCount() = {}", suggester.getCount());
                // In Lucene 5.x (LUCENE-5889), commit() must be called after build()
                // to make the suggestions visible for lookups
                suggester.commit();
                log.debug("updateSuggester: suggester.commit() completed");
            } else {
                log.debug("updateSuggester: skipping suggester build because no SUGGEST documents found");
            }
        } catch (RuntimeException e) {
            log.debug("could not update the suggester", e);
        } finally {
            if (shouldCloseDirectory) {
                closer.register(directory);
            }
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
        // Log the directory contents for debugging
        if (log.isDebugEnabled()) {
            try {
                String[] files = suggestDirectory.listAll();
                log.debug("Suggester directory contains {} files: {}", files.length,
                    files.length > 0 ? String.join(", ", files) : "(empty)");
                // Check if index exists
                boolean indexExists = org.apache.lucene.index.DirectoryReader.indexExists(suggestDirectory);
                log.debug("DirectoryReader.indexExists() returns: {}", indexExists);
            } catch (IOException e) {
                log.debug("Could not list suggester directory contents", e);
            }
        }

        AnalyzingInfixSuggester suggester = new AnalyzingInfixSuggester(suggestDirectory, analyzer, analyzer, 3, false) {
            @Override
            protected Directory getDirectory(java.nio.file.Path path) throws IOException {
                if (tempDir == null || tempDir.toPath().equals(path)) {
                    return suggestDirectory; // use oak directory for writing suggest index
                } else {
                    // In Lucene 5.x, FSDirectory.open() takes a Path instead of File
                    return FSDirectory.open(path);
                }
            }
        };

        // Log the suggester state after construction
        if (log.isDebugEnabled()) {
            try {
                log.debug("Suggester created, getCount() = {}", suggester.getCount());
            } catch (IOException e) {
                log.debug("Could not get suggester count", e);
            }
        }

        // In Lucene 5.x, when opening an existing suggester index, we need to call refresh()
        // to initialize the SearcherManager and make the committed data visible for lookups.
        // This is required because we use commitOnBuild=false and call commit() explicitly.
        // Note: refresh() is only needed if the suggester was built but searcherMgr wasn't
        // initialized in the constructor (which happens when DirectoryReader.indexExists() returns true).
        // If getCount() > 0, the searcherMgr was already initialized and refresh() is not needed.
        try {
            if (suggester.getCount() == 0) {
                // Try refresh in case the index exists but searcherMgr wasn't initialized
                suggester.refresh();
                log.debug("Suggester refreshed, new getCount() = {}", suggester.getCount());
            }
        } catch (IllegalStateException e) {
            // refresh() throws IllegalStateException if the suggester has never been built
            // (i.e., the directory is empty). This is expected for new indexes.
            log.debug("Suggester refresh skipped - index may be empty or not yet built", e);
        }
        return suggester;
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
