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

package org.apache.jackrabbit.oak.plugins.tika;

import com.google.common.base.Stopwatch;
import com.google.common.io.Closer;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.jackrabbit.oak.plugins.blob.datastore.TextWriter;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

import static com.google.common.base.Charsets.UTF_8;
import static org.apache.jackrabbit.JcrConstants.JCR_PATH;
import static org.apache.jackrabbit.oak.plugins.index.search.FieldNames.FULLTEXT;
import static org.apache.jackrabbit.oak.plugins.index.search.FieldNames.PATH;
import static org.apache.jackrabbit.oak.plugins.tika.CSVFileBinaryResourceProvider.FORMAT;

class TextPopulator {
    private static final Logger log = LoggerFactory.getLogger(TextPopulator.class);

    static final String BLOB_ID = "blobId";
    static final String ERROR_TEXT = "TextExtractionError";

    private final TextWriter textWriter;

    private PopulatorStats stats;

    TextPopulator(TextWriter textWriter) {
        this.textWriter = textWriter;
        this.stats = new PopulatorStats();
    }

    // exposed for test purposes only
    void setStats(PopulatorStats stats) {
        this.stats = stats;
    }

    void populate(File dataFile, File indexDir) throws IOException {
        try (Closer closer = Closer.create()) {
            Iterable<CSVRecord> csvRecords = closer.register(CSVParser.parse(dataFile, UTF_8, FORMAT));

            final FSDirectory dir = closer.register(FSDirectory.open(indexDir));
            final DirectoryReader reader = closer.register(DirectoryReader.open(dir));
            final IndexSearcher searcher = new IndexSearcher(reader);

            for (CSVRecord record : csvRecords) {
                String blobId = record.get(BLOB_ID);
                String jcrPath = record.get(JCR_PATH);

                if (!textWriter.isProcessed(blobId)) {
                    String text = getText(reader, searcher, jcrPath);

                    stats.processed++;

                    if (text == null) {
                        // Ignore errors as we might be processing partial OR incorrect index
                        // writer.markError(blobId);
                        stats.errored++;
                    } else if (ERROR_TEXT.equals(text)) {
                        textWriter.markError(blobId);
                        stats.errored++;
                    } else if (text.length() == 0) {
                        textWriter.markEmpty(blobId);
                        stats.empty++;
                    } else {
                        textWriter.write(blobId, text);
                        stats.parsed++;
                    }
                } else {
                    stats.ignored++;
                }

                stats.readAndDumpStatsIfRequired(jcrPath);
            }
            log.info(stats.toString());
        }
    }

    private static String getText(DirectoryReader reader, IndexSearcher searcher, String path) {
        TopDocs topDocs;
        try {
            topDocs = searcher.search(new TermQuery(new Term(PATH, path)), 1);

            ScoreDoc[] scoreDocs = topDocs.scoreDocs;
            if (scoreDocs.length != 1) {
                return null;
            }

            Document doc = reader.document(scoreDocs[0].doc);

            String[] ftVals = doc.getValues(FULLTEXT);
            if (ftVals.length != 1) {
                // being conservative... expecting only one stored fulltext field
                return null;
            }

            return ftVals[0].trim();
        } catch (IOException e) {
            // ignore
        }

        return null;
    }

    static class PopulatorStats {
        int read = 0;
        int ignored = 0;
        int processed = 0;
        int parsed = 0;
        int errored = 0;
        int empty = 0;

        Stopwatch w = Stopwatch.createStarted();

        void readAndDumpStatsIfRequired(String path) {
            read++;

            if (read%10000 == 0) {
                log.info("{} - currently at {}", this.toString(), path);
            }
        }

        @Override
        public String toString () {
            return String.format("Text populator stats - " +
                            "Read: %s; Ignored: %s; Processed: %s; Parsed: %s; Errored: %s; Empty: %s (in %s)",
                    read, ignored, processed, parsed, errored, empty, w);
        }
    }
}
