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

package org.apache.jackrabbit.oak.plugins.index.lucene.writer;

import java.io.IOException;
import java.util.Calendar;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.OakDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.SuggestHelper;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.util.PerfLogger;
import org.apache.jackrabbit.util.ISO8601;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.SUGGEST_DATA_CHILD_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.TermFactory.newPathTerm;
import static org.apache.jackrabbit.oak.plugins.index.lucene.writer.IndexWriterUtils.getIndexWriterConfig;
import static org.apache.jackrabbit.oak.plugins.index.lucene.writer.IndexWriterUtils.newIndexDirectory;

class DefaultIndexWriter implements LuceneIndexWriter {
    private static final Logger log = LoggerFactory.getLogger(DefaultIndexWriter.class);
    private static final PerfLogger PERF_LOGGER =
            new PerfLogger(LoggerFactory.getLogger(LuceneIndexWriter.class.getName() + ".perf"));

    private final IndexDefinition definition;
    private final NodeBuilder definitionBuilder;
    private final IndexCopier indexCopier;
    private final String dirName;
    private final String suggestDirName;
    private final boolean reindex;
    private IndexWriter writer;
    private Directory directory;

    public DefaultIndexWriter(IndexDefinition definition, NodeBuilder definitionBuilder,
                              @Nullable IndexCopier indexCopier, String dirName, String suggestDirName, boolean reindex){
        this.definition = definition;
        this.definitionBuilder = definitionBuilder;
        this.indexCopier = indexCopier;
        this.dirName = dirName;
        this.suggestDirName = suggestDirName;
        this.reindex = reindex;
    }

    @Override
    public void updateDocument(String path, Iterable<? extends IndexableField> doc) throws IOException {
        getWriter().updateDocument(newPathTerm(path), doc);
    }

    @Override
    public void deleteDocuments(String path) throws IOException {
        getWriter().deleteDocuments(newPathTerm(path));
        getWriter().deleteDocuments(new PrefixQuery(newPathTerm(path + "/")));
    }

    void deleteAll() throws IOException {
        getWriter().deleteAll();
    }

    @Override
    public boolean close(long timestamp) throws IOException {
        //If reindex or fresh index and write is null on close
        //it indicates that the index is empty. In such a case trigger
        //creation of write such that an empty Lucene index state is persisted
        //in directory
        boolean indexUpdated = false;
        if (reindex && writer == null){
            getWriter();
        }

        Calendar currentTime = Calendar.getInstance();
        currentTime.setTimeInMillis(timestamp);
        boolean updateSuggestions = shouldUpdateSuggestions(currentTime);
        if (writer == null && updateSuggestions) {
            log.debug("Would update suggester dictionary although no index changes were detected in current cycle");
            getWriter();
        }

        if (writer != null) {
            indexUpdated = true;
            if (log.isTraceEnabled()) {
                trackIndexSizeInfo(writer, definition, directory);
            }

            final long start = PERF_LOGGER.start();

            if (updateSuggestions) {
                updateSuggester(writer.getAnalyzer(), currentTime);
                PERF_LOGGER.end(start, -1, "Completed suggester for directory {}", definition);
            }

            writer.close();
            PERF_LOGGER.end(start, -1, "Closed writer for directory {}", definition);

            directory.close();
            PERF_LOGGER.end(start, -1, "Closed directory for directory {}", definition);
        }
        return indexUpdated;
    }

    //~----------------------------------------< internal >

    private IndexWriter getWriter() throws IOException {
        if (writer == null) {
            final long start = PERF_LOGGER.start();
            directory = newIndexDirectory(definition, definitionBuilder, dirName);
            IndexWriterConfig config;
            if (indexCopier != null){
                directory = indexCopier.wrapForWrite(definition, directory, reindex, dirName);
                config = getIndexWriterConfig(definition, false);
            } else {
                config = getIndexWriterConfig(definition, true);
            }
            writer = new IndexWriter(directory, config);
            PERF_LOGGER.end(start, -1, "Created IndexWriter for directory {}", definition);
        }
        return writer;
    }

    /**
     * eventually update suggest dictionary
     * @throws IOException if suggest dictionary update fails
     * @param analyzer the analyzer used to update the suggester
     */
    private void updateSuggester(Analyzer analyzer, Calendar currentTime) throws IOException {
        NodeBuilder suggesterStatus = definitionBuilder.child(suggestDirName);
        DirectoryReader reader = DirectoryReader.open(writer, false);
        final OakDirectory suggestDirectory = new OakDirectory(definitionBuilder, suggestDirName, definition, false);
        try {
            SuggestHelper.updateSuggester(suggestDirectory, analyzer, reader);
            suggesterStatus.setProperty("lastUpdated", ISO8601.format(currentTime), Type.DATE);
        } catch (Throwable e) {
            log.warn("could not update suggester", e);
        } finally {
            suggestDirectory.close();
            reader.close();
        }
    }

    /**
     * Checks if last suggestion build time was done sufficiently in the past AND that there were non-zero indexedNodes
     * stored in the last run. Note, if index is updated only to rebuild suggestions, even then we update indexedNodes,
     * which would be zero in case it was a forced update of suggestions.
     * @return is suggest dict should be updated
     */
    private boolean shouldUpdateSuggestions(Calendar currentTime) {
        boolean updateSuggestions = false;

        if (definition.isSuggestEnabled()) {
            NodeBuilder suggesterStatus = definitionBuilder.child(suggestDirName);

            PropertyState suggesterLastUpdatedValue = suggesterStatus.getProperty("lastUpdated");

            if (suggesterLastUpdatedValue != null) {
                Calendar suggesterLastUpdatedTime = ISO8601.parse(suggesterLastUpdatedValue.getValue(Type.DATE));

                int updateFrequency = definition.getSuggesterUpdateFrequencyMinutes();
                Calendar nextSuggestUpdateTime = (Calendar)suggesterLastUpdatedTime.clone();
                nextSuggestUpdateTime.add(Calendar.MINUTE, updateFrequency);
                if (currentTime.after(nextSuggestUpdateTime)) {
                    updateSuggestions = (writer != null || isIndexUpdatedAfter(suggesterLastUpdatedTime));
                }
            } else {
                updateSuggestions = true;
            }
        }

        return updateSuggestions;
    }

    /**
     * @return {@code false} if persisted lastUpdated time for index is after {@code calendar}. {@code true} otherwise
     */
    private boolean isIndexUpdatedAfter(Calendar calendar) {
        NodeBuilder indexStats = definitionBuilder.child(":status");
        PropertyState indexLastUpdatedValue = indexStats.getProperty("lastUpdated");
        if (indexLastUpdatedValue != null) {
            Calendar indexLastUpdatedTime = ISO8601.parse(indexLastUpdatedValue.getValue(Type.DATE));
            return indexLastUpdatedTime.after(calendar);
        } else {
            return true;
        }
    }

    private static void trackIndexSizeInfo(@Nonnull IndexWriter writer,
                                           @Nonnull IndexDefinition definition,
                                           @Nonnull Directory directory) throws IOException {
        checkNotNull(writer);
        checkNotNull(definition);
        checkNotNull(directory);

        int docs = writer.numDocs();
        int ram = writer.numRamDocs();

        log.trace("Writer for directory {} - docs: {}, ramDocs: {}", definition, docs, ram);

        String[] files = directory.listAll();
        long overallSize = 0;
        StringBuilder sb = new StringBuilder();
        for (String f : files) {
            sb.append(f).append(":");
            if (directory.fileExists(f)) {
                long size = directory.fileLength(f);
                overallSize += size;
                sb.append(size);
            } else {
                sb.append("--");
            }
            sb.append(", ");
        }
        log.trace("Directory overall size: {}, files: {}",
                org.apache.jackrabbit.oak.commons.IOUtils.humanReadableByteCount(overallSize),
                sb.toString());
    }
}
