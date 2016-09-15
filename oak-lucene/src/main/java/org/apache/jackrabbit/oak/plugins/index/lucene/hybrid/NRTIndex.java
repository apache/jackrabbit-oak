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

package org.apache.jackrabbit.oak.plugins.index.lucene.hybrid;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexCopier;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.reader.LuceneIndexReader;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.IndexWriterUtils;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkState;


public class NRTIndex implements Closeable {
    private static final AtomicInteger COUNTER = new AtomicInteger();
    private static final Logger log = LoggerFactory.getLogger(NRTIndex.class);

    /**
     * Prefix used for naming the directory created for NRT indexes
     */
    public static final String NRT_DIR_PREFIX = "nrt-";

    private final IndexDefinition definition;
    private final IndexCopier indexCopier;
    private final LuceneIndexReader previousReader;

    private IndexWriter indexWriter;
    private NRTIndexWriter nrtIndexWriter;
    private File indexDir;
    private Directory directory;
    private boolean closed;

    public NRTIndex(IndexDefinition definition, IndexCopier indexCopier, @Nullable NRTIndex previous) {
        this.definition = definition;
        this.indexCopier = indexCopier;
        this.previousReader = previous != null ? previous.getPrimaryReader() : null;
    }

    @CheckForNull
    LuceneIndexReader getPrimaryReader() {
        return createReader();
    }

    public LuceneIndexWriter getWriter() throws IOException {
        checkState(!closed);
        if (nrtIndexWriter == null) {
            nrtIndexWriter = createWriter();
        }
        return nrtIndexWriter;
    }

    public List<LuceneIndexReader> getReaders() {
        checkState(!closed);
        List<LuceneIndexReader> readers = Lists.newArrayListWithCapacity(2);
        LuceneIndexReader latestReader = createReader();
        if (latestReader != null) {
            readers.add(latestReader);
        }

        //Old reader should be added later
        if (previousReader != null) {
            readers.add(previousReader);
        }
        return readers;
    }

    public void close() throws IOException {
        if (closed) {
            return;
        }

        if (indexWriter != null) {
            indexWriter.close();
            directory.close();
            FileUtils.deleteQuietly(indexDir);
            log.debug("[{}] Removed directory [{}]", this, indexDir);
        }

        closed = true;
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public String toString() {
        return definition.getIndexPathFromConfig();
    }

    //For test
    File getIndexDir() {
        return indexDir;
    }

    @CheckForNull
    private LuceneIndexReader createReader() {
        checkState(!closed);
        //Its possible that readers are obtained
        //before anything gets indexed
        if (indexWriter == null) {
            return null;
        }
        try {
            //applyDeletes is false as layers above would take care of
            //stale result
            return new NRTReader(DirectoryReader.open(indexWriter, false));
        } catch (IOException e) {
            log.warn("Error opening index [{}]", e);
        }
        return null;
    }

    private synchronized NRTIndexWriter createWriter() throws IOException {
        String dirName = generateDirName();
        indexDir = indexCopier.getIndexDir(definition, definition.getIndexPathFromConfig(), dirName);
        Directory fsdir = FSDirectory.open(indexDir);
        //TODO make these configurable
        directory = new NRTCachingDirectory(fsdir, 1, 1);
        IndexWriterConfig config = IndexWriterUtils.getIndexWriterConfig(definition, false);
        indexWriter = new IndexWriter(directory, config);
        return new NRTIndexWriter(indexWriter);
    }

    public static String generateDirName() {
        long uniqueCount = System.currentTimeMillis() + COUNTER.incrementAndGet();
        return NRT_DIR_PREFIX + uniqueCount;
    }

    private static class NRTReader implements LuceneIndexReader {
        private final IndexReader indexReader;

        public NRTReader(IndexReader indexReader) {
            this.indexReader = indexReader;
        }

        @Override
        public IndexReader getReader() {
            return indexReader;
        }

        @Override
        public AnalyzingInfixSuggester getLookup() {
            return null;
        }

        @Override
        public Directory getSuggestDirectory() {
            return null;
        }

        @Override
        public void close() throws IOException {

        }
    }

    private static class NRTIndexWriter implements LuceneIndexWriter {
        private final IndexWriter indexWriter;

        public NRTIndexWriter(IndexWriter indexWriter) {
            this.indexWriter = indexWriter;
        }

        @Override
        public void updateDocument(String path, Iterable<? extends IndexableField> doc) throws IOException {
            //For NRT case documents are never updated
            //instead they are just added. This would cause duplicates
            //That should be taken care at query side via unique cursor
            indexWriter.addDocument(doc);
        }

        @Override
        public void deleteDocuments(String path) throws IOException {
            //Do not delete documents. Query side would handle it
        }

        @Override
        public boolean close(long timestamp) throws IOException {
            throw new IllegalStateException("Close should not be called");
        }
    }
}
