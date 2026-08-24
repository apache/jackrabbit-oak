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
package org.apache.jackrabbit.oak.plugins.index.luceneNg.internal.editor;

import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriter;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.PrefixQuery;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Adapts the raw Lucene {@link IndexWriter} this module already opens per commit into the
 * {@link FulltextIndexWriter} shape {@code FulltextIndexEditor} expects, so the editor no
 * longer manages the writer's lifecycle itself.
 *
 * <p>Mirrors the exact update/delete/commit/close sequence that
 * {@code LuceneNgIndexEditor} previously performed directly on its own {@link IndexWriter}
 * field.</p>
 */
public class LuceneNgFulltextIndexWriter implements FulltextIndexWriter<Document> {

    private static final Logger LOG = LoggerFactory.getLogger(LuceneNgFulltextIndexWriter.class);

    private final IndexWriter indexWriter;

    /**
     * Tracks whether any write (update or delete) happened through this instance, so
     * {@link #close(long)} can honour its documented contract of returning {@code true} only
     * "if index was updated or any write happened" — mirroring the {@code indexUpdated} field
     * in {@code oak-lucene}'s {@code DefaultIndexWriter}.
     */
    private boolean indexUpdated = false;

    public LuceneNgFulltextIndexWriter(@NotNull IndexWriter indexWriter) {
        this.indexWriter = indexWriter;
    }

    @Override
    public void updateDocument(String path, Document doc) throws IOException {
        indexWriter.updateDocument(new Term(FieldNames.PATH, path), doc);
        indexUpdated = true;
    }

    @Override
    public void deleteDocumentTree(String path) throws IOException {
        indexWriter.deleteDocuments(new Term(FieldNames.PATH, path));
        indexWriter.deleteDocuments(new PrefixQuery(new Term(FieldNames.PATH, path + "/")));
        indexUpdated = true;
    }

    @Override
    public void deleteDocument(String path) throws IOException {
        indexWriter.deleteDocuments(new Term(FieldNames.PATH, path));
        indexUpdated = true;
    }

    @Override
    public boolean close(long timestamp) throws IOException {
        try {
            indexWriter.commit();
            LOG.debug("Committed Lucene 9 index");
            return indexUpdated;
        } finally {
            indexWriter.close();
        }
    }
}
