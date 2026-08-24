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

import org.apache.jackrabbit.oak.plugins.index.luceneNg.LuceneNgIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.LuceneNgIndexStorage;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.directory.OakDirectory;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriter;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriterFactory;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Opens the same {@link OakDirectory}-backed Lucene {@link IndexWriter} that
 * {@code LuceneNgIndexEditor}'s constructor previously opened directly, wrapped behind the
 * {@link FulltextIndexWriterFactory} shape {@code FulltextIndexEditorContext} expects.
 *
 * <p>{@link #newInstance} does not declare a checked exception (per the
 * {@link FulltextIndexWriterFactory} interface), so any {@link IOException} raised while
 * opening the directory or writer is wrapped in an {@link UncheckedIOException}.</p>
 */
public class LuceneNgFulltextIndexWriterFactory implements FulltextIndexWriterFactory<Document> {

    private static final Logger LOG = LoggerFactory.getLogger(LuceneNgFulltextIndexWriterFactory.class);

    @Override
    public FulltextIndexWriter<Document> newInstance(IndexDefinition definition, NodeBuilder definitionBuilder,
                                                       CommitInfo commitInfo, boolean reindex) {
        LuceneNgIndexDefinition luceneNgDefinition = (LuceneNgIndexDefinition) definition;
        String indexName = luceneNgDefinition.getIndexName();
        NodeBuilder storage = LuceneNgIndexStorage.getOrCreateStorageBuilder(definitionBuilder);

        try {
            OakDirectory directory = new OakDirectory(storage, indexName, false);
            IndexWriterConfig config = new IndexWriterConfig();
            if (reindex) {
                config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
                LOG.debug("Reindexing: wiping existing index data for {}", luceneNgDefinition.getIndexPath());
            }
            IndexWriter indexWriter;
            try {
                indexWriter = new IndexWriter(directory, config);
            } catch (IOException e) {
                directory.close();
                throw e;
            }
            return new LuceneNgFulltextIndexWriter(indexWriter);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
