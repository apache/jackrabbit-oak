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

package org.apache.jackrabbit.oak.index.indexer.document;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.index.IndexHelper;
import org.apache.jackrabbit.oak.index.IndexerSupport;
import org.apache.jackrabbit.oak.plugins.index.lucene.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.binary.BinaryTextExtractor;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.DirectoryFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.FSDirectoryFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.DefaultIndexWriterFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriter;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriterFactory;
import org.apache.jackrabbit.oak.plugins.index.progress.IndexingProgressReporter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;

public class LuceneIndexerProvider implements NodeStateIndexerProvider {
    private final ExtractedTextCache textCache =
            new ExtractedTextCache(FileUtils.ONE_MB * 5, TimeUnit.HOURS.toSeconds(5));
    private final IndexHelper indexHelper;
    private final DirectoryFactory dirFactory;
    private final LuceneIndexWriterFactory indexWriterFactory;

    public LuceneIndexerProvider(IndexHelper indexHelper, IndexerSupport indexerSupport) throws IOException {
        this.indexHelper = indexHelper;
        this.dirFactory = new FSDirectoryFactory(indexerSupport.getLocalIndexDir());
        this.indexWriterFactory = new DefaultIndexWriterFactory(indexHelper.getMountInfoProvider(), dirFactory);
    }

    @Override
    public NodeStateIndexer getIndexer(@Nonnull String type, @Nonnull String indexPath,
                                       @Nonnull NodeBuilder definition, @Nonnull NodeState root,
                                       IndexingProgressReporter progressReporter) {
        if (!TYPE_LUCENE.equals(definition.getString(TYPE_PROPERTY_NAME))) {
            return null;
        }

        IndexDefinition idxDefinition = IndexDefinition.newBuilder(root, definition.getNodeState(), indexPath).reindex().build();

        LuceneIndexWriter indexWriter = indexWriterFactory.newInstance(idxDefinition, definition, true);
        BinaryTextExtractor textExtractor = new BinaryTextExtractor(textCache, idxDefinition, true);
        return new LuceneIndexer(
                idxDefinition,
                indexWriter,
                definition,
                textExtractor,
                progressReporter
        );
    }

    @Override
    public void close() throws IOException {

    }
}
