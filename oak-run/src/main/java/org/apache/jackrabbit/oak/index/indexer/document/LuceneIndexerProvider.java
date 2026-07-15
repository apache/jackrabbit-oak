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
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.index.ExtendedIndexHelper;
import org.apache.jackrabbit.oak.index.IndexerSupport;
import org.apache.jackrabbit.oak.plugins.index.ConfigHelper;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.DirectoryFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.FSDirectoryFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.DefaultIndexWriterFactory;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.IndexWriterPool;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriter;
import org.apache.jackrabbit.oak.plugins.index.progress.IndexingProgressReporter;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.spi.binary.FulltextBinaryTextExtractor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants.TYPE_LUCENE;

public class LuceneIndexerProvider implements NodeStateIndexerProvider {

    public static final String OAK_INDEXER_DOCUMENT_PARALLEL_WRITER_ENABLED = "oak.indexer.document.parallelWriter.enabled";

    private final ExtractedTextCache textCache =
            new ExtractedTextCache(FileUtils.ONE_MB * 5, TimeUnit.HOURS.toSeconds(5));
    private final DefaultIndexWriterFactory indexWriterFactory;
    private final ArrayList<LuceneIndexer> indexWriters = new ArrayList<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final IndexWriterPool indexWriterPool;

    public LuceneIndexerProvider(ExtendedIndexHelper extendedIndexHelper, IndexerSupport indexerSupport) throws IOException {
        DirectoryFactory dirFactory = new FSDirectoryFactory(indexerSupport.getLocalIndexDir());
        boolean parallelIndexingEnabled = ConfigHelper.getSystemPropertyAsBoolean(
                OAK_INDEXER_DOCUMENT_PARALLEL_WRITER_ENABLED, false);
        this.indexWriterPool = parallelIndexingEnabled? new IndexWriterPool() : null;
        this.indexWriterFactory = new DefaultIndexWriterFactory(
                extendedIndexHelper.getMountInfoProvider(),
                dirFactory,
                extendedIndexHelper.getLuceneIndexHelper().getWriterConfigForReindex(),
                indexWriterPool);
    }

    @Override
    public NodeStateIndexer getIndexer(@NotNull String type, @NotNull String indexPath,
                                       @NotNull NodeBuilder definition, @NotNull NodeState root,
                                       IndexingProgressReporter progressReporter) {
        if (!TYPE_LUCENE.equals(definition.getString(TYPE_PROPERTY_NAME))) {
            return null;
        }

        LuceneIndexDefinition idxDefinition = LuceneIndexDefinition.newLuceneBuilder(root, definition.getNodeState(), indexPath)
                .reindex()
                .build();

        LuceneIndexWriter indexWriter = indexWriterFactory.newInstance(idxDefinition, definition, null, true);
        FulltextBinaryTextExtractor textExtractor = new FulltextBinaryTextExtractor(textCache, idxDefinition, true);
        LuceneIndexer indexer = new LuceneIndexer(
                idxDefinition,
                indexWriter,
                definition,
                textExtractor,
                progressReporter
        );
        indexWriters.add(indexer);
        return indexer;
    }

    @Override
    public ExtractedTextCache getTextCache() {
        return textCache;
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            for (LuceneIndexer indexer : indexWriters) {
                indexer.close();
            }
            indexWriterFactory.close();
            if (indexWriterPool != null) {
                indexWriterPool.close();
            }
        }
    }
}
