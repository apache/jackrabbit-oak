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

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.index.IndexHelper;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexTracker;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticMetricHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticDocument;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexWriterFactory;
import org.apache.jackrabbit.oak.plugins.index.progress.IndexingProgressReporter;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.plugins.index.search.spi.binary.FulltextBinaryTextExtractor;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriter;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.jetbrains.annotations.NotNull;

import org.jetbrains.annotations.Nullable;

import java.util.concurrent.TimeUnit;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;

public class ElasticIndexerProvider implements NodeStateIndexerProvider {
    private final ExtractedTextCache textCache =
            new ExtractedTextCache(FileUtils.ONE_MB * 5, TimeUnit.HOURS.toSeconds(5));
    private final IndexHelper indexHelper;
    private final ElasticIndexWriterFactory indexWriterFactory;
    private final ElasticConnection connection;

    public ElasticIndexerProvider(IndexHelper indexHelper, ElasticConnection connection) {
        this.indexHelper = indexHelper;
        this.indexWriterFactory = new ElasticIndexWriterFactory(connection,
                new ElasticIndexTracker(connection, new ElasticMetricHandler(StatisticsProvider.NOOP)));
        this.connection = connection;
    }


    @Override
    public @Nullable NodeStateIndexer getIndexer(@NotNull String type, @NotNull String indexPath, @NotNull NodeBuilder definition, @NotNull NodeState root, IndexingProgressReporter progressReporter) {
        if (!ElasticIndexDefinition.TYPE_ELASTICSEARCH.equals(definition.getString(TYPE_PROPERTY_NAME))) {
            return null;
        }
        ElasticIndexDefinition idxDefinition = (ElasticIndexDefinition) new ElasticIndexDefinition.Builder(connection.getIndexPrefix()).
                root(root).indexPath(indexPath).defn(definition.getNodeState()).reindex().build();

        FulltextIndexWriter<ElasticDocument> indexWriter = indexWriterFactory.newInstance(idxDefinition, definition, CommitInfo.EMPTY, true);
        FulltextBinaryTextExtractor textExtractor = new FulltextBinaryTextExtractor(textCache, idxDefinition, true);

        ElasticIndexTracker indexTracker = new ElasticIndexTracker(connection, new ElasticMetricHandler(StatisticsProvider.NOOP));
        ElasticIndexEditorProvider elasticIndexEditorProvider = new ElasticIndexEditorProvider(indexTracker, connection, null);
        return new ElasticIndexer(idxDefinition, textExtractor, definition, progressReporter, indexWriter, elasticIndexEditorProvider, indexHelper);
    }

    @Override
    public void close() {}
}
