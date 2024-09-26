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
import java.util.Set;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneDocumentMaker;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.FacetHelper;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.FacetsConfigProvider;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriter;
import org.apache.jackrabbit.oak.plugins.index.progress.IndexingProgressReporter;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.binary.FulltextBinaryTextExtractor;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LuceneIndexer implements NodeStateIndexer, FacetsConfigProvider {
    private static final Logger LOG = LoggerFactory.getLogger(LuceneIndexer.class);

    private final IndexDefinition definition;
    private final FulltextBinaryTextExtractor binaryTextExtractor;
    private final NodeBuilder definitionBuilder;
    private final LuceneIndexWriter indexWriter;
    private final IndexingProgressReporter progressReporter;
    private FacetsConfig facetsConfig;

    private final IndexerStatisticsTracker indexerStatisticsTracker = new IndexerStatisticsTracker(LOG);

    public LuceneIndexer(IndexDefinition definition, LuceneIndexWriter indexWriter,
                         NodeBuilder builder, FulltextBinaryTextExtractor binaryTextExtractor,
                         IndexingProgressReporter progressReporter) {
        this.definition = definition;
        this.binaryTextExtractor = binaryTextExtractor;
        this.indexWriter = indexWriter;
        this.definitionBuilder = builder;
        this.progressReporter = progressReporter;
    }

    @Override
    public void onIndexingStarting() {
        indexerStatisticsTracker.onIndexingStarting();
        binaryTextExtractor.resetStartTime();
    }

    @Override
    public boolean shouldInclude(String path) {
        return getFilterResult(path) != PathFilter.Result.EXCLUDE;
    }

    @Override
    public boolean shouldInclude(NodeDocument doc) {
        //TODO possible optimization for NodeType based filtering
        return true;
    }

    @Override
    public boolean index(NodeStateEntry entry) throws IOException, CommitFailedException {
        if (getFilterResult(entry.getPath()) != PathFilter.Result.INCLUDE) {
            return false;
        }

        IndexDefinition.IndexingRule indexingRule = definition.getApplicableIndexingRule(entry.getNodeState());

        if (indexingRule == null) {
            return false;
        }

        long startEntryNanos = System.nanoTime();
        LuceneDocumentMaker maker = newDocumentMaker(indexingRule, entry.getPath());
        Document doc = maker.makeDocument(entry.getNodeState());
        long endEntryMakeDocumentNanos = System.nanoTime();

        if (doc != null) {
            writeToIndex(doc, entry.getPath());
            indexerStatisticsTracker.onEntryEnd(entry.getPath(), startEntryNanos, endEntryMakeDocumentNanos);
            progressReporter.indexUpdate(definition.getIndexPath());
            return true;
        }

        return false;
    }

    @Override
    public boolean indexesRelativeNodes() {
        return definition.indexesRelativeNodes();
    }

    @Override
    public Set<String> getRelativeIndexedNodeNames() {
        return definition.getRelativeNodeNames();
    }

    @Override
    public String getIndexName() {
        return definition.getIndexName();
    }

    @Override
    public void close() throws IOException {
        LOG.info("[{}] Statistics: {}", definition.getIndexName(), indexerStatisticsTracker.formatStats());
        binaryTextExtractor.logStats();
        indexWriter.close(System.currentTimeMillis());
    }

    private PathFilter.Result getFilterResult(String path) {
        return definition.getPathFilter().filter(path);
    }

    private void writeToIndex(Document doc, String path) throws IOException {
        indexWriter.updateDocument(path, doc);
    }

    private LuceneDocumentMaker newDocumentMaker(IndexDefinition.IndexingRule indexingRule, String path) {
        return new LuceneDocumentMaker(
                binaryTextExtractor,
                // we re-use the facet config
                this,
                // augmentorFactory is not supported (it is deprecated)
                null,
                definition,
                indexingRule,
                path
        );
    }

    @Override
    public FacetsConfig getFacetsConfig() {
        if (facetsConfig == null) {
            facetsConfig = FacetHelper.getFacetsConfig(definitionBuilder);
        }
        return facetsConfig;
    }

}
