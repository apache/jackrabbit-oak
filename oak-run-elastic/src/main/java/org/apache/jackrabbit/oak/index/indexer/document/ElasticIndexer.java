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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticDocument;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticDocumentMaker;
import org.apache.jackrabbit.oak.plugins.index.progress.IndexingProgressReporter;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.binary.FulltextBinaryTextExtractor;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriter;
import org.apache.jackrabbit.oak.spi.filter.PathFilter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

import java.io.IOException;
import java.util.Set;

/*
NodeStateIndexer for Elastic. Indexes entries from a given nodestate.
 */
public class ElasticIndexer implements NodeStateIndexer {

    private final IndexDefinition definition;
    private final FulltextBinaryTextExtractor binaryTextExtractor;
    private final NodeBuilder definitionBuilder;
    private final IndexingProgressReporter progressReporter;
    private final FulltextIndexWriter<ElasticDocument> indexWriter;

    public ElasticIndexer(IndexDefinition definition, FulltextBinaryTextExtractor binaryTextExtractor,
                          NodeBuilder definitionBuilder, IndexingProgressReporter progressReporter,
                          FulltextIndexWriter<ElasticDocument> indexWriter) {
        this.definition = definition;
        this.binaryTextExtractor = binaryTextExtractor;
        this.definitionBuilder = definitionBuilder;
        this.progressReporter = progressReporter;
        this.indexWriter = indexWriter;
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
        ElasticDocumentMaker maker = newDocumentMaker(indexingRule, entry.getPath());

        ElasticDocument doc = maker.makeDocument(entry.getNodeState());

        if (doc != null) {
            writeToIndex(doc, entry.getPath());
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
    public void close() throws IOException {
        indexWriter.close(System.currentTimeMillis());
    }

    private PathFilter.Result getFilterResult(String path) {
        return definition.getPathFilter().filter(path);
    }

    private void writeToIndex(ElasticDocument doc, String path) throws IOException {
        indexWriter.updateDocument(path, doc);
    }

    private ElasticDocumentMaker newDocumentMaker(IndexDefinition.IndexingRule indexingRule, String path) {
        return new ElasticDocumentMaker(binaryTextExtractor, definition,
                indexingRule,
                path);
    }
}
