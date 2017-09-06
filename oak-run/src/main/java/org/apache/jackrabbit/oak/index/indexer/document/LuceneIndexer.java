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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.index.PathFilter;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexDefinition.IndexingRule;
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneDocumentMaker;
import org.apache.jackrabbit.oak.plugins.index.lucene.binary.BinaryTextExtractor;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.FacetHelper;
import org.apache.jackrabbit.oak.plugins.index.lucene.writer.LuceneIndexWriter;
import org.apache.jackrabbit.oak.plugins.index.progress.IndexingProgressReporter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.lucene.document.Document;

public class LuceneIndexer implements NodeStateIndexer {
    private final IndexDefinition definition;
    private final BinaryTextExtractor binaryTextExtractor;
    private final NodeBuilder definitionBuilder;
    private final LuceneIndexWriter indexWriter;
    private final IndexingProgressReporter progressReporter;

    public LuceneIndexer(IndexDefinition definition, LuceneIndexWriter indexWriter,
                         NodeBuilder builder, BinaryTextExtractor binaryTextExtractor,
                         IndexingProgressReporter progressReporter) {
        this.definition = definition;
        this.binaryTextExtractor = binaryTextExtractor;
        this.indexWriter = indexWriter;
        this.definitionBuilder = builder;
        this.progressReporter = progressReporter;
    }

    @Override
    public boolean shouldInclude(String path) {
        return definition.getPathFilter().filter(path) != PathFilter.Result.EXCLUDE;
    }

    @Override
    public boolean shouldInclude(NodeDocument doc) {
        //TODO possible optimization for NodeType based filtering
        return true;
    }

    @Override
    public void index(NodeStateEntry entry) throws IOException, CommitFailedException {
        IndexingRule indexingRule = definition.getApplicableIndexingRule(entry.getNodeState());

        if (indexingRule == null) {
            return;
        }

        LuceneDocumentMaker maker = newDocumentMaker(indexingRule, entry.getPath());
        Document doc = maker.makeDocument(entry.getNodeState());
        if (doc != null) {
            writeToIndex(doc, entry.getPath());
            progressReporter.indexUpdate(definition.getIndexPath());
        }
    }

    @Override
    public void close() throws IOException {
        indexWriter.close(System.currentTimeMillis());
    }

    private void writeToIndex(Document doc, String path) throws IOException {
        indexWriter.updateDocument(path, doc);
    }

    private LuceneDocumentMaker newDocumentMaker(IndexingRule indexingRule, String path) {
        return new LuceneDocumentMaker(
                binaryTextExtractor,
                () -> FacetHelper.getFacetsConfig(definitionBuilder), //TODO FacetsConfig handling
                null,   //TODO augmentorFactory
                definition,
                indexingRule,
                path
        );
    }
}
