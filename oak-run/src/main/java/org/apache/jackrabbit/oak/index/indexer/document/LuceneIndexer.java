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
import org.apache.jackrabbit.oak.plugins.index.FormattingUtils;
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
    private static final int SLOW_DOCUMENT_LOG_THRESHOLD = Integer.getInteger("oak.lucene.slowDocumentLogThreshold", 1000);

    private final IndexDefinition definition;
    private final FulltextBinaryTextExtractor binaryTextExtractor;
    private final NodeBuilder definitionBuilder;
    private final LuceneIndexWriter indexWriter;
    private final IndexingProgressReporter progressReporter;
    private FacetsConfig facetsConfig;

    private long startTimeNanos = 0;
    private long totalTimeIndexingNanos = 0;
    private long totalTimeMakingDocumentsNanos = 0;
    private long totalTmeWritingToIndexNanos = 0;

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
        this.startTimeNanos = System.nanoTime();
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

        long startNanos = System.nanoTime();
        LuceneDocumentMaker maker = newDocumentMaker(indexingRule, entry.getPath());
        Document doc = maker.makeDocument(entry.getNodeState());
        long endMakeDocumentNanos = System.nanoTime();
        if (doc != null) {
            writeToIndex(doc, entry.getPath());
            long endWriteNanos = System.nanoTime();

            long totalTimeNanos = endWriteNanos - startNanos;
            long makeDocumentTimeNanos = endMakeDocumentNanos - startNanos;
            long writeTimeNanos = endWriteNanos - endMakeDocumentNanos;
            totalTimeIndexingNanos += totalTimeNanos;
            totalTimeMakingDocumentsNanos += makeDocumentTimeNanos;
            totalTmeWritingToIndexNanos += writeTimeNanos;
            if (totalTimeNanos >= (long)SLOW_DOCUMENT_LOG_THRESHOLD * 1_000_000) {
                LOG.info("Slow document: {}. Times: total={}ms, makeDocument={}ms, writeToIndex={}ms",
                        entry.getPath(), totalTimeNanos / 1_000_000, makeDocumentTimeNanos / 1_000_000, writeTimeNanos / 1_000_000);
            }

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
        LOG.info("Statistics: {}", formatStats());
        binaryTextExtractor.logStats();
        indexWriter.close(System.currentTimeMillis());
    }

    public String formatStats() {
        long endTimeNanos = System.nanoTime();
        long totalTimeNanos = endTimeNanos - startTimeNanos;
        long nodesIndexed = progressReporter.getTotalUpdatesCount();
        long avgTimePerDocumentMicros = nodesIndexed == 0 ? -1 : (totalTimeIndexingNanos / nodesIndexed)/1000;
        long otherTimeNanos = totalTimeIndexingNanos - totalTimeMakingDocumentsNanos - totalTmeWritingToIndexNanos;
        double percentageIndexing = FormattingUtils.safeComputePercentage(totalTimeIndexingNanos, totalTimeNanos);
        double percentageMakingDocument = FormattingUtils.safeComputePercentage(totalTimeMakingDocumentsNanos, totalTimeIndexingNanos);
        double percentageWritingToIndex = FormattingUtils.safeComputePercentage(totalTmeWritingToIndexNanos, totalTimeIndexingNanos);
        double percentageOther = FormattingUtils.safeComputePercentage(otherTimeNanos, totalTimeIndexingNanos);
        return String.format("Indexed %d nodes in %s. Avg per node: %d microseconds. indexingTime: %s (%2.1f%% of total time). Breakup of indexing time: makeDocument: %s (%2.1f%%), writeIndex: %s (%2.1f%%), other: %s (%2.1f%%)",
                progressReporter.getTotalUpdatesCount(), FormattingUtils.formatNanosToSeconds(totalTimeNanos), avgTimePerDocumentMicros,
                FormattingUtils.formatNanosToSeconds(totalTimeIndexingNanos), percentageIndexing,
                FormattingUtils.formatNanosToSeconds(totalTimeMakingDocumentsNanos), percentageMakingDocument,
                FormattingUtils.formatNanosToSeconds(totalTmeWritingToIndexNanos), percentageWritingToIndex,
                FormattingUtils.formatNanosToSeconds(otherTimeNanos), percentageOther);
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
