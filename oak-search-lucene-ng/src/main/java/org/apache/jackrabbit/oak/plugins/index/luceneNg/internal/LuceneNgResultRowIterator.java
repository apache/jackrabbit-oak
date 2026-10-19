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
package org.apache.jackrabbit.oak.plugins.index.luceneNg.internal;

import org.apache.jackrabbit.oak.plugins.index.luceneNg.LuceneNgIndexTracker;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex.FacetProvider;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex.FulltextResultRow;
import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex.IteratorRewoundStateProvider;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * Result rows of a LuceneNg query, loaded in growing batches. Each batch acquires the index node
 * only while it is loaded. When the index was refreshed since the previous batch, the search
 * restarts from the first result of the new index version, and {@link #rewoundCount()} grows so
 * that the consuming cursor can skip rows it already returned.
 */
public class LuceneNgResultRowIterator implements Iterator<FulltextResultRow>, IteratorRewoundStateProvider {

    private static final Logger LOG = LoggerFactory.getLogger(LuceneNgResultRowIterator.class);
    private static final int INITIAL_BATCH_SIZE = 50;
    private static final int MAX_BATCH_SIZE = 100_000;

    private final LuceneNgIndexTracker tracker;
    private final String indexPath;
    private final Query query;
    private final Sort sort;
    private final boolean excerpts;
    private final FacetProvider facetProvider;
    private final UnaryOperator<String> resultPath;
    private final Deque<FulltextResultRow> queue = new ArrayDeque<>();
    private ScoreDoc lastDoc;
    private int nextBatchSize = INITIAL_BATCH_SIZE;
    private boolean noMoreDocs;
    private int indexNodeId = -1;
    private int rewoundCount;

    /**
     * @param sort          the sort order, or {@code null} for score order
     * @param excerpts      whether rows carry a {@code rep:excerpt} for the fulltext condition
     * @param facetProvider the facets of the whole result, or {@code null}
     * @param resultPath    the result path of an indexed document path, or {@code null} to skip it
     */
    public LuceneNgResultRowIterator(LuceneNgIndexTracker tracker, String indexPath, Query query, @Nullable Sort sort,
                                     boolean excerpts, @Nullable FacetProvider facetProvider,
                                     UnaryOperator<String> resultPath) {
        this.tracker = tracker;
        this.indexPath = indexPath;
        this.query = query;
        this.sort = sort;
        this.excerpts = excerpts;
        this.facetProvider = facetProvider;
        this.resultPath = resultPath;
    }

    @Override
    public boolean hasNext() {
        while (queue.isEmpty() && !noMoreDocs) {
            loadBatch();
        }
        return !queue.isEmpty();
    }

    @Override
    public FulltextResultRow next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return queue.remove();
    }

    @Override
    public int rewoundCount() {
        return rewoundCount;
    }

    private void loadBatch() {
        LuceneNgIndexNode indexNode = tracker.acquireIndexNode(indexPath);
        if (indexNode == null) {
            noMoreDocs = true;
            return;
        }
        try {
            if (indexNode.getIndexNodeId() != indexNodeId) {
                if (indexNodeId != -1) {
                    LOG.info("Change in index version detected for {}. Query would be performed without offset",
                            indexPath);
                    rewoundCount++;
                }
                indexNodeId = indexNode.getIndexNodeId();
                lastDoc = null;
            }
            IndexSearcher searcher = indexNode.getSearcher();
            int batchSize = nextBatchSize;
            nextBatchSize = (int) Math.min(nextBatchSize * 2L, MAX_BATCH_SIZE);
            TopDocs docs;
            if (lastDoc == null) {
                docs = sort == null ? searcher.search(query, batchSize) : searcher.search(query, batchSize, sort);
            } else {
                docs = sort == null
                        ? searcher.searchAfter(lastDoc, query, batchSize)
                        : searcher.searchAfter(lastDoc, query, batchSize, sort);
            }
            String[] batchExcerpts = excerpts ? excerpts(searcher, docs) : new String[docs.scoreDocs.length];
            StoredFields storedFields = searcher.storedFields();
            for (int i = 0; i < docs.scoreDocs.length; i++) {
                ScoreDoc scoreDoc = docs.scoreDocs[i];
                lastDoc = scoreDoc;
                String path = resultPath.apply(
                        storedFields.document(scoreDoc.doc, Set.of(FieldNames.PATH)).get(FieldNames.PATH));
                if (path == null) {
                    continue;
                }
                Map<String, String> rowExcerpts = batchExcerpts[i] == null
                        ? Collections.emptyMap()
                        : Map.of(QueryConstants.REP_EXCERPT, batchExcerpts[i]);
                queue.add(new FulltextResultRow(path, scoreDoc.score, rowExcerpts, facetProvider, null));
            }
            if (docs.scoreDocs.length < batchSize) {
                noMoreDocs = true;
            }
        } catch (IOException e) {
            LOG.warn("Query on index {} failed", indexPath, e);
            noMoreDocs = true;
        } finally {
            indexNode.release();
        }
    }

    private String[] excerpts(IndexSearcher searcher, TopDocs docs) {
        try (Analyzer analyzer = new StandardAnalyzer()) {
            String[] snippets = new UnifiedHighlighter(searcher, analyzer).highlight(FieldNames.FULLTEXT, query, docs, 1);
            return snippets != null ? snippets : new String[docs.scoreDocs.length];
        } catch (IOException e) {
            LOG.debug("Failed to generate excerpts for a batch of {}: {}", indexPath, e.getMessage());
            return new String[docs.scoreDocs.length];
        }
    }
}
