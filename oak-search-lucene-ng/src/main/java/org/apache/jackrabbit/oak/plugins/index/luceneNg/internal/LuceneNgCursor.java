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

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.index.cursor.AbstractCursor;
import org.apache.jackrabbit.oak.plugins.index.luceneNg.LuceneNgIndexTracker;
import org.apache.jackrabbit.oak.plugins.index.search.FieldNames;
import org.apache.jackrabbit.oak.spi.query.IndexRow;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.uhighlight.UnifiedHighlighter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.ref.Cleaner;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;

/**
 * Cursor over Lucene 9 search results.
 *
 * <p>Two modes are supported:</p>
 * <ul>
 *   <li><b>Eager</b> (legacy) — constructed with a pre-computed {@link TopDocs} and a live
 *       {@link IndexSearcher}; holds the acquired index node open until the cursor is exhausted,
 *       closed, or garbage-collected. Used by direct-searcher tests and the older query paths.</li>
 *   <li><b>Lazy / batched</b> — constructed with a {@link LuceneNgIndexTracker} and a query. Each
 *       {@link #hasNext()}/{@link #next()} that runs off the end of the current batch acquires the
 *       index node <em>only</em> for the duration of fetching one bounded batch (via
 *       {@code search}/{@code searchAfter}), materializes that batch's rows into a detached queue
 *       — including per-batch excerpt generation — and releases the node again. This mirrors the
 *       shape of legacy {@code LucenePropertyIndex.loadDocs()} and avoids holding the searcher open
 *       for the whole cursor lifetime.</li>
 * </ul>
 *
 * <p>The mode is selected by whether {@link #tracker} is non-null (set only by the lazy
 * constructor).</p>
 */
public class LuceneNgCursor extends AbstractCursor {

    private static final Logger LOG = LoggerFactory.getLogger(LuceneNgCursor.class);
    private static final int DEFAULT_FACET_TOP_CHILDREN = 10;
    private static final int INITIAL_BATCH_SIZE = 50;
    private static final int MAX_BATCH_SIZE = 100_000;
    private static final Cleaner CLEANER = Cleaner.create();

    // --- eager-mode state (null / unused in lazy mode) ---
    private final TopDocs docs;
    private final IndexSearcher searcher;
    private final Map<Integer, String> excerptMap;  // docId -> highlighted excerpt
    private int currentIndex = 0;

    // --- shared state ---
    private final Map<String, String> facetColumns; // rep:facet(dim) -> JSON
    private final int facetTopChildren;
    private final Cleaner.Cleanable cleanable;

    // --- lazy-mode state (null / unused in eager mode) ---
    private final LuceneNgIndexTracker tracker;
    private final String indexPath;
    private final Query lazyQuery;
    private final Sort lazySort;
    private final boolean needsExcerpts;
    private final Analyzer excerptAnalyzer;
    private final Queue<LuceneNgIndexRow> pendingRows;
    private int nextBatchSize = INITIAL_BATCH_SIZE;
    private ScoreDoc lastScoreDoc = null;
    private boolean noMoreDocs = false;
    private long lazySize = 0;

    public LuceneNgCursor(TopDocs docs, IndexSearcher searcher) {
        this(docs, searcher, null, Collections.emptyMap(), DEFAULT_FACET_TOP_CHILDREN, null);
    }

    public LuceneNgCursor(TopDocs docs, IndexSearcher searcher,
                          LuceneNgIndexNode indexNode) {
        this(docs, searcher, null, Collections.emptyMap(), DEFAULT_FACET_TOP_CHILDREN, indexNode);
    }

    public LuceneNgCursor(TopDocs docs, IndexSearcher searcher, Map<String, Facets> facetsMap) {
        this(docs, searcher, facetsMap, Collections.emptyMap(), DEFAULT_FACET_TOP_CHILDREN, null);
    }

    public LuceneNgCursor(TopDocs docs, IndexSearcher searcher,
                          Map<String, Facets> facetsMap, Map<Integer, String> excerptMap,
                          int facetTopChildren, LuceneNgIndexNode indexNode) {
        this.docs = docs;
        this.searcher = searcher;
        this.facetTopChildren = Math.max(1, facetTopChildren);
        this.facetColumns = buildFacetColumns(facetsMap != null ? facetsMap : Collections.emptyMap());
        this.excerptMap = excerptMap != null ? excerptMap : Collections.emptyMap();
        // Eager mode: no lazy state.
        this.tracker = null;
        this.indexPath = null;
        this.lazyQuery = null;
        this.lazySort = null;
        this.needsExcerpts = false;
        this.excerptAnalyzer = null;
        this.pendingRows = null;
        // Fires on cursor GC if not already released via hasNext()==false or close().
        Runnable release = indexNode != null ? indexNode::release : () -> {};
        this.cleanable = CLEANER.register(this, release);
    }

    /**
     * Lazy, batched constructor: does not eagerly search or hold an {@link IndexSearcher}.
     * Each {@link #hasNext()}/{@link #next()} that runs off the current batch acquires the index
     * node only for the duration of fetching one bounded batch, then releases it — mirroring
     * legacy {@code LucenePropertyIndex.loadDocs()}, including per-batch excerpt generation.
     *
     * @param tracker        the tracker to acquire the index node from, per batch
     * @param indexPath      the index definition path
     * @param query          the Lucene query to page through
     * @param sort           the sort order, or {@code null} for score order
     * @param facetColumns   pre-computed {@code rep:facet(dim) -> JSON} columns (or empty)
     * @param needsExcerpts  whether excerpts should be generated per batch (fulltext queries)
     * @param excerptAnalyzer analyzer for excerpt highlighting; owned and closed by this cursor
     */
    public LuceneNgCursor(LuceneNgIndexTracker tracker, String indexPath, Query query, Sort sort,
                          Map<String, String> facetColumns, boolean needsExcerpts, Analyzer excerptAnalyzer) {
        this.tracker = tracker;
        this.indexPath = indexPath;
        this.lazyQuery = query;
        this.lazySort = sort;
        this.facetColumns = facetColumns != null ? facetColumns : Collections.emptyMap();
        this.facetTopChildren = DEFAULT_FACET_TOP_CHILDREN;
        this.needsExcerpts = needsExcerpts;
        this.excerptAnalyzer = excerptAnalyzer;
        this.pendingRows = new LinkedList<>();
        // Eager fields unused in lazy mode.
        this.docs = null;
        this.searcher = null;
        this.excerptMap = Collections.emptyMap();
        // The analyzer (a Closeable) is held for the cursor's whole life; close it on
        // exhaustion / close() / GC. The runnable must not capture `this`.
        final Analyzer analyzerToClose = excerptAnalyzer;
        this.cleanable = CLEANER.register(this, () -> {
            if (analyzerToClose != null) {
                analyzerToClose.close();
            }
        });
    }

    private Map<String, String> buildFacetColumns(Map<String, Facets> facetsMap) {
        if (facetsMap.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, Facets> entry : facetsMap.entrySet()) {
            String dimension = entry.getKey();
            try {
                // Dimension is the Oak property name (matches legacy lucene index / rep:facet(foo)).
                String luceneFieldName = FieldNames.createFacetFieldName(dimension);
                FacetResult fr = entry.getValue().getTopChildren(facetTopChildren, dimension);
                if (fr == null || fr.labelValues == null) {
                    fr = entry.getValue().getTopChildren(facetTopChildren, luceneFieldName);
                }
                if (fr != null && fr.labelValues != null) {
                    JsopBuilder json = new JsopBuilder();
                    json.object();
                    for (org.apache.lucene.facet.LabelAndValue lv : fr.labelValues) {
                        json.key(lv.label);
                        json.value(lv.value.intValue());
                    }
                    json.endObject();
                    result.put(QueryConstants.REP_FACET + "(" + dimension + ")", json.toString());
                }
            } catch (IOException e) {
                LOG.error("Failed to build facets for {}: {}", dimension, e.getMessage());
            }
        }
        return Collections.unmodifiableMap(result);
    }

    @Override
    public boolean hasNext() {
        if (tracker == null) {
            // legacy eager path
            boolean more = currentIndex < docs.scoreDocs.length;
            if (!more) {
                cleanable.clean();
            }
            return more;
        }
        if (!pendingRows.isEmpty()) {
            return true;
        }
        if (noMoreDocs) {
            cleanable.clean();
            return false;
        }
        if (loadNextBatch()) {
            return true;
        }
        cleanable.clean();
        return false;
    }

    @Override
    public IndexRow next() {
        if (tracker == null) {
            // legacy eager path
            ScoreDoc scoreDoc = docs.scoreDocs[currentIndex++];
            try {
                Document doc = searcher.storedFields().document(scoreDoc.doc);
                String path = doc.get(FieldNames.PATH);
                String excerpt = excerptMap.get(scoreDoc.doc);
                return new LuceneNgIndexRow(path, scoreDoc.score, facetColumns, excerpt);
            } catch (IOException e) {
                LOG.error("Error reading document", e);
                throw new RuntimeException(e);
            }
        }
        if (pendingRows.isEmpty() && !loadNextBatch()) {
            throw new NoSuchElementException();
        }
        return pendingRows.poll();
    }

    /**
     * Fetches one bounded batch: acquires the index node, runs one {@code search}/{@code searchAfter}
     * call, generates excerpts for that batch if needed, materializes every resulting row into
     * {@link #pendingRows}, releases the index node, and returns whether any rows were added.
     */
    private boolean loadNextBatch() {
        LuceneNgIndexNode indexNode = tracker.acquireIndexNode(indexPath);
        if (indexNode == null) {
            noMoreDocs = true;
            return false;
        }
        try {
            IndexSearcher batchSearcher = indexNode.getSearcher();
            int batchSize = nextBatchSize;
            TopDocs batchDocs;
            if (lastScoreDoc == null) {
                batchDocs = lazySort == null
                        ? batchSearcher.search(lazyQuery, batchSize)
                        : batchSearcher.search(lazyQuery, batchSize, lazySort);
            } else {
                batchDocs = lazySort == null
                        ? batchSearcher.searchAfter(lastScoreDoc, lazyQuery, batchSize)
                        : batchSearcher.searchAfter(lastScoreDoc, lazyQuery, batchSize, lazySort);
            }
            nextBatchSize = (int) Math.min(nextBatchSize * 2L, MAX_BATCH_SIZE);

            if (batchDocs.scoreDocs.length == 0) {
                noMoreDocs = true;
                return false;
            }

            Map<Integer, String> batchExcerpts = Collections.emptyMap();
            if (needsExcerpts) {
                batchExcerpts = generateExcerptsForBatch(batchSearcher, lazyQuery, batchDocs, excerptAnalyzer);
            }

            for (ScoreDoc scoreDoc : batchDocs.scoreDocs) {
                Document doc = batchSearcher.storedFields().document(scoreDoc.doc);
                String path = doc.get(FieldNames.PATH);
                String excerpt = batchExcerpts.get(scoreDoc.doc);
                pendingRows.add(new LuceneNgIndexRow(path, scoreDoc.score, facetColumns, excerpt));
                lastScoreDoc = scoreDoc;
                lazySize++;
            }
            if (batchDocs.scoreDocs.length < batchSize) {
                // fewer hits than requested — no more results after this batch
                noMoreDocs = true;
            }
            return true;
        } catch (IOException e) {
            LOG.error("Error executing batched query on index: " + indexPath, e);
            noMoreDocs = true;
            return false;
        } finally {
            indexNode.release();
        }
    }

    /**
     * Same {@link UnifiedHighlighter}-based approach as the eager excerpt generation in
     * {@code LuceneNgIndex}, scoped to one batch's {@link TopDocs} instead of the whole result set.
     */
    private static Map<Integer, String> generateExcerptsForBatch(IndexSearcher searcher, Query query,
            TopDocs docs, Analyzer analyzer) {
        if (docs.scoreDocs.length == 0) {
            return Collections.emptyMap();
        }
        try {
            UnifiedHighlighter highlighter = new UnifiedHighlighter(searcher, analyzer);
            String[] snippets = highlighter.highlight(FieldNames.FULLTEXT, query, docs, 1);
            if (snippets == null) {
                return Collections.emptyMap();
            }
            Map<Integer, String> excerptMap = new HashMap<>();
            for (int i = 0; i < snippets.length; i++) {
                if (snippets[i] != null) {
                    excerptMap.put(docs.scoreDocs[i].doc, snippets[i]);
                }
            }
            return excerptMap;
        } catch (IOException e) {
            LOG.debug("Failed to generate excerpts for batch: {}", e.getMessage());
            return Collections.emptyMap();
        }
    }

    @Override
    public long getSize(org.apache.jackrabbit.oak.api.Result.SizePrecision precision, long max) {
        if (tracker == null) {
            return docs.totalHits.value;
        }
        // Lazy mode does not know the total up front; report the number materialized so far
        // only once the result set is fully drained, otherwise "unknown" (-1), matching the
        // legacy contract for streamed cursors.
        return noMoreDocs && pendingRows.isEmpty() ? lazySize : -1;
    }

    public void close() {
        cleanable.clean();
    }
}
